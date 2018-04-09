// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Connections;
using Microsoft.AspNetCore.Server.Kestrel.Transport.Abstractions.Internal;
using Microsoft.Extensions.Logging;

namespace Microsoft.AspNetCore.Server.Kestrel.Transport.Sockets.Internal
{
    internal sealed class SocketConnection : TransportConnection
    {
        private const int MinAllocBufferSize = 2048;
        public readonly static bool IsWindows = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);

        private readonly Socket _socket;
        private readonly SocketScheduler _scheduler;
        private readonly ISocketsTrace _trace;
        private readonly SocketReceiver _receiver;
        private readonly SocketSender _sender;

        private volatile bool _aborted;

        internal SocketConnection(Socket socket, MemoryPool<byte> memoryPool, SocketScheduler scheduler, ISocketsTrace trace)
        {
            Debug.Assert(socket != null);
            Debug.Assert(memoryPool != null);
            Debug.Assert(trace != null);

            _socket = socket;
            MemoryPool = memoryPool;
            _scheduler = scheduler;
            _trace = trace;

            var localEndPoint = (IPEndPoint)_socket.LocalEndPoint;
            var remoteEndPoint = (IPEndPoint)_socket.RemoteEndPoint;

            LocalAddress = localEndPoint.Address;
            LocalPort = localEndPoint.Port;

            RemoteAddress = remoteEndPoint.Address;
            RemotePort = remoteEndPoint.Port;

            // On *nix platforms, Sockets already dispatches to the ThreadPool.
            var awaiterScheduler = IsWindows ? _scheduler.IOCompletionScheduler : PipeScheduler.Inline;

            _receiver = new SocketReceiver(_socket, awaiterScheduler);
            _sender = new SocketSender(_socket, awaiterScheduler);
        }

        public override MemoryPool<byte> MemoryPool { get; }
        public override PipeScheduler InputWriterScheduler => PipeScheduler.Inline;
        public override PipeScheduler OutputReaderScheduler => PipeScheduler.Inline;
        
        public async Task StartAsync(IConnectionDispatcher connectionDispatcher)
        {
            Exception sendError = null;
            try
            {
                connectionDispatcher.OnConnection(this);

                // Spawn send and receive logic
                Task receiveTask = DoReceive();
                Task<Exception> sendTask = DoSend();

                // If the sending task completes then close the receive
                // We don't need to do this in the other direction because the kestrel
                // will trigger the output closing once the input is complete.
                if (await Task.WhenAny(receiveTask, sendTask) == sendTask)
                {
                    // Tell the reader it's being aborted
                    _socket.Dispose();
                }

                // Now wait for both to complete
                await receiveTask;
                sendError = await sendTask;

                // Dispose the socket(should noop if already called)
                _socket.Dispose();
            }
            catch (Exception ex)
            {
                _trace.LogError(0, ex, $"Unexpected exception in {nameof(SocketConnection)}.{nameof(StartAsync)}.");
            }
            finally
            {
                // Complete the output after disposing the socket
                Output.Complete(sendError);
            }
        }

        private async Task DoReceive()
        {
            Exception error = null;

            try
            {
                await ProcessReceives();
            }
            catch (SocketException ex) when (ex.SocketErrorCode == SocketError.ConnectionReset)
            {
                error = new ConnectionResetException(ex.Message, ex);
                _trace.ConnectionReset(ConnectionId);
            }
            catch (SocketException ex) when (ex.SocketErrorCode == SocketError.OperationAborted ||
                                             ex.SocketErrorCode == SocketError.ConnectionAborted ||
                                             ex.SocketErrorCode == SocketError.Interrupted ||
                                             ex.SocketErrorCode == SocketError.InvalidArgument)
            {
                if (!_aborted)
                {
                    // Calling Dispose after ReceiveAsync can cause an "InvalidArgument" error on *nix.
                    error = new ConnectionAbortedException();
                    _trace.ConnectionError(ConnectionId, error);
                }
            }
            catch (ObjectDisposedException)
            {
                if (!_aborted)
                {
                    error = new ConnectionAbortedException();
                    _trace.ConnectionError(ConnectionId, error);
                }
            }
            catch (IOException ex)
            {
                error = ex;
                _trace.ConnectionError(ConnectionId, error);
            }
            catch (Exception ex)
            {
                error = new IOException(ex.Message, ex);
                _trace.ConnectionError(ConnectionId, error);
            }
            finally
            {
                if (_aborted)
                {
                    error = error ?? new ConnectionAbortedException();
                }

                Input.Complete(error);
            }
        }

        private async Task ProcessReceives()
        {
            while (true)
            {
                // Ensure we have some reasonable amount of buffer space
                var buffer = Input.GetMemory(MinAllocBufferSize);

                await _scheduler.ReceiveAwaitableInstance;
                var bytesReceived = await _receiver.ReceiveAsync(buffer);

                if (bytesReceived == 0)
                {
                    // FIN
                    _trace.ConnectionReadFin(ConnectionId);
                    break;
                }

                Input.Advance(bytesReceived);

                var flushTask = Input.FlushAsync();

                if (!flushTask.IsCompleted)
                {
                    _trace.ConnectionPause(ConnectionId);

                    await flushTask;

                    _trace.ConnectionResume(ConnectionId);
                }

                var result = flushTask.GetAwaiter().GetResult();
                if (result.IsCompleted)
                {
                    // Pipe consumer is shut down, do we stop writing
                    break;
                }
            }
        }

        private async Task<Exception> DoSend()
        {
            Exception error = null;

            try
            {
                await ProcessSends();
            }
            catch (SocketException ex) when (ex.SocketErrorCode == SocketError.OperationAborted)
            {
                error = null;
            }
            catch (ObjectDisposedException)
            {
                error = null;
            }
            catch (IOException ex)
            {
                error = ex;
            }
            catch (Exception ex)
            {
                error = new IOException(ex.Message, ex);
            }
            finally
            {
                // Make sure to close the connection only after the _aborted flag is set.
                // Without this, the RequestsCanBeAbortedMidRead test will sometimes fail when
                // a BadHttpRequestException is thrown instead of a TaskCanceledException.
                _aborted = true;
                _trace.ConnectionWriteFin(ConnectionId);
                _socket.Shutdown(SocketShutdown.Both);
            }

            return error;
        }

        private async Task ProcessSends()
        {
            while (true)
            {
                // Wait for data to write from the pipe producer
                var result = await Output.ReadAsync();
                var buffer = result.Buffer;

                if (result.IsCanceled)
                {
                    break;
                }

                var end = buffer.End;
                var isCompleted = result.IsCompleted;
                if (!buffer.IsEmpty)
                {
                    await _scheduler.SendAwaitableInstance;
                    await _sender.SendAsync(buffer);
                }

                Output.AdvanceTo(end);

                if (isCompleted)
                {
                    break;
                }
            }
        }
    }

    internal sealed class SocketScheduler
    {
        private static readonly WaitCallback _doWorkCallback = s => ((SocketScheduler)s).DoWork();
        private readonly ConcurrentQueue<Action> _sendWorkItems = new ConcurrentQueue<Action>();
        private readonly ConcurrentQueue<Action> _receiveWorkItems = new ConcurrentQueue<Action>();
        private readonly object _workSync = new object();
        private bool _doingWork;

        public SocketScheduler()
        {
            SendAwaitableInstance = new SendAwaitable(this);
            ReceiveAwaitableInstance = new ReceiveAwaitable(this);

            // CONSIDER: Don't know if scheduling this is necessary in this model.
            IOCompletionScheduler = new IOQueue();
        }

        public PipeScheduler IOCompletionScheduler { get; }

        public SendAwaitable SendAwaitableInstance { get; }
        public ReceiveAwaitable ReceiveAwaitableInstance { get; }

        public void ScheduleSend(Action action)
        {
            _sendWorkItems.Enqueue(action);

            lock (_workSync)
            {
                if (!_doingWork)
                {
                    System.Threading.ThreadPool.QueueUserWorkItem(_doWorkCallback, this);
                    _doingWork = true;
                }
            }
        }

        public void ScheduleReceive(Action action)
        {
            _receiveWorkItems.Enqueue(action);

            lock (_workSync)
            {
                if (!_doingWork)
                {
                    System.Threading.ThreadPool.QueueUserWorkItem(_doWorkCallback, this);
                    _doingWork = true;
                }
            }
        }

        private void DoWork()
        {
            while (true)
            {
                while (_sendWorkItems.TryDequeue(out Action action))
                {
                    action();
                }

                while (_receiveWorkItems.TryDequeue(out Action action))
                {
                    action();
                }

                lock (_workSync)
                {
                    if (_sendWorkItems.IsEmpty && _receiveWorkItems.IsEmpty)
                    {
                        _doingWork = false;
                        return;
                    }
                }
            }
        }

        public class SendAwaitable : ICriticalNotifyCompletion
        {
            private readonly SocketScheduler _socketScheduler;

            public SendAwaitable(SocketScheduler socketScheduler)
            {
                _socketScheduler = socketScheduler;
            }

            public SendAwaitable GetAwaiter() => this;
            public bool IsCompleted => false;

            public void GetResult()
            {
            }

            public void OnCompleted(Action continuation)
            {
                UnsafeOnCompleted(continuation);
            }

            public void UnsafeOnCompleted(Action continuation)
            {
                _socketScheduler.ScheduleSend(continuation);
            }
        }

        public class ReceiveAwaitable : ICriticalNotifyCompletion
        {
            private readonly SocketScheduler _socketScheduler;

            public ReceiveAwaitable(SocketScheduler socketScheduler)
            {
                _socketScheduler = socketScheduler;
            }

            public ReceiveAwaitable GetAwaiter() => this;
            public bool IsCompleted => false;

            public void GetResult()
            {
            }

            public void OnCompleted(Action continuation)
            {
                UnsafeOnCompleted(continuation);
            }

            public void UnsafeOnCompleted(Action continuation)
            {
                _socketScheduler.ScheduleReceive(continuation);
            }
        }
    }
}
