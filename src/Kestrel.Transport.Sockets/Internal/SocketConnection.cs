// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Buffers;
using System.Diagnostics;
using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using Microsoft.AspNetCore.Connections.Features;
using Microsoft.AspNetCore.Server.Kestrel.Transport.Abstractions.Internal;
using Microsoft.Extensions.Logging;

namespace Microsoft.AspNetCore.Server.Kestrel.Transport.Sockets.Internal
{
    internal sealed class SocketConnection : TransportConnection, IConnectionTransportFeature
    {
        private const int MinAllocBufferSize = 2048;

        private readonly Socket _socket;
        private readonly ISocketsTrace _trace;

        private readonly IDuplexPipe _transportPipes;

        internal SocketConnection(Socket socket, MemoryPool<byte> memoryPool, PipeScheduler scheduler, ISocketsTrace trace)
        {
            Debug.Assert(socket != null);
            Debug.Assert(memoryPool != null);
            Debug.Assert(trace != null);

            _socket = socket;
            MemoryPool = memoryPool;
            _trace = trace;

            var localEndPoint = (IPEndPoint)_socket.LocalEndPoint;
            var remoteEndPoint = (IPEndPoint)_socket.RemoteEndPoint;

            LocalAddress = localEndPoint.Address;
            LocalPort = localEndPoint.Port;

            RemoteAddress = remoteEndPoint.Address;
            RemotePort = remoteEndPoint.Port;

            var pipeReader = new SocketPipeReader(this);
            var pipeWriter = new SocketPipeWriter(this);
            _transportPipes = new DuplexPipe(pipeReader, pipeWriter);
        }

        public override MemoryPool<byte> MemoryPool { get; }

        public async Task StartAsync(IConnectionDispatcher connectionDispatcher)
        {
            try
            {
                connectionDispatcher.OnConnection(this);

                // TODO: Handle connection shutdown
                await Task.Delay(-1);
            }
            catch (Exception ex)
            {
                _trace.LogError(0, ex, $"Unexpected exception in {nameof(SocketConnection)}.{nameof(StartAsync)}.");
            }
        }

        public override IDuplexPipe Transport
        {
            get => _transportPipes;
            set { }
        }

        private const int BufferSize = 4096;

        sealed class SocketPipeReader : PipeReader
        {
            private readonly SocketConnection _socketConnection;
            private readonly Memory<byte> _readBuffer;
            private int _readStart;
            private int _readEnd;

            private static readonly BatchScheduler s_batchScheduler = new BatchScheduler();

            public SocketPipeReader(SocketConnection socketConnection)
            {
                _socketConnection = socketConnection;

                _readBuffer = _socketConnection.MemoryPool.Rent(BufferSize).Memory;
            }

            public override void AdvanceTo(SequencePosition consumed)
            {
                AdvanceTo(consumed, consumed);
            }

            public override void AdvanceTo(SequencePosition consumed, SequencePosition examined)
            {
                int offset = consumed.GetInteger();

                if (offset + _readStart > _readEnd)
                {
                    throw new InvalidOperationException("SocketPipeReader.AdvanceTo past _readEnd");
                }

                _readStart += offset;
                if (_readStart == _readEnd)
                {
                    _readStart = 0;
                    _readEnd = 0;
                }
            }

            public override void CancelPendingRead()
            {
//                throw new NotImplementedException();
            }

            public override void Complete(Exception exception = null)
            {
                // TODO: Shutdown
            }

            public override void OnWriterCompleted(Action<Exception, object> callback, object state)
            {
                // TODO
            }

            public override async ValueTask<ReadResult> ReadAsync(CancellationToken cancellationToken = default)
            {
                Memory<byte> availableBuffer = _readBuffer.Slice(_readEnd);
                if (availableBuffer.Length == 0)
                {
                    throw new NotSupportedException("Out of buffer space");
                }

                int bytesRead = await _socketConnection._socket.ReceiveAsync(availableBuffer, SocketFlags.None);

                // Force rest of continuation to be executed in batch
                await s_batchScheduler.BatchAsync();

                if (bytesRead == 0)
                {
                    return new ReadResult(default(ReadOnlySequence<byte>), false, true);
                }

                _readEnd += bytesRead;
                return new ReadResult(new ReadOnlySequence<byte>(_readBuffer.Slice(_readStart, _readEnd - _readStart)), false, false);
            }

            public override bool TryRead(out ReadResult result)
            {
                ReadOnlyMemory<byte> readBytes = _readBuffer.Slice(_readStart, _readEnd - _readStart);
                if (readBytes.Length == 0)
                {
                    result = default;
                    return false;
                }

                result = new ReadResult(new ReadOnlySequence<byte>(readBytes), false, false);
                return true;
            }
        }

        sealed class SocketPipeWriter : PipeWriter
        {
            private SocketConnection _socketConnection;
            private readonly Memory<byte> _writeBuffer;
            private int _writeEnd;

            public SocketPipeWriter(SocketConnection socketConnection)
            {
                _socketConnection = socketConnection;

                _writeBuffer = _socketConnection.MemoryPool.Rent(BufferSize).Memory;
            }

            public override void Advance(int bytes)
            {
                if (_writeEnd + bytes > _writeBuffer.Length)
                {
                    throw new InvalidOperationException("Advance past end of buffer");
                }

                _writeEnd += bytes;
            }

            public override void CancelPendingFlush()
            {
                // TODO
            }

            public override void Complete(Exception exception = null)
            {
                // TODO
            }

            public override async ValueTask<FlushResult> FlushAsync(CancellationToken cancellationToken = default)
            {
                if (_writeEnd == 0)
                {
                    return new FlushResult(false, false);
                }

                await _socketConnection._socket.SendAsync(_writeBuffer.Slice(0, _writeEnd), SocketFlags.None);

                _writeEnd = 0;
                return new FlushResult(false, false);
            }

            public override Memory<byte> GetMemory(int sizeHint = 0)
            {
                Memory<byte> availableBuffer = _writeBuffer.Slice(_writeEnd);

                if (availableBuffer.Length < sizeHint)
                {
                    throw new NotSupportedException("Out of buffer space");
                }

                return availableBuffer;
            }

            public override Span<byte> GetSpan(int sizeHint = 0)
            {
                return GetMemory(sizeHint).Span;
            }

            public override void OnReaderCompleted(Action<Exception, object> callback, object state)
            {
                // TODO
            }
        }
    }

    sealed class BatchScheduler
    {
        private const int BatchSize = 8;

        private readonly object _lockObj;
        private TaskCompletionSource<bool>[] _batchItems;
        private int _itemCount;
        private readonly Timer _timer;

        public BatchScheduler()
        {
            _lockObj = new object();
            _batchItems = new TaskCompletionSource<bool>[BatchSize];
            _itemCount = 0;

            _timer = new Timer((s) =>
            {
                TimerCallback();
            }, null, 100, 100);
        }

        public Task BatchAsync()
        {
            TaskCompletionSource<bool> source = new TaskCompletionSource<bool>();

            TaskCompletionSource<bool>[] batchToExecute = null;

            lock (_lockObj)
            {
                _batchItems[_itemCount] = source;
                _itemCount++;

                if (_itemCount == BatchSize)
                {
                    batchToExecute = _batchItems;
                    _batchItems = new TaskCompletionSource<bool>[BatchSize];
                    _itemCount = 0;
                }
            }

            if (batchToExecute != null)
            {
                for (int i = 0; i < BatchSize; i++)
                {
                    batchToExecute[i].SetResult(true);
                }
            }

            return source.Task;
        }

        private void TimerCallback()
        {
            TaskCompletionSource<bool>[] batchToExecute = null;
            int itemCount = 0;

            lock (_lockObj)
            {
                if (_itemCount > 0)
                {
                    batchToExecute = _batchItems;
                    itemCount = _itemCount;

                    _batchItems = new TaskCompletionSource<bool>[BatchSize];
                    _itemCount = 0;
                }
            }

            if (batchToExecute != null)
            {
                for (int i = 0; i < itemCount; i++)
                {
                    batchToExecute[i].SetResult(true);
                }
            }
        }
    }
}
