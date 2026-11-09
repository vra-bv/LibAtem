﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using log4net;
using LibAtem.Commands;
using LibAtem.Net.DataTransfer;

namespace LibAtem.Net
{
    public class AtemClient : IDisposable
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(AtemClient));

        public const int DefaultPort = 9910;

        // How quickly the receive thread notices Dispose. Closing the socket does not reliably unblock a
        // synchronous Receive: on macOS the close spins until the Receive returns.
        private const int ReceivePollInterval = 100; // ms
        private const int IdleSendWait = 100; // ms
        // Room for a full initial state dump of a large switcher without the kernel dropping packets
        private const int ReceiveBufferSize = 1 << 20;
        private static readonly TimeSpan ShutdownTimeout = TimeSpan.FromSeconds(1);

        [ThreadStatic]
        private static AtemClient _timerCallbackOwner;

        private readonly UdpClient _client;
        private readonly IPEndPoint _remoteEp;

        private readonly AtemClientConnection _connection;
        private readonly CancellationTokenSource _cts = new CancellationTokenSource();
        private readonly object _lifecycleLock = new object();
        private Timer _timeoutTimer;
        private Thread _receiveThread;
        private Thread _sendThread;
        private Thread _handleThread;
        private volatile bool _run;
        private volatile bool _awaitingHandshake;
        private volatile bool _sessionAssigned;
        private int _disposed;
        private int _sendThreadsStarted;
        private long _sendLoopIterations;

        public delegate void CommandHandler(object sender, IReadOnlyList<ICommand> commands);
        public delegate void ConnectedHandler(object sender);
        public delegate void DisconnectedHandler(object sender);

        public event CommandHandler OnReceive;
        public event ConnectedHandler OnConnection;
        public event DisconnectedHandler OnDisconnect;

        public event AtemConnection.PacketHandler OnReceivePacket
        {
            add => _connection.OnReceivePacket += value;
            remove => _connection.OnReceivePacket -= value;
        }

        public DataTransferManager DataTransfer { get; }

        public ProtocolVersion? ConnectionVersion => _connection.ConnectionVersion;

        /// <summary>
        /// Received packets waiting for the OnReceive handlers. See <see cref="AtemConnection.PendingInboundPackets"/>.
        /// </summary>
        public int PendingInboundPackets => _connection.PendingInboundPackets;

        internal AtemClientConnection Connection => _connection;
        internal Thread ReceiveThread => _receiveThread;
        internal Thread SendThread => _sendThread;
        internal Thread HandleThread => _handleThread;
        internal int SendThreadsStarted => Volatile.Read(ref _sendThreadsStarted);
        internal long SendLoopIterations => Interlocked.Read(ref _sendLoopIterations);

        public AtemClient(string address, bool autoConnect = true) : this(address, DefaultPort, autoConnect)
        {
        }

        public AtemClient(string address, int port, bool autoConnect = true)
        {
            _remoteEp = new IPEndPoint(IPAddress.Parse(address), port);
            _client = new UdpClient(new IPEndPoint(IPAddress.Any, 0));
            try
            {
                _client.Client.ReceiveBufferSize = ReceiveBufferSize;
            }
            catch (SocketException)
            {
                _client.Client.ReceiveBufferSize = 1500 * 50;
            }
            _client.Client.ReceiveTimeout = ReceivePollInterval;

            _connection = new AtemClientConnection(_remoteEp, new Random().Next(32767));
            _connection.OnDisconnect += sender => Raise(() => OnDisconnect?.Invoke(this), nameof(OnDisconnect));
            _connection.OnInitComplete += sender => Raise(() => OnConnection?.Invoke(this), nameof(OnConnection));

            DataTransfer = new DataTransferManager(_connection);

            if (autoConnect)
                Connect();
        }

        public bool Connect()
        {
            lock (_lifecycleLock)
            {
                if (Volatile.Read(ref _disposed) != 0)
                    throw new ObjectDisposedException(nameof(AtemClient));

                // Check if connect has already been called
                if (_run)
                    return false;

                _run = true;
                StartReceiving();
                BeginSession();
                SendHandshake();
                StartSendThread();
                StartHandleThread();
                StartTimeoutTimer();
                return true;
            }
        }

        private bool Reconnect()
        {
            try
            {
                lock (_connection)
                {
                    if (!_run || _cts.IsCancellationRequested)
                        return false;

                    if (!_connection.HasTimedOut)
                        return false;

                    if (_connection.ConnectionVersion != null)
                        Raise(() => OnDisconnect?.Invoke(this), nameof(OnDisconnect));

                    Log.InfoFormat("Attempting Reconnect");
                    BeginSession();
                    _connection.ResetConnStatsInfo();
                    _connection.SessionId = new Random().Next(32767);
                    SendHandshake();
                }
            }
            catch (SocketException e)
            {
                Log.DebugFormat("Failed to reconnect: {0}", e);
            }

            return true;
        }

        // The ATEM keeps resending the previous session's window for a while after a reconnect. Until the
        // handshake reply and the new session's packet 1 arrive, those resends must not reach the connection.
        private void BeginSession()
        {
            _awaitingHandshake = true;
            _sessionAssigned = false;
        }

        // Returns false for a packet that belongs to no session or to another one than the current.
        private bool AcceptSession(ReceivedPacket packet)
        {
            if (_awaitingHandshake)
                return false;

            if (_sessionAssigned)
                return _connection.SessionId == packet.SessionId;

            if (!packet.CommandCode.HasFlag(ReceivedPacket.CommandCodeFlags.AckRequest) || packet.PacketId != 1)
                return false;

            _connection.SessionId = (int)packet.SessionId;
            _sessionAssigned = true;
            Log.InfoFormat("Got new session id: {0}", packet.SessionId);
            return true;
        }

        private void StartTimeoutTimer()
        {
            _timeoutTimer = new Timer(o =>
            {
                if (!_run)
                    return;

                _timerCallbackOwner = this;
                try
                {
                    if (_connection.HasTimedOut)
                        Reconnect();
                }
                catch (Exception e)
                {
                    Log.ErrorFormat("Timeout check failed: {0}", e);
                }
                finally
                {
                    _timerCallbackOwner = null;
                }
            }, null, 0, AtemConstants.TimeoutInterval);
        }

        // One send thread for the lifetime of the client. It also flushes deferred acks, so no separate
        // ack timer is needed. It sleeps until something is queued, acked or reset, or an ack or resend is due.
        private void StartSendThread()
        {
            _sendThread = new Thread(() =>
            {
                CancellationToken token = _cts.Token;
                while (!token.IsCancellationRequested)
                {
                    Interlocked.Increment(ref _sendLoopIterations);

                    bool sent = false;
                    try
                    {
                        if (!_connection.HasTimedOut)
                        {
                            _connection.FlushPendingAck(_client.Client);
                            sent = _connection.TrySendQueued(_client.Client);
                        }
                    }
                    catch (ObjectDisposedException) when (token.IsCancellationRequested)
                    {
                        break;
                    }
                    catch (Exception e)
                    {
                        Log.ErrorFormat("Send failed: {0}", e);
                    }

                    if (!sent)
                        _connection.WaitForOutbound(_connection.HasTimedOut ? IdleSendWait : _connection.NextSendWaitMs(IdleSendWait));
                }
            });
            _sendThread.Name = "LibAtem.Send";
            _sendThread.IsBackground = true;
            Interlocked.Increment(ref _sendThreadsStarted);
            _sendThread.Start();
        }

        private void StartHandleThread()
        {
            _handleThread = new Thread(o =>
            {
                CancellationToken token = _cts.Token;
                while (!token.IsCancellationRequested)
                {
                    List<ICommand> cmds;
                    try
                    {
                        cmds = _connection.GetNextCommands(token);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                    catch (InvalidOperationException)
                    {
                        // Processing was completed by Dispose
                        break;
                    }
                    catch (Exception e)
                    {
                        Log.ErrorFormat("Failed to parse received commands: {0}", e);
                        continue;
                    }

                    try
                    {
                        int rawCount = cmds.Count;

                        cmds = cmds.Where(c => !DataTransfer.HandleCommand(c)).ToList();
                        Log.DebugFormat("Recieved {0} commands. {1} to be handle by user code", rawCount, cmds.Count);

                        if (cmds.Any())
                            OnReceive?.Invoke(this, cmds);
                    }
                    catch (Exception e)
                    {
                        Log.ErrorFormat("Handling received commands failed: {0}", e);
                    }
                }
            });
            _handleThread.Name = "LibAtem.Handle";
            _handleThread.IsBackground = true;
            _handleThread.Start();
        }

        private void SendHandshake()
        {
            // send handshake back
            byte[] handshake =
            {
                0x10, 0x14, // flags + length
                (byte) (_connection.SessionId >> 8), (byte) _connection.SessionId, // session id
                0x00, 0x00, // acked pkt id
                0x00, 0x00, // unknown
                0x00, 0x68, // unknown2
                0x00, 0x00, // server pkt id
                0x01, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00
            };

            Log.DebugFormat("Starting handshake");
            try
            {
                _client.Send(handshake, handshake.Length, _remoteEp);
            }
            catch (SocketException e)
            {
                Log.DebugFormat("Failed to send handshake: {0}", e.SocketErrorCode);
            }
            catch (ObjectDisposedException)
            {
            }
        }

        private void StartReceiving()
        {
            _receiveThread = new Thread(() =>
            {
                CancellationToken token = _cts.Token;
                while (_run && !token.IsCancellationRequested)
                {
                    byte[] data;
                    try
                    {
                        IPEndPoint ep = _remoteEp;
                        data = _client.Receive(ref ep);
                    }
                    catch (SocketException e) when (e.SocketErrorCode == SocketError.TimedOut)
                    {
                        continue;
                    }
                    catch (SocketException e)
                    {
                        if (!_run)
                            break;
                        Log.ErrorFormat("Socket Exception: {0}", e.SocketErrorCode);
                        continue;
                    }
                    catch (ObjectDisposedException)
                    {
                        break;
                    }

                    try
                    {
                        if (data.Length < ReceivedPacket.HeaderLength)
                            continue;

                        ReceivedPacket packet = new ReceivedPacket(data);

                        if (packet.CommandCode.HasFlag(ReceivedPacket.CommandCodeFlags.Handshake))
                        {
                            if (_awaitingHandshake)
                            {
                                Log.DebugFormat("Completed handshake");
                                DataTransfer.Reset();
                                _awaitingHandshake = false;
                            }
                            // A resent handshake reply means our ack was lost: ack it again
                            _connection.SendAckNow(_client.Client, true);
                            continue;
                        }

                        if (!AcceptSession(packet))
                            continue;

                        _connection.Receive(_client.Client, packet);
                    }
                    catch (ObjectDisposedException)
                    {
                        break;
                    }
                    catch (Exception e)
                    {
                        Log.ErrorFormat("Failed to handle received packet: {0}", e);
                    }
                }
            });
            _receiveThread.Name = "LibAtem.Receive";
            _receiveThread.IsBackground = true;
            _receiveThread.Start();
        }

        public void SendCommand(ICommand cmd)
        {
            _connection.QueueCommand(cmd);
        }

        public void DirectQueueMessage(OutboundMessage msg)
        {
            _connection.QueueMessage(msg);
        }

        private static void Raise(Action invoke, string name)
        {
            try
            {
                invoke();
            }
            catch (Exception e)
            {
                Log.ErrorFormat("{0} handler threw: {1}", name, e);
            }
        }

        /// <summary>
        /// Stops all threads and the timer, then closes the socket. No event fires once Dispose has started.
        /// Safe to call more than once, and from inside an event handler.
        /// </summary>
        public void Dispose()
        {
            lock (_lifecycleLock)
            {
                if (Interlocked.Exchange(ref _disposed, 1) != 0)
                    return;

                OnReceive = null;
                OnConnection = null;
                OnDisconnect = null;

                _run = false;
                _cts.Cancel();
            }

            bool fromTimer = _timerCallbackOwner == this;
            DisposeTimer(_timeoutTimer, !fromTimer);

            _connection.CompleteProcessing();

            // The receive thread must be out of Receive before the socket is closed
            bool joined = Join(_receiveThread);
            joined &= Join(_sendThread);
            joined &= Join(_handleThread);

            _client.Dispose();
            DataTransfer?.Dispose();

            // A thread still running (Dispose called from one of ours) may still read the token
            if (joined && !fromTimer)
                _cts.Dispose();
        }

        private static void DisposeTimer(Timer timer, bool waitForCallbacks)
        {
            if (timer == null)
                return;

            if (!waitForCallbacks)
            {
                timer.Dispose();
                return;
            }

            // Not disposed after the wait: the timer may still signal it if a callback overruns the timeout
            var done = new ManualResetEvent(false);
            if (timer.Dispose(done))
                done.WaitOne(ShutdownTimeout);
        }

        private static bool Join(Thread thread)
        {
            if (thread == null)
                return true;
            if (thread == Thread.CurrentThread)
                return false;
            if (thread.Join(ShutdownTimeout))
                return true;

            Log.WarnFormat("{0} did not stop within {1}ms", thread.Name, ShutdownTimeout.TotalMilliseconds);
            return false;
        }

        public bool HasQueuedOutbound()
        {
            return _connection.HasQueuedOutbound();
        }
    }
}
