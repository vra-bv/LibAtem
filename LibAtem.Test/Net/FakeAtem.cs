using System;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using LibAtem.Commands;
using LibAtem.Commands.DeviceProfile;

namespace LibAtem.Test.Net
{
    /// <summary>
    /// A minimal ATEM on a loopback port. With answer false it only counts datagrams, like an unreachable
    /// switcher. With answer true it completes the handshake, sends a version and InitializationComplete,
    /// then streams ack-requested keepalives. Silent simulates a link drop.
    /// </summary>
    internal sealed class FakeAtem : IDisposable
    {
        private readonly UdpClient _udp;
        private readonly bool _answer;
        private readonly TimeSpan _keepalive;
        private readonly CancellationTokenSource _cts = new CancellationTokenSource();
        private readonly Thread _thread;
        private int _received;
        private volatile bool _silent;

        public FakeAtem(bool answer, TimeSpan? keepaliveInterval = null)
        {
            _answer = answer;
            _keepalive = keepaliveInterval ?? TimeSpan.FromMilliseconds(50);
            _udp = new UdpClient(new IPEndPoint(IPAddress.Loopback, 0));
            _thread = new Thread(Run) { IsBackground = true, Name = "FakeAtem" };
            _thread.Start();
        }

        public int Port => ((IPEndPoint)_udp.Client.LocalEndPoint).Port;

        public int Received => Volatile.Read(ref _received);

        public bool Silent
        {
            get => _silent;
            set => _silent = value;
        }

        private void Run()
        {
            IPEndPoint client = null;
            const ushort session = 0x8001;
            uint nextId = 1;
            var lastKeepalive = Stopwatch.StartNew();
            _udp.Client.ReceiveTimeout = 1;
            while (!_cts.IsCancellationRequested)
            {
                try
                {
                    var ep = new IPEndPoint(IPAddress.Any, 0);
                    byte[] data = _udp.Receive(ref ep);
                    Interlocked.Increment(ref _received);
                    if (_answer && !_silent && data.Length >= 12 && ((data[0] >> 3) & 0x02) != 0)
                    {
                        client = ep;
                        nextId = 1;
                        var hs = new byte[20];
                        hs[0] = 0x10;
                        hs[1] = 20;
                        hs[2] = data[2];
                        hs[3] = data[3];
                        hs[12] = 0x02;
                        Send(hs, client);
                        Send(Packet(session, nextId++, new VersionCommand().ToByteArray()), client);
                        Send(Packet(session, nextId++, new InitializationCompleteCommand().ToByteArray()), client);
                    }
                }
                catch (SocketException)
                {
                }
                catch (ObjectDisposedException)
                {
                    return;
                }

                if (_answer && !_silent && client != null && lastKeepalive.Elapsed >= _keepalive)
                {
                    Send(Packet(session, nextId, Array.Empty<byte>()), client);
                    nextId = (nextId + 1) % 32768;
                    lastKeepalive.Restart();
                }
            }
        }

        public static byte[] Packet(ushort session, uint id, byte[] payload, int flags = 0x01)
        {
            int len = 12 + payload.Length;
            var b = new byte[len];
            b[0] = (byte)((flags << 3) | (len >> 8));
            b[1] = (byte)len;
            b[2] = (byte)(session >> 8);
            b[3] = (byte)session;
            b[10] = (byte)(id >> 8);
            b[11] = (byte)id;
            payload.CopyTo(b, 12);
            return b;
        }

        private void Send(byte[] data, IPEndPoint to)
        {
            try
            {
                _udp.Send(data, data.Length, to);
            }
            catch (SocketException)
            {
            }
        }

        public void Dispose()
        {
            _cts.Cancel();
            _thread.Join(TimeSpan.FromSeconds(2));
            _udp.Dispose();
        }
    }
}
