using System;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using LibAtem.Commands;
using LibAtem.Commands.DeviceProfile;
using LibAtem.Net;
using Xunit;

namespace LibAtem.Test.Net
{
    public sealed class TestAtemClientReconnect : IDisposable
    {
        private const ushort OldSession = 0x8001;
        private const ushort NewSession = 0x8002;

        private readonly UdpClient _atem = new UdpClient(new IPEndPoint(IPAddress.Loopback, 0));

        public void Dispose() => _atem.Dispose();

        private int Port => ((IPEndPoint)_atem.Client.LocalEndPoint).Port;

        private (IPEndPoint client, byte[] reply) WaitForHandshake()
        {
            _atem.Client.ReceiveTimeout = 50;
            var deadline = Stopwatch.StartNew();
            while (deadline.Elapsed < TimeSpan.FromSeconds(5))
            {
                try
                {
                    var ep = new IPEndPoint(IPAddress.Any, 0);
                    byte[] data = _atem.Receive(ref ep);
                    if (((data[0] >> 3) & 0x02) == 0)
                        continue;

                    var reply = new byte[20];
                    reply[0] = 0x10;
                    reply[1] = 20;
                    reply[2] = data[2];
                    reply[3] = data[3];
                    reply[12] = 0x02;
                    return (ep, reply);
                }
                catch (SocketException)
                {
                }
            }
            throw new TimeoutException("client sent no handshake");
        }

        private void Send(IPEndPoint to, byte[] data) => _atem.Send(data, data.Length, to);

        private void SendInit(IPEndPoint to, ushort session)
        {
            Send(to, FakeAtem.Packet(session, 1, new VersionCommand().ToByteArray()));
            Send(to, FakeAtem.Packet(session, 2, new InitializationCompleteCommand().ToByteArray()));
        }

        private static void WaitFor(Func<bool> condition, string what)
        {
            var deadline = Stopwatch.StartNew();
            while (!condition())
            {
                Assert.True(deadline.Elapsed < TimeSpan.FromSeconds(3), what);
                Thread.Sleep(10);
            }
        }

        [Fact]
        public void Resends_from_the_old_session_do_not_hijack_the_new_one_after_a_reconnect()
        {
            using var client = new AtemClient("127.0.0.1", Port, autoConnect: false);
            int inits = 0;
            client.OnConnection += sender => Interlocked.Increment(ref inits);
            client.Connect();

            var (ep, reply) = WaitForHandshake();
            Send(ep, reply);
            SendInit(ep, OldSession);
            for (uint id = 3; id <= 400; id++)
                Send(ep, FakeAtem.Packet(OldSession, id, Array.Empty<byte>()));
            WaitFor(() => Volatile.Read(ref inits) == 1, "first session never completed init");

            // The link drops: the client times out and handshakes again, while the ATEM is still
            // resending the old session's window
            (ep, reply) = WaitForHandshake();
            Send(ep, FakeAtem.Packet(OldSession, 401, Array.Empty<byte>()));
            Send(ep, reply);
            Send(ep, FakeAtem.Packet(OldSession, 402, Array.Empty<byte>()));
            SendInit(ep, NewSession);

            WaitFor(() => Volatile.Read(ref inits) == 2, "new session never completed init");
        }
    }
}
