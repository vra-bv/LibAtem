using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using LibAtem.Net;
using Xunit;

namespace LibAtem.Test.Net
{
    public sealed class TestAtemConnection : IDisposable
    {
        private const int AckRequest = 0x01;
        private const int AckReply = 0x10;

        private readonly UdpClient _sink = new UdpClient(new IPEndPoint(IPAddress.Loopback, 0));
        private readonly Socket _socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
        private readonly AtemClientConnection _connection;
        private readonly List<uint> _delivered = new List<uint>();
        private long _now = 1_000_000;

        public TestAtemConnection()
        {
            _sink.Client.ReceiveTimeout = 200;
            _connection = new AtemClientConnection(_sink.Client.LocalEndPoint, 0x1234) { Clock = () => _now };
            _connection.ResetConnStatsInfo();
            _connection.OnReceivePacket += (sender, pkt) => _delivered.Add(pkt.PacketId);
        }

        public void Dispose()
        {
            _socket.Dispose();
            _sink.Dispose();
        }

        private void Receive(uint packetId, int flags = AckRequest) =>
            _connection.Receive(_socket, new ReceivedPacket(FakeAtem.Packet(0x8001, packetId, Array.Empty<byte>(), flags)));

        private List<uint> ReadAcks()
        {
            var acks = new List<uint>();
            try
            {
                while (true)
                {
                    var ep = new IPEndPoint(IPAddress.Any, 0);
                    byte[] data = _sink.Receive(ref ep);
                    if ((data[0] >> 3) == AckReply)
                        acks.Add((uint)((data[4] << 8) | data[5]));
                    _sink.Client.ReceiveTimeout = 50;
                }
            }
            catch (SocketException)
            {
            }
            return acks;
        }

        [Theory]
        [InlineData(59_500)]
        [InlineData(60_500)]
        [InlineData(120_500)]
        public void HasTimedOut_uses_the_total_silence_not_its_seconds_component(int silenceMs)
        {
            _now += 500;
            Assert.False(_connection.HasTimedOut);

            _now += silenceMs;
            Assert.True(_connection.HasTimedOut);
        }

        [Fact]
        public void Packets_after_a_gap_are_delivered_in_order_once_it_fills()
        {
            Receive(2);
            Receive(3);
            Receive(3);
            Receive(4);
            Assert.Empty(_delivered);
            Assert.Equal(3, _connection.OutOfOrderPackets);

            Receive(1);
            Assert.Equal(new uint[] { 1, 2, 3, 4 }, _delivered);
            Assert.Equal(4u, ReadAcks().Last());
            Assert.Equal(0, _connection.OutOfOrderPackets);

            Receive(2);
            Assert.Equal(new uint[] { 1, 2, 3, 4 }, _delivered);
        }

        [Fact]
        public void Out_of_order_buffering_works_across_the_packet_id_wrap()
        {
            for (uint id = 1; id <= 32766; id++)
                Receive(id);
            _delivered.Clear();

            Receive(0);
            Receive(1);
            Assert.Empty(_delivered);

            Receive(32767);
            Assert.Equal(new uint[] { 32767, 0, 1 }, _delivered);
        }

        [Fact]
        public void Packets_outside_the_window_or_beyond_15_bits_are_not_buffered()
        {
            Receive(1);
            Receive(300);
            Receive(2 + 32768);
            Assert.Equal(0, _connection.OutOfOrderPackets);

            Receive(2);
            Assert.Equal(new uint[] { 1, 2 }, _delivered);
        }

        [Fact]
        public void A_gap_that_never_fills_times_the_connection_out_and_a_reset_clears_the_buffer()
        {
            Receive(1);
            Receive(3);

            _now += 600;
            Receive(0, AckReply);
            Assert.False(_connection.HasTimedOut);

            _now += 500;
            Receive(0, AckReply);
            Assert.True(_connection.HasTimedOut);
            Assert.Equal(0, _connection.OutOfOrderPackets);

            _connection.ResetConnStatsInfo();
            Receive(2);
            Assert.Equal(1, _connection.OutOfOrderPackets);
            _connection.ResetConnStatsInfo();
            Assert.Equal(0, _connection.OutOfOrderPackets);
        }

        [Fact]
        public void PendingInboundPackets_counts_packets_not_yet_handled()
        {
            Receive(1);
            Receive(2);
            Receive(3);
            Assert.Equal(3, _connection.PendingInboundPackets);

            for (int i = 0; i < 3; i++)
                _connection.GetNextCommands();
            Assert.Equal(0, _connection.PendingInboundPackets);
        }

        [Fact]
        public void A_deferred_ack_is_flushed_after_AckInterval()
        {
            Receive(1);
            Assert.Equal(new uint[] { 1 }, ReadAcks());

            Receive(2);
            _connection.FlushPendingAck(_socket);
            Assert.Empty(ReadAcks());

            _now += AtemConstants.AckInterval;
            _connection.FlushPendingAck(_socket);
            Assert.Equal(new uint[] { 2 }, ReadAcks());
        }
    }
}
