using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using LibAtem.Net;
using Xunit;

namespace LibAtem.Test.Net
{
    public sealed class TestAtemClientLifecycle : IDisposable
    {
        private static readonly TimeSpan ConnectTimeout = TimeSpan.FromSeconds(5);

        private readonly ConcurrentQueue<object> _unhandled = new ConcurrentQueue<object>();

        public TestAtemClientLifecycle()
        {
            AppDomain.CurrentDomain.UnhandledException += OnUnhandled;
        }

        public void Dispose() => AppDomain.CurrentDomain.UnhandledException -= OnUnhandled;

        private void OnUnhandled(object sender, UnhandledExceptionEventArgs e) => _unhandled.Enqueue(e.ExceptionObject);

        private static AtemClient ConnectTo(FakeAtem atem)
        {
            var client = new AtemClient("127.0.0.1", atem.Port, autoConnect: false);
            var connected = new ManualResetEventSlim();
            client.OnConnection += sender => connected.Set();
            client.Connect();
            Assert.True(connected.Wait(ConnectTimeout), "fake ATEM did not complete init");
            return client;
        }

        private static void AssertStopped(AtemClient client)
        {
            Assert.False(client.ReceiveThread.IsAlive, "receive thread alive");
            Assert.False(client.SendThread.IsAlive, "send thread alive");
            Assert.False(client.HandleThread.IsAlive, "handle thread alive");
        }

        private static void AssertNoTrafficAfter(Func<int> received)
        {
            Thread.Sleep(100);
            int before = received();
            Thread.Sleep(500);
            Assert.Equal(before, received());
        }

        [Fact]
        public void Dispose_is_idempotent_and_Connect_after_Dispose_throws()
        {
            var client = new AtemClient("127.0.0.1", autoConnect: false);
            client.Dispose();
            client.Dispose();

            Assert.Throws<ObjectDisposedException>(() => client.Connect());
        }

        [Fact]
        public void A_silent_peer_gets_one_send_thread_and_Dispose_stops_all_threads_and_traffic()
        {
            using var atem = new FakeAtem(answer: false);
            var client = new AtemClient("127.0.0.1", atem.Port);

            // Several Reconnect cycles: 1 s timeout, then a handshake every 100 ms
            Thread.Sleep(3500);
            Assert.Equal(1, client.SendThreadsStarted);
            Assert.True(atem.Received > 1, "no reconnect handshakes were sent");

            var sw = Stopwatch.StartNew();
            client.Dispose();
            Assert.True(sw.Elapsed < TimeSpan.FromSeconds(1), $"Dispose took {sw.ElapsedMilliseconds}ms");

            AssertStopped(client);
            AssertNoTrafficAfter(() => atem.Received);
        }

        [Fact]
        public void Dispose_while_the_peer_floods_never_throws_on_a_LibAtem_thread()
        {
            using var atem = new FakeAtem(answer: true, keepaliveInterval: TimeSpan.FromMilliseconds(1));
            var client = ConnectTo(atem);
            client.OnReceive += (sender, cmds) => { };

            Thread.Sleep(300);
            client.Dispose();

            AssertStopped(client);
            AssertNoTrafficAfter(() => atem.Received);
            Assert.Empty(_unhandled);
        }

        [Fact]
        public async Task Dispose_from_inside_OnReceive_and_OnDisconnect_completes()
        {
            using (var atem = new FakeAtem(answer: true))
            {
                // The init dump is the first OnReceive; keepalives carry no commands
                var client = new AtemClient("127.0.0.1", atem.Port, autoConnect: false);
                var disposed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                client.OnReceive += (sender, cmds) =>
                {
                    client.Dispose();
                    disposed.TrySetResult(true);
                };
                client.Connect();
                Assert.Same(disposed.Task, await Task.WhenAny(disposed.Task, Task.Delay(ConnectTimeout)));
            }

            using (var atem = new FakeAtem(answer: true))
            {
                var client = ConnectTo(atem);
                var disposed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                client.OnDisconnect += sender =>
                {
                    client.Dispose();
                    disposed.TrySetResult(true);
                };
                atem.Silent = true;
                Assert.Same(disposed.Task, await Task.WhenAny(disposed.Task, Task.Delay(ConnectTimeout)));
                AssertStopped(client);
            }

            Assert.Empty(_unhandled);
        }

        [Fact]
        public void An_idle_connected_client_does_not_spin()
        {
            using var atem = new FakeAtem(answer: true, keepaliveInterval: TimeSpan.FromMilliseconds(500));
            using var client = ConnectTo(atem);

            Thread.Sleep(200);
            long before = client.SendLoopIterations;
            Thread.Sleep(1000);
            long iterations = client.SendLoopIterations - before;

            // The old loop slept 1 ms between empty polls: about 1000 iterations a second
            Assert.True(iterations < 100, $"{iterations} send loop iterations in 1 s");
        }
    }
}
