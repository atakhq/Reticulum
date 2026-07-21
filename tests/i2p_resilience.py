import asyncio
import threading
import types
import unittest
from unittest import mock

import RNS
from RNS.Interfaces.I2PInterface import I2PController, I2PRetryPolicy
from RNS.vendor.i2plib import exceptions as i2p_exceptions
from RNS.vendor.i2plib.tunnel import ClientTunnel


class _FakeWriter:
    def __init__(self):
        self.closed = False
        self.waited = False

    def close(self):
        self.closed = True

    async def wait_closed(self):
        self.waited = True


class _FakeServer(_FakeWriter):
    pass


class _FakeTunnel:
    def __init__(self):
        self.closed = False
        self.close_thread = None

    async def close(self):
        self.close_thread = threading.get_ident()
        self.closed = True


class _SetupFailedTunnel(_FakeTunnel):
    created = []

    def __init__(self, *args, **kwargs):
        super().__init__()
        self.status = {
            "setup_ran": True,
            "setup_failed": False,
            "exception": i2p_exceptions.InvalidKey(),
        }
        self.created.append(self)

    async def run(self):
        return None


class TestI2PRetryPolicy(unittest.TestCase):
    def setUp(self):
        self.policy = I2PRetryPolicy(random_source=lambda lower, upper: 1.0)

    def test_transient_failures_back_off_to_cap(self):
        delays = [self.policy.next_delay(permanent=False)[0] for _ in range(7)]
        self.assertEqual(delays, [15, 30, 60, 120, 240, 300, 300])

    def test_permanent_failures_open_circuit_after_three_attempts(self):
        results = [self.policy.next_delay(permanent=True) for _ in range(6)]
        self.assertEqual(
            results,
            [(15, False), (30, False), (900, True), (1800, True), (3600, True), (3600, True)],
        )

    def test_stable_connection_resets_failure_history(self):
        self.policy.next_delay(permanent=True)
        self.policy.next_delay(permanent=True)
        delay, circuit_open = self.policy.next_delay(
            permanent=False,
            connected_seconds=I2PRetryPolicy.STABLE_CONNECTION_SECONDS,
        )
        self.assertEqual(delay, 15)
        self.assertFalse(circuit_open)
        self.assertEqual(self.policy.failures, 1)


class TestI2PTunnelCleanup(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()

    def tearDown(self):
        self.loop.close()

    def test_client_close_waits_for_resources_and_cancels_tasks(self):
        tunnel = ClientTunnel("peer.b32.i2p", ("127.0.0.1", 0), loop=self.loop)
        writer = _FakeWriter()
        tunnel.session_writer = writer
        tunnel.server = _FakeServer()
        task = tunnel._track_task(self.loop.create_task(asyncio.sleep(60)))

        self.loop.run_until_complete(tunnel.close())

        self.assertTrue(tunnel.session_writer is None)
        self.assertTrue(writer.closed)
        self.assertTrue(writer.waited)
        self.assertTrue(tunnel.server.closed)
        self.assertTrue(tunnel.server.waited)
        self.assertTrue(task.cancelled())
        self.assertTrue(tunnel._closed)

    def test_controller_closes_on_owner_loop_and_removes_state(self):
        loop = asyncio.new_event_loop()
        ready = threading.Event()
        loop_thread_id = []

        def run_loop():
            asyncio.set_event_loop(loop)
            loop_thread_id.append(threading.get_ident())
            ready.set()
            loop.run_forever()

        thread = threading.Thread(target=run_loop)
        thread.start()
        ready.wait(timeout=2)

        controller = I2PController.__new__(I2PController)
        controller.loop = loop
        tunnel = _FakeTunnel()
        controller.i2plib_tunnels = {"peer": tunnel}
        controller.client_tunnels = {"peer": True}
        controller.server_tunnels = {}

        try:
            controller.stop_tunnel(tunnel)
            self.assertTrue(tunnel.closed)
            self.assertEqual(tunnel.close_thread, loop_thread_id[0])
            self.assertNotIn("peer", controller.i2plib_tunnels)
            self.assertNotIn("peer", controller.client_tunnels)
        finally:
            loop.call_soon_threadsafe(loop.stop)
            thread.join(timeout=2)
            loop.close()

    def test_repeated_failed_tunnel_cleanup_leaves_no_live_tasks(self):
        writers = []
        servers = []

        async def exercise():
            for _ in range(100):
                tunnel = ClientTunnel("invalid.b32.i2p", ("127.0.0.1", 0), loop=self.loop)
                writer = _FakeWriter()
                server = _FakeServer()
                tunnel.session_writer = writer
                tunnel.server = server
                tunnel._track_task(self.loop.create_task(asyncio.sleep(60)))
                await tunnel.close()
                writers.append(writer)
                servers.append(server)

        self.loop.run_until_complete(exercise())

        self.assertTrue(all(writer.closed and writer.waited for writer in writers))
        self.assertTrue(all(server.closed and server.waited for server in servers))
        self.assertFalse([task for task in asyncio.all_tasks(self.loop) if not task.done()])

    def test_repeated_controller_setup_failures_remove_tunnel_state(self):
        loop = asyncio.new_event_loop()
        ready = threading.Event()

        def run_loop():
            asyncio.set_event_loop(loop)
            ready.set()
            loop.run_forever()

        thread = threading.Thread(target=run_loop)
        thread.start()
        ready.wait(timeout=2)

        controller = I2PController.__new__(I2PController)
        controller.loop = loop
        controller.sam_address = ("127.0.0.1", 7656)
        controller.i2plib = types.SimpleNamespace(ClientTunnel=_SetupFailedTunnel)
        controller.i2plib_tunnels = {}
        controller.client_tunnels = {}
        controller.server_tunnels = {}
        controller.client_tunnel_errors = {}
        controller.client_tunnel_online_since = {}
        owner = types.SimpleNamespace(socket=None, awaiting_i2p_tunnel=True, local_addr=("127.0.0.1", 1))
        destination = "invalid.b32.i2p"
        _SetupFailedTunnel.created = []

        try:
            with mock.patch.object(RNS, "log"), mock.patch(
                "RNS.Interfaces.I2PInterface.time.sleep", return_value=None
            ):
                for _ in range(100):
                    self.assertFalse(controller.client_tunnel(owner, destination))
                    self.assertNotIn(destination, controller.i2plib_tunnels)
                    self.assertNotIn(destination, controller.client_tunnels)

            self.assertEqual(len(_SetupFailedTunnel.created), 100)
            self.assertTrue(all(tunnel.closed for tunnel in _SetupFailedTunnel.created))
            self.assertIsInstance(
                controller.get_client_tunnel_error(destination),
                i2p_exceptions.InvalidKey,
            )
        finally:
            loop.call_soon_threadsafe(loop.stop)
            thread.join(timeout=2)
            loop.close()


if __name__ == "__main__":
    unittest.main()
