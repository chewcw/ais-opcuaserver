import asyncio
import signal
from pathlib import Path

from server.OPCUAGatewayServer import OPCUAGatewayServer


async def shutdown(server: OPCUAGatewayServer, signal=None):
    """Cleanup tasks tied to the service's shutdown."""
    if signal:
        print(f"Received exit signal {signal.name}")

    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]

    for task in tasks:
        task.cancel()

    print("Canceling outstanding tasks")
    await asyncio.gather(*tasks, return_exceptions=True)
    print("Stopping server")
    await server.stop()
    print("Shutdown complete")


async def main():
    server = OPCUAGatewayServer(Path("config.yaml"))

    # Handle signals
    loop = asyncio.get_running_loop()
    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)

    # Create a shutdown event to prevent multiple shutdowns
    shutdown_event = asyncio.Event()

    def signal_handler(s):
        if not shutdown_event.is_set():
            shutdown_event.set()
            asyncio.create_task(shutdown(server, signal=s))

    for s in signals:
        loop.add_signal_handler(s, lambda s=s: signal_handler(s))

    # Start the server
    try:
        await server.start()
    except asyncio.CancelledError:
        pass


if __name__ == "__main__":
    asyncio.run(main())
