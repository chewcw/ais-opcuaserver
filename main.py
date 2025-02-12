import asyncio
import random
import signal
from pathlib import Path

from server.OPCUAGatewayServer import OPCUAGatewayServer


async def value_simulator(server: OPCUAGatewayServer):
    """Simulates changing values every second"""
    while True:
        try:
            temperature_value = random.uniform(0, 100)
            server.update_value(
                namespace="DataCollectors",
                variable_name="Temperature",
                value=temperature_value,
            )

            await asyncio.sleep(1)
        except asyncio.CancelledError:
            print("Simulator shutdown")
            break
        except Exception as e:
            print(f"Error updating value: {e}")
            await asyncio.sleep(1)


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
    server = OPCUAGatewayServer()
    await server.init(Path("config.yaml"))

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

    # Create the simulator task before starting the server
    asyncio.create_task(value_simulator(server))

    # Start the server
    await server.start()


if __name__ == "__main__":
    asyncio.run(main())
