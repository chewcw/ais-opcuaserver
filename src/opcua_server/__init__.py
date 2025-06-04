import asyncio
import logging
import signal
from pathlib import Path
from .server.OPCUAGatewayServer import OPCUAGatewayServer

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("opcua_gateway")


async def shutdown(server: OPCUAGatewayServer, signal=None):
    """Cleanup tasks tied to the service's shutdown."""
    if signal:
        logger.info(f"Received exit signal {signal.name}")

    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]

    for task in tasks:
        task.cancel()

    logger.info("Canceling outstanding tasks")
    await asyncio.gather(*tasks, return_exceptions=True)
    logger.info("Stopping server")
    await server.stop()
    logger.info("Shutdown complete")


async def async_main():
    # Try different possible config paths
    config_paths = [
        Path("config.yaml"),  # For local development
        Path("config/config.yaml"),  # Legacy path
        Path("src/opcua_server/config/config.yaml"),  # Development path
        Path(__file__).parent / "config" / "config.yaml",  # Relative to package
    ]
    
    config_path = None
    for path in config_paths:
        if path.exists():
            config_path = path
            break
    
    if config_path is None:
        raise FileNotFoundError(f"Config file not found. Tried: {[str(p) for p in config_paths]}")
    
    server = OPCUAGatewayServer(config_path)

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


def main():
    """Entry point for the console script."""
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
