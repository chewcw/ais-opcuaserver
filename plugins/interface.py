from abc import ABC, abstractmethod
from asyncio import Task
from typing import TYPE_CHECKING, Any, Coroutine, Optional, Union

from server.NamespaceConfig import NamespaceConfig

if TYPE_CHECKING:
    pass


class PluginInterface(ABC):
    """Abstract interface for plugins that provide values to the OPC UA server"""

    @abstractmethod
    def get_namespace(self) -> Optional[NamespaceConfig]:
        """Return namespace configuration for the plugin"""
        pass

    @abstractmethod
    async def start(self, server: Any) -> Union[None, Task[None]]:
        """
        Execute the main plugin logic

        This method should implement the core functionality of the plugin, such as:
        - Collecting data from sources
        - Processing information
        - Updating values in the OPC UA server
        - Handling any periodic tasks

        Args:
            server: The OPC UA Gateway Server instance to work with

        The method should run continuously until the stop() method is called.
        """
        pass

    @abstractmethod
    async def stop(self):
        """Stop the plugin and perform cleanup operations.

        This method should handle the graceful shutdown of the plugin, including:
        - Closing any open connections
        - Releasing system resources
        - Saving any pending state
        - Cancelling any running tasks
        - Unregistering from the OPC UA server

        Any cleanup necessary to prevent resource leaks should be done here.
        """
        pass
