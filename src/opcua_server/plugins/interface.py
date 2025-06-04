from abc import ABC, abstractmethod
from asyncio import Task
from typing import Any, Optional, Union

from ..server.NamespaceConfig import NamespaceConfig


class PluginInterface(ABC):
    """Abstract interface for plugins that provide values to the OPC UA server
    
    This interface defines the core functionality that must be implemented by any plugin
    that wishes to interact with the OPC UA Gateway Server. It provides methods for
    namespace configuration, startup, and shutdown operations.

    Methods:
        get_namespace: Returns the namespace configuration for the plugin
        start: Initializes and runs the main plugin logic
        stop: Performs cleanup and shutdown operations
    """

    @abstractmethod
    def get_namespace(self) -> Optional[NamespaceConfig]:
        """Return namespace configuration for the plugin
        
        Returns:
            Optional[NamespaceConfig]: The namespace configuration for this plugin,
            or None if the plugin doesn't require a specific namespace.
        """
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

        Returns:
            Union[None, Task[None]]: Either None or an asyncio Task that represents
            the plugin's ongoing operations.

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
