from abc import ABC, abstractmethod
from typing import Any


class PluginInterface(ABC):
    """Abstract interface for plugins that provide values to the OPC UA server"""

    @abstractmethod
    async def get_value(self, namespace: str, variable_name: str) -> Any:
        """
        Get a value for a specific variable in a namespace

        Args:
            namespace: The namespace name
            variable_name: The variable name within the namespace

        Returns:
            Any: The value for the specified variable
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
        
        Any cleanup necessary to prevent resource leaks should be done here.
        """
        pass
