import importlib.util
import logging
from pathlib import Path
from typing import Any, Dict, List

from plugins.interface import PluginInterface

logger = logging.getLogger(__name__)


class PluginLoadError(Exception):
    """Raised when plugin loading fails"""

    pass


class PluginManager:
    def __init__(self):
        self.plugins: Dict[str, Any] = {}

    def load_plugin(self, name: str, path: str) -> Any:
        """
        Dynamically load a plugin module from a file path

        Args:
            name: Plugin name
            path: Path to plugin module (can be relative or absolute)

        Returns:
            Loaded plugin module instance

        Raises:
            PluginLoadError: If plugin loading fails
        """
        try:
            # Handle both absolute and relative paths
            plugin_path = Path(path).resolve()
            if not plugin_path.exists():
                raise PluginLoadError(f"Plugin path does not exist: {plugin_path}")

            logger.info(f"Loading plugin {name} from {plugin_path}")

            # Import the module
            spec = importlib.util.spec_from_file_location(name, str(plugin_path))
            if spec is None or spec.loader is None:
                raise PluginLoadError(f"Could not load spec for plugin {name}")

            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            # Create plugin instance
            if not hasattr(module, "Plugin"):
                raise PluginLoadError(f"Plugin {name} does not have a Plugin class")

            plugin_instance = module.Plugin()
            self.plugins[name] = plugin_instance

            logger.info(f"Loaded plugin {name}")
            return plugin_instance

        except Exception as e:
            raise PluginLoadError(f"Failed to load plugin {name}: {str(e)}")

    def load_plugins(
        self, plugin_configs: List[Dict[str, str]]
    ) -> Dict[str, PluginInterface]:
        """
        Load multiple plugins from their configurations

        Args:
            plugin_configs: List of plugin configurations containing name and path

        Returns:
            Dictionary of loaded plugin instances
        """
        for plugin_config in plugin_configs:
            name = plugin_config.get("name")
            path = plugin_config.get("path")

            if not name or not path:
                logger.error(f"Invalid plugin configuration: {plugin_config}")
                continue

            try:
                self.load_plugin(name, path)
            except PluginLoadError as e:
                logger.error(f"Error loading plugin {name}: {e}")

        return self.plugins
