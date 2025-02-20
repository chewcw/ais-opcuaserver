import asyncio
import logging
import queue
from pathlib import Path
import threading
from typing import Any, Dict, Optional

import yaml
from asyncua import Server, ua
from asyncua.common.node import Node
from asyncua.server.history import SubHandler

from server.NamespaceConfig import NamespaceConfig
from server.NodeConfig import NodeConfig
from server.PluginManager import PluginManager

logger = logging.getLogger(__name__)


class DataChangeHandler(SubHandler):
    def __init__(self, path):
        self.path = path

    def datachange_notification(self, node, val, data) -> None:
        print(f"Value changed for {self.path}: {val}")


class OPCUAGatewayServer:
    def __init__(self, config_path: Path):
        self.server = Server()
        self.data_queue = queue.Queue()
        self.nodes: Dict[str, Node] = {}
        self.namespaces: Dict[str, int] = {}
        self.plugin_manager = PluginManager()
        self.load_config(config_path)

    def load_config(self, config_path: Path):
        """Load server configuration from YAML file

        Args:
            config_path: Path to YAML configuration file

        Raises:
            FileNotFoundError: If config file doesn't exist
            KeyError: If required config fields are missing
        """
        try:
            with open(config_path) as f:
                config = yaml.safe_load(f)

                # Validate required server configuration
                if "server" not in config:
                    raise KeyError("Missing 'server' configuration")

                server_config = config["server"]
                required_server_fields = ["endpoint", "uri", "name"]
                missing_fields = [
                    field
                    for field in required_server_fields
                    if field not in server_config
                ]
                if missing_fields:
                    raise KeyError(
                        f"Missing required server fields: {', '.join(missing_fields)}"
                    )

                # Set default values for optional server configurations
                server_config.setdefault("security_mode", "None")
                server_config.setdefault("security_policy", "None")
                server_config.setdefault("certificate_path", "certs/cert.pem")
                server_config.setdefault("private_key_path", "certs/key.pem")
                server_config.setdefault("max_clients", 100)
                server_config.setdefault("max_subscription_lifetime", 3600)
                server_config.setdefault("discovery_registration_interval", 60)

                self.config = config

                # Load plugins
                if "plugins" in config:
                    self.plugins = self.plugin_manager.load_plugins(config["plugins"])

                # Initialize server settings
                security_policy = server_config["security_policy"]
                if security_policy.lower() == "none":
                    self.server.set_security_policy([])  # Empty list for no security
                else:
                    self.server.set_security_policy(
                        [ua.SecurityPolicyType[security_policy]]
                    )

        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML format in config file: {e}")
        except Exception as e:
            raise Exception(f"Error loading configuration: {str(e)}")

    def _get_ua_type(self, type_str: str) -> ua.VariantType:
        """Convert string type to UA variant type"""
        type_mapping = {
            "double": ua.VariantType.Double,
            "float": ua.VariantType.Float,
            "int": ua.VariantType.Int32,
            "bool": ua.VariantType.Boolean,
            "string": ua.VariantType.String,
        }
        return type_mapping.get(type_str.lower(), ua.VariantType.Variant)

    async def __init_server(self):
        """Initialize server with configuration"""
        # Get server config
        server_config = self.config["server"]

        # Set up server parameters before initialization
        self.server.set_security_IDs(["Anonymous", "Basic256Sha256"])

        # Add these lines for anonymous access
        self.server.set_security_policy([])  # Empty list means no security
        await self.server.init()

        # Allow anonymous access
        self.server.set_endpoint(server_config["endpoint"])
        self.server.set_security_policy([ua.SecurityPolicyType.NoSecurity])

        # Load certificates if needed
        if "certificate_path" in server_config and "private_key_path" in server_config:
            await self.server.load_certificate(server_config["certificate_path"])
            await self.server.load_private_key(server_config["private_key_path"])

        # Set server properties from config
        await self.server.set_application_uri(server_config["uri"])

        # Get server node
        server_node = self.server.get_node(ua.NodeId(ua.Int32(ua.ObjectIds.Server)))

        # Set server name using proper node
        name_node = await server_node.get_child(
            ["0:ServerStatus", "0:BuildInfo", "0:ProductName"]
        )
        await name_node.write_value(server_config["name"])

    async def init_namespace(self, ns_config: NamespaceConfig):
        """Initialize a namespace and its objects"""
        # Check if namespace already exists
        if ns_config.name in self.namespaces:
            raise ValueError(f"Namespace {ns_config.name} already exists")

        # Register namespace
        ns_idx = await self.server.register_namespace(ns_config.name)
        self.namespaces[ns_config.name] = ns_idx

        # Add objects folder for this namespace
        objects_folder = await self.server.nodes.objects.add_folder(
            ns_idx, ns_config.name
        )

        # Create variables in the namespace
        for obj_config in ns_config.objects:
            await self.create_variable(objects_folder, ns_idx, obj_config)
            # Store node path mapping
            full_path = f"{ns_config.name}.{obj_config.name}"
            node = await objects_folder.get_child([f"{ns_idx}:{obj_config.name}"])
            self.nodes[full_path] = node

    def get_namespace_index(self, namespace: str) -> Optional[int]:
        """Get the index of an existing namespace

        Args:
            namespace: Namespace name

        Returns:
            int or None: Namespace index if found, None otherwise
        """
        return self.namespaces.get(namespace)

    async def create_variable(
        self, parent_node: Node, ns_idx: int, obj_config: NodeConfig
    ):
        """Create a variable node with configuration"""
        node = await parent_node.add_variable(
            ns_idx,
            obj_config.name,
            obj_config.initial_value,
            datatype=self._get_ua_type(obj_config.type),
        )

        # Set access level
        access = obj_config.access.lower()
        if access == "rw":
            await node.set_writable(True)
        elif access == "r":
            await node.set_writable(False)

        # Store node reference
        browse_name = await parent_node.read_browse_name()
        full_path = f"{browse_name.Name}.{obj_config.name}"
        self.nodes[full_path] = node

        # Create subscription with proper handler
        handler = DataChangeHandler(full_path)
        subscription = await self.server.create_subscription(100, handler)
        await subscription.subscribe_data_change(node)

    # async def create_variable2(
    #     self, parent_node: Node, ns_idx: int, obj_config: NodeConfig
    # ):
    #     """Create a variable node with configuration"""
    #     if obj_config.type.lower() == "object":
    #         # Create a folder for objects
    #         node = await parent_node.add_folder(ns_idx, obj_config.name)
    #
    #         # Create child nodes if they exist
    #         if hasattr(obj_config, "children"):
    #             for child_config in obj_config.children:
    #                 await self.create_variable(node, ns_idx, child_config)
    #     else:
    #         # Create regular variable node
    #         node = await parent_node.add_variable(
    #             ns_idx,
    #             obj_config.name,
    #             obj_config.initial_value,
    #             datatype=self.get_ua_type(obj_config.type),
    #         )
    #
    #         # Set access level
    #         access = obj_config.access.lower()
    #         if access == "rw":
    #             await node.set_writable(True)
    #         elif access == "r":
    #             await node.set_writable(False)
    #
    #     # Store node reference with full path
    #     parent_path = await self.get_node_path(parent_node)
    #     full_path = f"{parent_path}.{obj_config.name}"
    #     self.nodes[full_path] = node
    #
    #     # Create subscription with proper handler
    #     if obj_config.type.lower() != "object":
    #         handler = DataChangeHandler(full_path)
    #         subscription = await self.server.create_subscription(100, handler)
    #         await subscription.subscribe_data_change(node)

    async def register_namespace(self, namespace: str) -> int:
        """Register a new namespace

        Args:
            namespace: Namespace URI

        Returns:
            int: Namespace index
        """
        return await self.server.register_namespace(namespace)

    async def get_namespaces_array(self) -> list:
        """Get array of registered namespaces.

        Returns:
            list: List of namespace URIs as strings
        """
        try:
            # Get the namespace array from the server
            return await self.server.get_namespace_array()
        except Exception as e:
            print(f"Error getting namespace array: {e}")
            return []

    async def create_folder(
        self, ns_idx: int, parent: Node, name: str, description: str
    ) -> Node:
        """Create a folder node

        Args:
            ns_idx: Namespace index
            parent: Parent node
            name: Folder name
            description: Optional folder description

        Returns:
            Node: Created folder node
        """
        # Create a NodeConfig for the folder
        folder_config = NodeConfig(
            name=name, type="object", access="r", initial_value=None
        )

        # Create folder with a unique NodeId
        folder = await parent.add_folder(ns_idx, folder_config.name)

        if description:
            await folder.write_attribute(
                ua.AttributeIds.Description,
                ua.DataValue(ua.Variant(ua.LocalizedText(description))),
            )

        # Store node reference with full path
        parent_path = await self.get_node_path(parent)
        full_path = f"{parent_path}.{folder_config.name}"
        self.nodes[full_path] = folder

        return folder

    async def _create_or_get_node(
        self, parent_node: Node, name: str, ns_idx: int
    ) -> Node:
        """Create a new node or get existing node.

        Args:
            parent_node: Parent node under which to create/get child
            name: Name of the node
            ns_idx: Namespace index

        Returns:
            Node: Created or existing node
        """
        try:
            # First try to find the node by browsename
            for child in await parent_node.get_children():
                browse_name = await child.read_browse_name()
                if browse_name.Name == name and browse_name.NamespaceIndex == ns_idx:
                    return child

            # If not found, create new folder node
            return await self.create_folder(ns_idx, parent_node, name, "")

        except ua.UaError as e:
            # Create new folder node if it doesn't exist
            # return await self.create_folder(ns_idx, parent_node, name, "")
            print(f"Error creating/getting node {name}: {e}")
            raise

    async def create_variable_node(
        self, parent_node: Node, node_config: dict, ns_idx: int, value: Any = None
    ) -> Node:
        """Create a variable node based on configuration

        Args:
            parent_node: Parent node
            node_config: Node configuration
            ns_idx: Namespace index
            value: Initial value for the variable

        Returns:
            Node: Created variable node
        """
        node_type = node_config.get("type", "string")

        # Map node types to UA types
        type_mapping = {
            "string": ua.VariantType.String,
            "integer": ua.VariantType.Int32,
            "double": ua.VariantType.Double,
            "json_string": ua.VariantType.String,
            "bool": ua.VariantType.Boolean,
        }

        ua_type = type_mapping.get(node_type, ua.VariantType.String)

        # Create variable node
        node = await parent_node.add_variable(
            ns_idx,
            node_config["name"],
            value if value is not None else "",
            ua_type,
        )

        # Set access level based on configuration
        if node_config.get("access") == "rw":
            await node.set_writable(True)

        return node

    async def get_node_path(self, node: Node) -> str:
        """Get the full path of a node in string format (e.g. 'Objects.MyFolder.MyVariable')

        Args:
            node: The node to get the path for

        Returns:
            str: The full path of the node
        """
        try:
            if node == self.server.nodes.objects:
                return "Objects"

            browse_name = await node.read_browse_name()
            parent = await node.get_parent()

            if parent:
                parent_path = await self.get_node_path(parent)
                return f"{parent_path}.{browse_name.Name}"

            return browse_name.Name

        except Exception as e:
            print(f"Error getting node path: {e}")
            return ""

    def update_value(self, namespace: str, variable_name: str, value: Any):
        """Update value for a specific variable"""
        # Remove the namespace index if present (e.g., "0:" becomes "")
        if ":" in namespace:
            namespace = namespace.split(":")[1]

        # Handle both namespace and nested paths
        if namespace:
            full_path = f"{namespace}.{variable_name}"
        else:
            full_path = variable_name

        # Normalize the path (handle multiple dots)
        full_path = ".".join(part for part in full_path.split(".") if part)
        self.data_queue.put((full_path, value))

    async def value_updater(self):
        """Background task to update node values"""
        while True:
            while not self.data_queue.empty():
                full_path, value = self.data_queue.get()
                print(f"Processing update for {full_path}")  # Debug line

                node = None
                if full_path in self.nodes:
                    node = self.nodes[full_path]

                if node:
                    try:
                        await node.write_value(value)
                        print(f"Updated {full_path} with value: {value}")
                    except ua.UaError as e:
                        print(f"Error updating {full_path}: {e}")
                else:
                    print(f"Node not found: {full_path}")  # Debug line

            await asyncio.sleep(0.1)

    async def start(self):
        """Start the OPC UA server"""
        # Initialize the server
        await self.__init_server()

        # Initialize and run plugins
        plugin_tasks = []
        for name, plugin in self.plugins.items():
            try:
                # Get namespace config
                ns_config = plugin.get_namespace()
                if ns_config:
                    try:
                        await self.init_namespace(ns_config)
                    except ValueError as e:
                        print(f"Error initializing namespace {ns_config.name}: {e}")
                        continue

                # Run the plugin and store the task
                plugin_task = await plugin.start(self)
                if isinstance(plugin_task, (asyncio.Task, threading.Thread)):
                    plugin_tasks.append(plugin_task)
                    print(f"Started plugin {name}")

            except Exception as e:
                print(f"Error running plugin {name}: {e}")
                continue

        # Run the server
        async with self.server:
            try:
                while True:
                    await asyncio.sleep(1)
            except asyncio.CancelledError:
                # Stop all plugins
                for name, plugin in self.plugins.items():
                    try:
                        await plugin.stop()
                        print(f"Stopped plugin {name}")
                    except Exception as e:
                        print(f"Error stopping plugin {name}: {e}")

                # Cancel all plugin tasks
                for task in plugin_tasks:
                    if isinstance(task, asyncio.Task) and not task.done():
                        task.cancel()
                        try:
                            await task
                        except asyncio.CancelledError:
                            pass

    async def stop(self):
        """Stop the OPC UA server and cleanup resources"""
        try:
            # Cancel the value updater task if it exists
            tasks = [
                t
                for t in asyncio.all_tasks()
                if t is not asyncio.current_task() and not t.done()
            ]
            for task in tasks:
                task.cancel()

            # Wait for tasks to complete
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

            # Stop the server
            await self.server.stop()

            print("Server stopped successfully")
        except Exception as e:
            print(f"Error stopping server: {e}")
