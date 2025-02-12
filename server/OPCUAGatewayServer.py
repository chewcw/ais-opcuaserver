import asyncio
import queue
from pathlib import Path
from typing import Any, Dict, List

import yaml
from asyncua import Server, ua
from asyncua.common.node import Node
from asyncua.server.history import SubHandler

from server.NamespaceConfig import NamespaceConfig
from server.NodeConfig import NodeConfig


class DataChangeHandler(SubHandler):
    def __init__(self, path):
        self.path = path

    def datachange_notification(self, node, val, data) -> None:
        print(f"Value changed for {self.path}: {val}")


class OPCUAGatewayServer:
    def __init__(self):
        self.server = Server()
        self.data_queue = queue.Queue()
        self.nodes: Dict[str, Node] = {}
        self.namespaces: Dict[str, int] = {}

    async def init(self, config_path: Path):
        """Initialize the server with configuration"""
        self.load_config(config_path)
        await self.__init_server()

    def load_config(self, config_path: Path):
        """Load server configuration from YAML file"""
        with open(config_path) as f:
            raw_config = yaml.safe_load(f)
            self.config = raw_config
            # Convert raw namespace configs to NamespaceConfig objects
            self.namespace_configs: List[NamespaceConfig] = []
            for ns in raw_config["namespaces"]:
                objects = [
                    NodeConfig(
                        name=obj["name"],
                        type=obj["type"],
                        initial_value=obj["initial_value"],
                        access=obj["access"],
                    )
                    for obj in ns["objects"]
                ]
                self.namespace_configs.append(
                    NamespaceConfig(name=ns["name"], objects=objects)
                )

    def get_ua_type(self, type_str: str) -> ua.VariantType:
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
        await self.server.init()
        await self.server.load_certificate("certs/cert.pem")
        await self.server.load_private_key("certs/key.pem")

        # Set server properties from config
        server_config = self.config["server"]
        self.server.set_endpoint(server_config["endpoint"])
        await self.server.set_application_uri(server_config["uri"])

        # Set server name
        server_node = self.server.nodes.server
        await server_node.write_attribute(
            ua.AttributeIds.DisplayName,
            ua.DataValue(ua.Variant(ua.LocalizedText(server_config["name"]))),
        )

        # Initialize configured namespaces
        for ns_config in self.namespace_configs:
            await self.init_namespace(ns_config)

    async def init_namespace(self, ns_config: NamespaceConfig):
        """Initialize a namespace and its objects"""
        # Register namespace
        ns_idx = await self.server.register_namespace(ns_config.name)
        self.namespaces[ns_config.name] = ns_idx

        # Add objects folder for this namespace
        objects_folder = await self.server.nodes.objects.add_folder(ns_idx, ns_config.name)

        # Create variables in the namespace
        for obj_config in ns_config.objects:
            await self.create_variable(objects_folder, ns_idx, obj_config)
            # Store node path mapping
            full_path = f"{ns_config.name}.{obj_config.name}"
            node = await objects_folder.get_child([f"{ns_idx}:{obj_config.name}"])
            self.nodes[full_path] = node

    async def create_variable(
        self, parent_node: Node, ns_idx: int, obj_config: NodeConfig
    ):
        """Create a variable node with configuration"""
        node = await parent_node.add_variable(
            ns_idx,
            obj_config.name,
            obj_config.initial_value,
            datatype=self.get_ua_type(obj_config.type),
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

    def update_value(self, namespace: str, variable_name: str, value: Any):
        """Update value for a specific variable"""
        full_path = f"{namespace}.{variable_name}"
        self.data_queue.put((full_path, value))

    async def value_updater(self):
        """Background task to update node values"""
        while True:
            while not self.data_queue.empty():
                full_path, value = self.data_queue.get()
                print(f"Processing update for {full_path}")  # Debug line
                if full_path in self.nodes:
                    node = self.nodes[full_path]
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
        asyncio.create_task(self.value_updater())
        async with self.server:
            while True:
                await asyncio.sleep(1)

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
