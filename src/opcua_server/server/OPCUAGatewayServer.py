import logging
import queue
import subprocess
import os
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional
import asyncio

import yaml
from asyncua import Server, ua
from asyncua.common.node import Node
from asyncua.server.history import SubHandler
from asyncua.server.user_managers import UserManager
from asyncua.server.users import UserRole, User

from .NamespaceConfig import NamespaceConfig
from .NodeConfig import NodeConfig
from .PluginManager import PluginManager

logger = logging.getLogger(__name__)


class ConfigUserManager(UserManager):
    """User manager that authenticates users based on credentials from config file.

    This class extends the base UserManager to provide authentication using
    username/password pairs defined in the server configuration.
    """

    def __init__(self, users_config: List[Dict[str, str]]):
        """Initialize the user manager with config-defined users.

        Args:
            users_config: List of user dictionaries with username and password
        """
        super().__init__()
        self.users = {user["username"]: user["password"] for user in users_config}
        logger.info(f"User manager initialized with {len(self.users)} users")

    def validate_password(self, username: str, password: str) -> bool:
        """Validate user credentials.

        Args:
            username: The username to validate
            password: The password to validate

        Returns:
            bool: True if credentials are valid, False otherwise
        """
        if username in self.users and self.users[username] == password:
            return True
        return False

    def get_user(self, iserver, username=None, password=None, certificate=None):
        """Get user from the user database.

        Implements the method required by asyncua's UserManager interface.

        Args:
            iserver: Server instance
            username: The username of the user
            password: The password provided by the user
            certificate: The user certificate

        Returns:
            User object if credentials are valid, otherwise None
        """
        if username and password and self.validate_password(username, password):
            role = UserRole.Admin if username == "admin" else UserRole.User
            return User(role=role)

        return None


class DataChangeHandler(SubHandler):
    """Handler for data change notifications in OPC UA subscriptions.

    Implements the SubHandler interface to receive notifications when monitored values change.
    """

    def __init__(self, path):
        """Initialize the data change handler.

        Args:
            path: String path identifier for the monitored node
        """
        self.path = path

    def datachange_notification(self, node, val, data) -> None:
        """Called when a monitored value changes.

        Args:
            node: The node that changed
            val: The new value
            data: Additional data about the change
        """
        logger.info(f"Value changed for {self.path}: {val}")


class OPCUAGatewayServer:
    """OPC UA server implementation that acts as a gateway between plugins and clients.

    Manages server configuration, namespaces, nodes and plugin integration.
    """

    def __init__(self, config_path: Path):
        """Initialize the OPC UA server.

        Args:
            config_path: Path to the YAML configuration file
        """
        self.server = Server()
        self.data_queue = queue.Queue()
        self.nodes: Dict[str, Node] = {}
        self.namespaces: Dict[str, int] = {}
        self.plugin_manager = PluginManager()
        self.load_config(config_path)

    def _generate_certificate(
        self, cert_path: str, key_path: str, hostname: str | None = None
    ) -> bool:
        """Generate a self-signed certificate with OpenSSL.

        Creates certificates directory if it doesn't exist and generates
        a self-signed certificate with the proper Subject Alternative Names.

        Args:
            cert_path: Path to save the certificate
            key_path: Path to save the private key
            hostname: Server hostname, defaults to localhost if None

        Returns:
            bool: True if certificate generation succeeded, False otherwise
        """
        try:
            # Create directory if it doesn't exist
            cert_dir = os.path.dirname(cert_path)
            if not os.path.exists(cert_dir):
                os.makedirs(cert_dir)
                logger.info(f"Created certificates directory: {cert_dir}")

            try:
                cert_info = subprocess.run(
                    ["openssl", "x509", "-text", "-noout", "-in", cert_path],
                    check=True,
                    capture_output=True,
                    text=True,
                ).stdout
                
                # Check for required extensions
                extensions = cert_info.split('X509v3 extensions:')[1].split('Signature Algorithm:')[0].lower()
                if ('key usage' in extensions and 
                    'digital signature' in extensions and 
                    'key encipherment' in extensions and
                    'extended key usage' in extensions and 
                    'server auth' in extensions and
                    'client auth' in extensions):
                    logger.info("Existing certificate has required extensions")
                    return True
                else:
                    logger.warning("Existing certificate doesn't have required extensions. Regenerating...")
                    # Delete existing files to regenerate
                    os.remove(cert_path)
                    os.remove(key_path)
            except Exception as e:
                logger.warning(f"Failed to verify existing certificate: {e}. Regenerating...")
                # Delete existing files to regenerate
                if os.path.exists(cert_path):
                    os.remove(cert_path)
                if os.path.exists(key_path):
                    os.remove(key_path)

            # Get server hostname if not provided
            if not hostname:
                try:
                    hostname = subprocess.check_output(["hostname"], text=True).strip()
                except Exception:
                    hostname = "localhost"
                    logger.warning("Failed to get hostname, using 'localhost' instead")

            # Create temporary OpenSSL config
            config_path = os.path.join(cert_dir, "openssl.cnf")
            with open(config_path, "w") as f:
                f.write(f"""[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[req_distinguished_name]
C = MY
ST = YNY
O = YNY
CN = pyopcuaserver

[v3_req]
basicConstraints = critical, CA:true
keyUsage = critical, digitalSignature, keyEncipherment, dataEncipherment, keyCertSign, cRLSign
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = @alt_names

[alt_names]
URI.1 = urn:freeopcua:python:server
DNS.1 = localhost
DNS.2 = {hostname}
IP.1 = 127.0.0.1
""")

            # Generate private key
            subprocess.run(
                ["openssl", "genrsa", "-out", key_path, "2048"],
                check=True,
                capture_output=True,
                text=True,
            )

            # Generate certificate signing request
            subprocess.run(
                [
                    "openssl",
                    "req",
                    "-new",
                    "-key",
                    key_path,
                    "-out",
                    f"{cert_dir}/cert.csr",
                    "-config",
                    config_path,
                ],
                check=True,
                capture_output=True,
                text=True,
            )

            # Generate self-signed certificate with proper extensions
            subprocess.run(
                [
                    "openssl",
                    "x509",
                    "-req",
                    "-days",
                    "3650",
                    "-in",
                    f"{cert_dir}/cert.csr",
                    "-signkey",
                    key_path,
                    "-out",
                    cert_path,
                    "-extensions",
                    "v3_req",
                    "-extfile",
                    config_path,
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            
            # Verify certificate has proper extensions
            try:
                cert_info = subprocess.run(
                    ["openssl", "x509", "-text", "-noout", "-in", cert_path],
                    check=True,
                    capture_output=True,
                    text=True,
                ).stdout
                logger.info(f"Certificate generated successfully with extensions:\n{cert_info.split('X509v3 extensions:')[1].split('Signature Algorithm:')[0]}")
            except Exception as e:
                logger.error(f"Failed to verify generated certificate: {e}")
                return False

            # Set file permissions for private key
            os.chmod(key_path, 0o600)

            # Clean up CSR
            if os.path.exists(f"{cert_dir}/cert.csr"):
                os.remove(f"{cert_dir}/cert.csr")

            logger.info(f"Successfully generated certificate at {cert_path}")
            return True

        except Exception as e:
            logger.error(f"Failed to generate certificate: {str(e)}")
            return False

    async def __init_server(self):
        """Initialize server with configuration.

        Sets up server parameters including:
        - Security policies and authentication
        - Endpoints and certificates
        - Server identification properties

        This method should be called before starting the server.
        """
        # Get server config
        self.server_config = self.config["server"]

        # Set up user authentication if enabled in config
        if "user_manager" in self.server_config and self.server_config["user_manager"].get(
            "enabled", False
        ):
            users_config = self.server_config["user_manager"].get("users", [])
            if users_config:
                self.server = Server(user_manager=ConfigUserManager(users_config))
                logger.info("User authentication enabled")
            else:
                self.server = Server()
                logger.warning("User authentication enabled but no users defined")

        self.server.set_identity_tokens([ua.UserNameIdentityToken])

        # Generate certificates if needed
        if "certificate_path" in self.server_config and "private_key_path" in self.server_config:
            cert_path = self.server_config["certificate_path"]
            key_path = self.server_config["private_key_path"]
            
            # Force regenerate certificates for testing
            self._generate_certificate(cert_path, key_path)
            
            logger.info(f"Certificate path: {cert_path}")
            logger.info(f"Certificate exists: {os.path.exists(cert_path)}")
            logger.info(f"Private key exists: {os.path.exists(key_path)}")

        # Set security policies based on configuration
        security_policy = self.server_config.get("security_policy", "None")
        security_mode = self.server_config.get("security_mode", "None")
        
        logger.info(f"Security policy: {security_policy}")
        logger.info(f"Security mode: {security_mode}")
        
        # Map string security policy to enum
        security_policies = []
        
        if security_policy.lower() == "none":
            security_policies = [ua.SecurityPolicyType.NoSecurity]
        else:
            # Add NoSecurity for compatibility
            security_policies = [ua.SecurityPolicyType.NoSecurity]
            
            # Add configured security policy
            if "basic256sha256" in security_policy.lower():
                if security_mode.lower() == "sign":
                    security_policies.append(ua.SecurityPolicyType.Basic256Sha256_Sign)
                elif security_mode.lower() == "signandencrypt":
                    security_policies.append(ua.SecurityPolicyType.Basic256Sha256_SignAndEncrypt)
            elif "basic256" in security_policy.lower():
                if security_mode.lower() == "sign":
                    security_policies.append(ua.SecurityPolicyType.Basic256_Sign)
                elif security_mode.lower() == "signandencrypt":
                    security_policies.append(ua.SecurityPolicyType.Basic256_SignAndEncrypt)
        
        logger.info(f"Setting security policies: {security_policies}")
        self.server.set_security_policy(security_policies)

        self.server.set_endpoint(self.server_config["endpoint"])

        # Generate certificates if needed, then load them
        if "certificate_path" in self.server_config and "private_key_path" in self.server_config:
            # Ensure certificate exists before loading
            cert_path = self.server_config["certificate_path"]
            key_path = self.server_config["private_key_path"]

            # Generate certificates if they don't exist
            self._generate_certificate(cert_path, key_path)

            # Load certificates before server initialization
            try:
                await self.server.load_certificate(cert_path)
                await self.server.load_private_key(key_path)
                logger.info("Successfully loaded certificate and key")
            except Exception as e:
                logger.error(f"Error loading certificates: {e}")

        # Now initialize the server with certificates loaded
        await self.server.init()

        self.server.set_server_name(self.server_config["name"])

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

                self.server_config = config["server"]
                required_server_fields = ["endpoint", "uri", "name"]
                missing_fields = [
                    field
                    for field in required_server_fields
                    if field not in self.server_config
                ]
                if missing_fields:
                    raise KeyError(
                        f"Missing required server fields: {', '.join(missing_fields)}"
                    )

                # Set default values for optional server configurations
                self.server_config.setdefault("security_mode", "None")
                self.server_config.setdefault("security_policy", "None")
                self.server_config.setdefault("certificate_path", "certs/cert.pem")
                self.server_config.setdefault("private_key_path", "certs/key.pem")
                self.server_config.setdefault("max_clients", 100)
                self.server_config.setdefault("max_subscription_lifetime", 3600)
                self.server_config.setdefault("discovery_registration_interval", 60)
                self.server_config.setdefault("publish_interval_in_second", 1)

                self.config = config

                # Load plugins
                if "plugins" in config:
                    self.plugins = self.plugin_manager.load_plugins(config["plugins"])

                # Initialize server settings
                security_policy = self.server_config["security_policy"]
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
        """Convert string type to UA variant type.

        Maps common data type strings to their corresponding OPC UA VariantType.

        Args:
            type_str: String representation of the data type

        Returns:
            ua.VariantType: The corresponding OPC UA variant type.
            If no match is found, returns ua.VariantType.Variant as default.
        """
        type_mapping = {
            "double": ua.VariantType.Double,
            "float": ua.VariantType.Float,
            "int": ua.VariantType.Int32,
            "bool": ua.VariantType.Boolean,
            "string": ua.VariantType.String,
        }
        return type_mapping.get(type_str.lower(), ua.VariantType.Variant)

    async def init_namespace(self, ns_config: NamespaceConfig):
        """Initialize a namespace and its objects"""
        # Check if namespace already exists
        if ns_config.name in self.namespaces:
            raise ValueError(f"Namespace {ns_config.name} already exists")

        # Register namespace
        ns_idx = await self.register_namespace(ns_config.name)
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
        """
        Create a variable node with configuration.

        Creates a new variable node under the specified parent with the given configuration.
        Also sets up data change monitoring through subscriptions.

        Args:
            parent_node: The parent node under which to create the variable
            ns_idx: Namespace index for the new variable
            obj_config: Configuration object containing variable properties

        Returns:
            The created variable node
        """
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

    async def register_namespace(self, namespace: str) -> int:
        """Register a new namespace

        Args:
            namespace: Namespace URI

        Returns:
            int: Namespace index
        """
        return await self.server.register_namespace(namespace)

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

    async def create_or_get_folder_node(
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
            logger.info(f"Error creating/getting node {name}: {e}")
            raise

    async def create_or_get_variable_node(
        self, parent_node: Node, node_config: dict, ns_idx: int, value: Any = None
    ) -> Node:
        """Create a variable node based on configuration or get existing one

        Args:
            parent_node: Parent node
            node_config: Node configuration
            ns_idx: Namespace index
            value: Initial value for the variable

        Returns:
            Node: Created or existing variable node
        """
        try:
            # First try to find existing node by browsename
            for child in await parent_node.get_children():
                browse_name = await child.read_browse_name()
                if (
                    browse_name.Name == node_config["name"]
                    and browse_name.NamespaceIndex == ns_idx
                ):
                    # Update value if provided
                    if value is not None:
                        await child.write_value(value)
                    return child

            # Map configuration node type to UA types
            # The key is the type name in the configuration, the value is the corresponding UA type
            type_mapping = {
                # Scalar types
                "boolean": ua.VariantType.Boolean,  # 1
                "sbyte": ua.VariantType.SByte,  # 2
                "byte": ua.VariantType.Byte,  # 3
                "int16": ua.VariantType.Int16,  # 4
                "uint16": ua.VariantType.UInt16,  # 5
                "int32": ua.VariantType.Int32,  # 6
                "uint32": ua.VariantType.UInt32,  # 7
                "int64": ua.VariantType.Int64,  # 8
                "uint64": ua.VariantType.UInt64,  # 9
                "float": ua.VariantType.Float,  # 10
                "double": ua.VariantType.Double,  # 11
                "string": ua.VariantType.String,  # 12
                "datetime": ua.VariantType.DateTime,  # 13
                "variant": ua.VariantType.Variant,  # 24
                "extension_object": ua.VariantType.ExtensionObject,  # 22
                # Array types
                "array_float": ua.VariantType.Float,
                "array_double": ua.VariantType.Double,
                "array_int32": ua.VariantType.Int32,
                "array_int64": ua.VariantType.Int64,
            }

            # Determine data type
            node_type = node_config.get("type", "string").lower()
            if node_type.startswith("array_"):
                # Get base type for array
                base_type = node_type.replace("array_", "")
                ua_type = type_mapping.get(base_type, ua.VariantType.Variant)

                # Create the variable node with an empty list as initial value
                node = await parent_node.add_variable(
                    ns_idx,
                    node_config["name"],
                    [],
                    varianttype=ua_type,
                )
                # Mark as a one-dimensional array
                await node.write_value_rank(1)

                if value is not None and isinstance(value, (list, tuple)):
                    await node.write_array_dimensions(len(value))
                    await node.write_value(value, ua_type)

            else:
                # Handle scalar types
                ua_type = type_mapping.get(node_type, ua.VariantType.Variant)
                if value is None:
                    value = "" if ua_type == ua.VariantType.String else 0

                node = await parent_node.add_variable(
                    ns_idx,
                    node_config["name"],
                    value,
                    varianttype=ua_type,
                    datatype=ua_type,
                )

            # Set access level based on configuration
            if node_config.get("access", "r").lower() == "rw":
                await node.set_writable(True)

            return node

        except ua.UaError as e:
            logger.error(f"Error creating/getting variable node {node_config['name']}: {e}")
            raise

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
            logger.error(f"Error getting node path: {e}")
            return ""

    def update_value(self, namespace: str, variable_name: str, value: Any):
        """
        Update value for a specific variable.

        Updates the value of a node identified by namespace and variable name.
        Handles both direct namespace references and nested paths.

        Args:
            namespace: Namespace identifier or path segment
            variable_name: Name of the variable to update
            value: New value to set

        Note:
            The update is queued and processed asynchronously by the server.
        """
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

    async def start(self):
        """
        Start the OPC UA server.

        Initializes the server, loads plugins, and starts the main server loop.
        Handles graceful shutdown of plugins and tasks on cancellation.

        The server runs until explicitly stopped or cancelled.
        """
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
                        logger.error(f"Error initializing namespace {ns_config.name}: {e}")
                        continue

                # Run the plugin and store the task
                plugin_task = await plugin.start(self)
                if isinstance(plugin_task, (asyncio.Task, threading.Thread)):
                    plugin_tasks.append(plugin_task)
                    logger.info(f"Started plugin {name}")

            except Exception as e:
                logger.error(f"Error running plugin {name}: {e}")
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
                        logger.info(f"Stopped plugin {name}")
                    except Exception as e:
                        logger.info(f"Error stopping plugin {name}: {e}")

                # Cancel all plugin tasks
                for task in plugin_tasks:
                    if isinstance(task, asyncio.Task) and not task.done():
                        task.cancel()
                        try:
                            await task
                        except asyncio.CancelledError:
                            pass

    async def stop(self):
        """
        Stop the OPC UA server and cleanup resources.

        Cancels all running tasks and stops the server gracefully.
        Ensures proper cleanup of resources and shutdown of the server.
        """
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

            logger.info("Server stopped successfully")
        except Exception as e:
            logger.error(f"Error stopping server: {e}")
