import asyncio
import json
import sqlite3
from asyncio.tasks import Task
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

import psycopg2
import yaml
from asyncua import Node
from psycopg2.extras import DictCursor

from plugins.aissens_sqldb.map_validator import MapValidator
from plugins.interface import PluginInterface
from server.NamespaceConfig import NamespaceConfig

if TYPE_CHECKING:
    pass


class Plugin(PluginInterface):
    """AISSENS with sql based database plugin for OPC UA server"""

    def __init__(self):
        """Initialize the AissensSqlDb plugin"""
        super().__init__()

        # Database connection settings
        self.conn = None
        self.running = False

        # Configuration storage
        self.db_config = {}
        self.db_tables_config = []
        self.db_tag_config = []
        self.db_polling_interval_in_second = 10
        self.opcua_config = {}
        self.opcua_tag_config = []

        # Load and validate config
        self.config = self._load_config(Path(__file__).parent / "config.yaml")

        # If using SQLite, store the path
        if self.db_config["type"] == "sqlite":
            self.database_path = Path(self.db_config["path"])

    def get_namespace(self) -> NamespaceConfig:
        """Create namespace from config"""
        nodes = []
        return NamespaceConfig(name=self.opcua_config["namespace"], objects=nodes)

    def _load_config(self, config_path: Path):
        """Load server configuration from YAML file"""
        try:
            with open(config_path) as f:
                self.config = yaml.safe_load(f)

                # Validate required sections
                required_sections = ["database", "opcua"]
                for section in required_sections:
                    if section not in self.config:
                        raise ValueError(f"Missing required section: {section}")

                # Store configurations
                self.db_config = self.config["database"]
                self.db_tables_config = self.db_config["tables"]
                self.db_tag_config = self.db_config["tag"]
                self.opcua_config = self.config["opcua"]

                # Validate database configuration
                if "type" not in self.db_config:
                    raise ValueError("Database type not specified")

                # Validate tables configuration
                if not isinstance(self.db_tables_config, list):
                    raise ValueError("Tables configuration must be a list")

                for table in self.db_tables_config:
                    if "name" not in table or "columns" not in table:
                        raise ValueError(
                            "Each table must have name and columns defined"
                        )

                # Validate tag configuration
                if not isinstance(self.db_tag_config, list):
                    raise ValueError("Tag configuration must be a list")

                for tag in self.db_tag_config:
                    required_tag_fields = [
                        "map_logic",
                        "table_map",
                        "column_map",
                        "opcua_folder_name",
                    ]
                    missing_fields = [
                        field for field in required_tag_fields if field not in tag
                    ]
                    if missing_fields:
                        raise ValueError(
                            f"Tag configuration missing required fields: {', '.join(missing_fields)}"
                        )

                # Validate OPCUA configuration
                if (
                    "namespace" not in self.opcua_config
                    or "tag" not in self.opcua_config
                ):
                    raise ValueError(
                        "OPCUA configuration must have namespace and tag defined"
                    )

                self.opcua_tag_config = self.opcua_config["tag"]

                # Validate tag structure
                for tag in self.opcua_tag_config:
                    if "folder_name" not in tag or "child_node" not in tag:
                        raise ValueError(
                            "Each tag must have folder_name and child_node defined"
                        )

                    for node in tag["child_node"]:
                        required_node_fields = ["name", "type", "description", "access"]
                        missing_fields = [
                            field for field in required_node_fields if field not in node
                        ]
                        if missing_fields:
                            raise ValueError(
                                f"Node in folder {tag['folder_name']} missing required fields: {', '.join(missing_fields)}"
                            )

                        # If it's a JSON string type, validate its objects
                        if node.get("type") == "json_string" and "objects" in node:
                            for obj in node["objects"]:
                                required_obj_fields = [
                                    "name",
                                    "type",
                                    "description",
                                    "access",
                                ]
                                missing_fields = [
                                    field
                                    for field in required_obj_fields
                                    if field not in obj
                                ]
                                if missing_fields:
                                    raise ValueError(
                                        f"JSON object in node {node['name']} missing required fields: {', '.join(missing_fields)}"
                                    )

                # Set polling interval
                if "polling_interval_in_second" in self.db_config:
                    self.db_polling_interval_in_second = self.db_config[
                        "polling_interval_in_second"
                    ]

        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML format in config file: {e}")
        except KeyError as e:
            raise ValueError(f"Missing required configuration field: {str(e)}")
        except Exception as e:
            raise Exception(f"Error loading configuration: {str(e)}")

    def _setup(self):
        """Setup the plugin and connect to the database"""
        try:
            db_type = self.db_config["type"]

            if db_type == "sqlite":
                self.conn = sqlite3.connect(self.db_config["path"])
                self.conn.row_factory = sqlite3.Row
            elif db_type == "postgres":
                # Connect to PostgreSQL database
                self.conn = psycopg2.connect(
                    database=self.db_config["database"],
                    user=self.db_config["user"],
                    password=self.db_config["password"],
                    host=self.db_config["host"],
                    port=self.db_config["port"],
                    cursor_factory=DictCursor,
                )
            else:
                raise ValueError(f"Unsupported database type: {db_type}")

            print("Successfully connected to database")

        except sqlite3.Error as e:
            print(f"Error connecting to database: {e}")
            self.conn = None
        except Exception as e:
            print(f"Unexpected error connecting to database: {e}")
            self.conn = None

    async def _get_latest_values(
        self,
    ) -> Dict[str, Optional[Dict[str, Union[int, float, str, List[Any]]]]]:
        """Get latest values from all configured tables.

        Returns:
            Dict[str, Optional[Dict[str, Union[int, float, str, List[Any]]]]]:
                A dictionary where:
                - Key is the table name (str)
                - Value is either None (if no data/error) or a dictionary containing:
                    - Column names as keys (str)
                    - Column values as either int, float, str, or List[Any]

        Example:
            {
                "vibration_data": {
                    "id": 123,
                    "namespace": "device1",
                    "timestamp": "2023-...",
                    "sampling_rate": 1000,
                    "acceleration_x": [-0.1, ...],
                    ...
                }
            }
        """

        try:
            results = {}
            loop = asyncio.get_running_loop()

            for table_config in self.db_tables_config:
                table_name = table_config["name"]
                # Get configured column names
                configured_columns = [col["name"] for col in table_config["columns"]]

                # Build query
                query = f"""
                    SELECT * FROM {table_name}
                    ORDER BY id DESC LIMIT 1
                """

                # Execute query based on database type
                if self.db_config["type"] == "sqlite":

                    def execute_query():
                        # Create new connection inside the executor
                        with sqlite3.connect(self.db_config["path"]) as conn:
                            conn.row_factory = sqlite3.Row
                            cursor = conn.cursor()
                            cursor.execute(query)
                            row = cursor.fetchone()
                            return tuple(row) if row is not None else None

                    result = await loop.run_in_executor(None, execute_query)

                else:  # postgresql
                    if self.conn is None:
                        raise ValueError("No database connection")
                    cursor = self.conn.cursor()
                    cursor.execute(query)
                    row = cursor.fetchone()
                    result = tuple(row) if row is not None else None

                if result is not None:
                    # Verify result length matches configured columns
                    result_tuple = tuple(result)
                    if len(result_tuple) != len(configured_columns):
                        print(
                            f"Warning: Configured column count mismatch for table {table_name}."
                            f"Expected {len(table_config['columns'])}, got {len(result_tuple)}"
                        )
                        results[table_name] = None
                        continue

                    # Create dictionary from column name and value
                    row_dict = dict(zip(configured_columns, result))
                    results[table_name] = row_dict
                else:
                    results[table_name] = None

            return results

        except (sqlite3.Error, psycopg2.Error) as e:
            print(f"Database error: {e}")
            return {}
        except Exception as e:
            print(f"Unexpected error in _get_latest_values: {e}")
            return {}

    async def start(self, server: Any) -> Task[None]:
        """
        Run the main plugin loop

        Args:
            server: OPCUAGatewayServer instance
        """
        # Setup the plugin
        self._setup()
        self.running = True

        # Return the task instead of running it directly
        return asyncio.create_task(self._run_loop(server))

    async def _run_loop(self, server):
        """Main plugin loop"""
        while self.running:
            try:
                latest_values = await self._get_latest_values()

                # Basically only one row will be returned here
                for table_name, row_data in latest_values.items():
                    if row_data is not None:
                        await self._process_row_data(row_data, table_name, server)

                await asyncio.sleep(self.db_polling_interval_in_second)
            except Exception as e:
                print(f"Error in plugin loop: {e}")
                await asyncio.sleep(1)

    async def _initialize_root_nodes(self, server):
        """Initialize the node structure once during startup"""
        namespace = self.opcua_config["namespace"]
        ns_idx = server.get_namespace_index(namespace)

        for opcua_tag in self.opcua_tag_config:
            await server._create_or_get_node(
                server.server.nodes.objects, opcua_tag["folder_name"], ns_idx
            )

    async def _process_row_data(self, row_data: dict, table_name: str, server: Any):
        """Process a single row of data against configuration"""
        namespace = self.opcua_config["namespace"]
        ns_idx = server.get_namespace_index(namespace)
        if ns_idx is None:
            print(f"Namespace {namespace} not found")
            return

        if not row_data:
            return

        matching_map_logics = [
            data for data in self.db_tag_config if data["table_map"] == table_name
        ]

        for logic in matching_map_logics:
            if not MapValidator.validate(row_data, logic["map_logic"]):
                continue

            tag_name = row_data[logic["column_map"]]
            if not tag_name:
                continue

            folder_config = next(
                (
                    f
                    for f in self.opcua_tag_config
                    if f["folder_name"] == logic["opcua_folder_name"]
                ),
                None,
            )

            if folder_config:
                # Create main folder node
                folder_node = await server._create_or_get_node(
                    server.server.nodes.objects, folder_config["folder_name"], ns_idx
                )

                # Create tag node under folder
                tag_node = await server._create_or_get_node(
                    folder_node, tag_name, ns_idx
                )

                # Process each child node
                for child_node in folder_config["child_node"]:
                    if (
                        child_node.get("table_map") == table_name
                        and "column_map" in child_node
                    ):
                        value = row_data.get(child_node["column_map"])
                        node_type = child_node.get("type")

                        # Construct base path
                        base_path = f"{logic['opcua_folder_name']}.{tag_name}.{child_node['name']}"

                        if node_type == "json_string":
                            # Create a folder node for JSON type
                            json_folder = await server._create_or_get_node(
                                tag_node, child_node["name"], ns_idx
                            )

                            if value:
                                try:
                                    # Parse JSON data
                                    data = (
                                        json.loads(value)
                                        if isinstance(value, str)
                                        else value
                                    )

                                    # Create/update raw_data node
                                    await server.create_variable_node(
                                        json_folder,
                                        {
                                            "name": "raw_data",
                                            "type": "string",
                                            "access": child_node["access"],
                                        },
                                        ns_idx,
                                        value,
                                    )

                                    # Create/update nodes for each object in JSON
                                    if "objects" in child_node:
                                        for obj_config in child_node["objects"]:
                                            obj_name = obj_config["name"]
                                            obj_value = data.get(obj_name)
                                            if obj_value is not None:
                                                # Create/update variable node
                                                await server.create_variable_node(
                                                    json_folder,
                                                    {
                                                        "name": obj_name,
                                                        "type": obj_config["type"],
                                                        "access": obj_config["access"],
                                                    },
                                                    ns_idx,
                                                    obj_value,
                                                )

                                except json.JSONDecodeError as e:
                                    print(f"Error decoding JSON value: {e}")
                                except Exception as e:
                                    print(
                                        f"Error processing JSON node {child_node['name']}: {e}"
                                    )
                        else:
                            # Create/update regular variable node
                            await server.create_variable_node(
                                tag_node, child_node, ns_idx, value
                            )
                            server.update_value(namespace, base_path, value)

    async def _process_json_node(
        self,
        server: Any,
        parent_node: Node,
        node_config: dict,
        ns_idx: int,
        json_value: str,
    ) -> None:
        try:
            # Create the main JSON string node
            json_node = await server.create_variable_node(
                parent_node, node_config, ns_idx, json_value
            )

            # Parse JSON value
            data = json.loads(json_value) if isinstance(json_value, str) else json_value

            # Process sub-objects
            if "objects" in node_config:
                for obj_config in node_config["objects"]:
                    obj_name = obj_config["name"]
                    value = data.get(obj_name)

                    if value is not None:
                        await server.create_variable_node(
                            json_node,
                            {
                                "name": obj_name,
                                "type": obj_config["type"],
                                "description": obj_config["description"],
                                "access": obj_config["access"],
                            },
                            ns_idx,
                            value,
                        )

        except json.JSONDecodeError as e:
            print(f"Error decoding JSON value: {e}")
        except Exception as e:
            print(f"Error processing JSON node {node_config['name']}: {e}")

    async def stop(self):
        """Stop the plugin and cleanup"""
        self.running = False
