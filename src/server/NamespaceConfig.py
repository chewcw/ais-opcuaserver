from dataclasses import dataclass

from src.server.NodeConfig import NodeConfig


@dataclass
class NamespaceConfig:
    name: str
    objects: list[NodeConfig]
