from dataclasses import dataclass

from server.NodeConfig import NodeConfig


@dataclass
class NamespaceConfig:
    name: str
    objects: list[NodeConfig]
