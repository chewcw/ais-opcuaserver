from dataclasses import dataclass

from .NodeConfig import NodeConfig


@dataclass
class NamespaceConfig:
    name: str
    objects: list[NodeConfig]
