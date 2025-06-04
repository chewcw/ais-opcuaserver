from dataclasses import dataclass

from asyncua.ua import Any


@dataclass
class NodeConfig:
    name: str
    type: str
    initial_value: Any
    access: str
