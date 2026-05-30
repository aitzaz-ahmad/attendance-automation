from typing import Any, Dict, Protocol, Type, TypeVar


class ISerializable(Protocol):
    def to_dict(self) -> Dict[str, Any]: ...


T = TypeVar("T")


class IDeserializable(Protocol):
    @classmethod
    def from_dict(cls: Type[T], payload: Dict[str, Any]) -> T: ...
