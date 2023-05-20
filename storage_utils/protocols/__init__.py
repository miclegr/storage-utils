from typing import Any, Protocol

class Pushable(Protocol):
    @classmethod
    def from_domain(cls, domain: Any) -> Any:
        pass

class Pullable(Protocol):
    def to_domain(self) -> Any:
        pass

class Parsable(Protocol):
    @classmethod
    def parse_raw(cls, raw: str) -> Any:
        pass
