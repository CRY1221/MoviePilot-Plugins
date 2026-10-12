"""Descriptor-safe, ownership-aware reversible patches."""
import inspect
from dataclasses import dataclass


@dataclass
class Patch:
    target: object
    name: str
    original: object
    replacement: object
    owned: bool


class Hooks:
    def __init__(self):
        self.patches = []

    def install(self, target, name, replacement):
        original = inspect.getattr_static(target, name)
        record = Patch(target, name, original, replacement, name in vars(target))
        setattr(target, name, replacement)
        self.patches.append(record)

    def close(self):
        for patch in reversed(self.patches):
            if inspect.getattr_static(patch.target, patch.name) is patch.replacement:
                if patch.owned:
                    setattr(patch.target, patch.name, patch.original)
                else:
                    delattr(patch.target, patch.name)
        self.patches.clear()
