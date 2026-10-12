"""Offline boundary tests. Native algorithms are read from the pinned checkout."""
import ast
import importlib.util
import os
from pathlib import Path
import sys
import types

ROOT = Path(__file__).resolve().parents[1]
KERNEL = Path(os.environ.get("MP_KERNEL_REFERENCE", ROOT.parent / "kernel-reference"))
package = types.ModuleType("fastsearch")
package.__path__ = [str(ROOT / "plugins.v3" / "fastintersectsearch")]
sys.modules["fastsearch"] = package


def native_nodes(relative, names, namespace=None):
    """Execute unchanged native definitions with only external services substituted."""
    tree = ast.parse((KERNEL / relative).read_text(encoding="utf-8"))
    selected = [node for node in tree.body if getattr(node, "name", None) in names]
    assert len(selected) == len(names)
    future = ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0)
    module = ast.fix_missing_locations(ast.Module(body=[future, *selected], type_ignores=[]))
    module_object = types.ModuleType("app.native_test")
    sys.modules[module_object.__name__] = module_object
    scope = module_object.__dict__
    scope.update(namespace or {})
    exec(compile(module, str(KERNEL / relative), "exec"), scope)
    return scope


def native_domain():
    spec = importlib.util.spec_from_file_location("native_search_domain", KERNEL / "app/domain/search.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module
