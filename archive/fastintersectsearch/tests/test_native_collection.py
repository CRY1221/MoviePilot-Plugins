"""Run MP's own pure collection cases without its service/bootstrap fixtures."""
import ast

import pytest

from conftest import KERNEL, native_domain, native_nodes

tree = ast.parse((KERNEL / "tests/test_search_collection.py").read_text(encoding="utf-8"))
names = {node.name for node in tree.body if isinstance(node, ast.FunctionDef)}
scope = native_nodes("tests/test_search_collection.py", names, vars(native_domain()))


@pytest.mark.parametrize("case", sorted(name for name in names if name.startswith("test_")))
def test_pinned_native_collection(case):
    scope[case]()
