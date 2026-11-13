import ast
import inspect
import json
import random
import sys
from pathlib import Path

import demand
import driver
import vectors
from columns import Columns
from demand import Complete, DemandKernel, Paused
from expr import E
from scan import scan
from reference import scan as reference
from samples import DATA, EXPECTED, TRACE, query


class Recording:
    """Only the public column-source protocol, with no expression knowledge."""
    def __init__(self, data):
        self.data, self.trace = data, []

    def groups(self):
        for g, columns in enumerate(self.data):
            self.trace.append(["group", g])
            yield g, len(next(iter(columns.values())))

    def read(self, group, name, rows):
        self.trace.append(["read", group, name, list(rows)])
        if name not in self.data[group]:
            raise KeyError("column:" + name)
        return [self.data[group][name][i] for i in rows]


def scalar(node, row):
    op, args = node.op, node.args
    if op == "col":
        return row[args[0]]
    if op == "lit":
        return args[0]
    if op == "if":
        return scalar(args[1] if scalar(args[0], row) is True else args[2], row)
    a = scalar(args[0], row)
    if op == "and":
        if a is False:
            return False
        b = scalar(args[1], row)
        return b if a is True else (False if b is False else None)
    if op == "or":
        if a is True:
            return True
        b = scalar(args[1], row)
        return b if a is False else (True if b is True else None)
    b = scalar(args[1], row)
    if a is None or b is None:
        return None
    return {"eq": lambda: a == b, "gt": lambda: a > b,
            "div": lambda: a / b}[op]()


def expect_error(predicate, projections, data, kind, message, trace):
    source = Recording(data)
    stream = scan(source, predicate, projections, 10)
    try:
        next(stream)
    except kind as exc:
        assert exc.args == (message,), (type(exc).__name__, exc.args)
    else:
        raise AssertionError("expected error before the first row")
    assert source.trace == trace, source.trace


def regression():
    source = Recording(DATA)
    p, out = query()
    stream = scan(source, p, out, 4)
    assert source.trace == []
    assert next(stream) == EXPECTED[0]
    assert source.trace == TRACE[:6]
    stream.close()
    assert source.trace == TRACE[:6]  # do not open the next row group
    for source in (Recording(DATA), Columns("data/manifest.json")):
        p, out = query()
        assert list(scan(source, p, out, 4)) == EXPECTED
        assert source.trace == TRACE

    # All empty demand: a missing column and a zero divisor remain cold.
    source = Recording([{"a": [1, 2]}])
    bad = E("div", E("col", "absent"), E("lit", 0))
    assert list(scan(source, E("lit", False), {"bad": bad}, 2)) == []
    assert source.trace == [["group", 0]]

    # IF completes the true arm vector before starting the false arm vector.
    # A row-at-a-time rewrite would raise ZeroDivisionError at row 0 instead.
    expect_error(E("lit", True),
                 {"v": E("if", E("col", "c"), E("col", "absent"),
                         E("div", E("lit", 1), E("col", "z")))},
                 [{"c": [False, True], "z": [0, 1]}],
                 KeyError, "column:absent",
                 [["group", 0], ["read", 0, "c", [0, 1]],
                  ["read", 0, "absent", [1]]])

    # Binary expressions finish both child vectors before their own division.
    expect_error(E("lit", True),
                 {"v": E("div", E("col", "a"),
                         E("if", E("col", "c"), E("lit", 0),
                           E("col", "absent")))},
                 [{"a": [1, 1], "c": [True, False]}],
                 KeyError, "column:absent",
                 [["group", 0], ["read", 0, "a", [0, 1]],
                  ["read", 0, "c", [0, 1]], ["read", 0, "absent", [1]]])
    expect_error(E("lit", True),
                 {"first": E("div", E("lit", 1), E("col", "z")),
                  "second": E("col", "absent")},
                 [{"z": [1, 0]}], ZeroDivisionError, "division by zero",
                 [["group", 0], ["read", 0, "z", [0, 1]]])

    # NULL is a successfully cached value, not a marker for missing work.
    x = E("col", "x")
    source = Recording([{"x": [None]}])
    assert list(scan(source, E("lit", True), {"a": x, "b": x}, 1)) == [
        {"a": None, "b": None}]
    assert source.trace == [["group", 0], ["read", 0, "x", [0]]]

    # Equal-looking nodes are distinct; the public trace preserves this.
    source = Recording([{"x": [7]}])
    assert list(scan(source, E("lit", True),
                     {"a": E("col", "x"), "b": E("col", "x")}, 1)) == [
        {"a": 7, "b": 7}]
    assert source.trace == [["group", 0], ["read", 0, "x", [0]],
                            ["read", 0, "x", [0]]]

    # Source failures pass through by identity, including the attempted read.
    marker = RuntimeError("source-stop")
    class Failing(Recording):
        def read(self, group, name, rows):
            self.trace.append(["read", group, name, list(rows)])
            raise marker
    source = Failing([{"x": [1]}])
    try:
        list(scan(source, E("lit", True), {"v": E("col", "x")}, 1))
    except RuntimeError as exc:
        assert exc is marker
    else:
        raise AssertionError("source failure swallowed")
    assert source.trace == [["group", 0], ["read", 0, "x", [0]]]

    source = Recording([{"x": []}, {"x": [8]}])
    assert list(scan(source, E("lit", True), {}, 2)) == [{}]
    assert source.trace == [["group", 0], ["group", 1]]
    for size in (0, -1, True, 1.5):
        source = Recording([{"x": [1]}])
        try:
            next(scan(source, E("lit", True), {}, size))
        except ValueError as exc:
            assert exc.args == ("batch_size",)
        else:
            raise AssertionError("invalid batch_size accepted")
        assert source.trace == []

    # Independent truth tables, including NULL on either input.
    for a in (True, False, None):
        for b in (True, False, None):
            for op in ("and", "or", "eq"):
                term = E(op, E("lit", a), E("lit", b))
                source = Recording([{"anchor": [0]}])
                actual = list(scan(source, E("lit", True), {"v": term}, 1))
                assert actual == [{"v": scalar(term, {})}]
    term = E("div", E("lit", None), E("lit", 0))
    assert list(scan(Recording([{"anchor": [0]}]), E("lit", True),
                     {"v": term}, 1)) == [{"v": None}]

    rng = random.Random(31)
    for _ in range(40):
        n = rng.randrange(1, 13)
        data = {"gate": [rng.choice([True, False, None]) for _ in range(n)],
                "fallback": [rng.choice([True, False, None]) for _ in range(n)],
                "want": [rng.choice([True, False, None]) for _ in range(n)],
                "x": [rng.choice([None, 1, 2, 4, 8]) for _ in range(n)]}
        p, out = query()
        expected = []
        for i in range(n):
            row = {k: v[i] for k, v in data.items()}
            if scalar(p, row) is True:
                expected.append({k: scalar(v, row) for k, v in out.items()})
        for size in (1, 3, 20):
            assert list(scan(Recording([data]), p, out, size)) == expected
    # Frozen vector semantics cover traces, partial output and error precedence.
    def capture(executor, data, p, out, size):
        source, prefix, error = Recording(data), [], None
        try:
            prefix.extend(executor(source, p, out, size))
        except Exception as exc:
            error = (type(exc).__name__, exc.args)
        return prefix, source.trace, error

    for _ in range(35):
        n = rng.randrange(1, 9)
        data = {"x": [rng.choice([None, 0, 1, 2]) for _ in range(n)],
                "gate": [rng.choice([True, False, None]) for _ in range(n)],
                "fallback": [rng.choice([True, False, None]) for _ in range(n)]}
        nums = [E("col", "x"), E("lit", 0), E("lit", 1), E("lit", None)]
        bools = [E("col", "gate"), E("col", "fallback")]
        for step in range(6):
            a, b = rng.choice(nums), rng.choice(nums)
            nums.append(E("div", a, b) if step % 2 else
                        E("if", rng.choice(bools), a, b))
            bools.append(E(rng.choice(["eq", "gt"]), rng.choice(nums),
                           rng.choice(nums)))
            bools.append(E(rng.choice(["and", "or"]), rng.choice(bools),
                           rng.choice(bools)))
        p = rng.choice(bools)
        out = {"a": rng.choice(nums), "b": rng.choice(nums)}
        size = rng.choice([1, 3, 10])
        assert capture(scan, [data], p, out, size) == capture(
            reference, [data], p, out, size)
    print("BEHAVIOR_REGRESSION_OK")


def drive(kernel, node, rows, columns):
    """Run one kernel request, answering column demands from ``columns``."""
    demanded = []
    result = kernel.evaluate(node, rows)
    while isinstance(result, Paused):
        demanded.append((result.column, list(result.rows)))
        result = kernel.resume(result, [columns[result.column][i]
                                       for i in result.rows])
    assert isinstance(result, Complete), result
    return result.values, demanded


def kernel_and_operator_checks():
    # Independent kernel run with hand-made column responses, including the
    # documented shared node: [0,2,3] then [1,2], where row 2 is cached NULL.
    q = E("div", E("lit", 20), E("col", "x"))
    kernel = DemandKernel()
    x = {0: 2, 1: 4, 2: None, 3: 5}
    assert drive(kernel, q, [0, 2, 3], {"x": x}) == (
        [10.0, None, 4.0], [("x", [0, 2, 3])])
    values, demanded = drive(kernel, q, [1, 2], {"x": x})
    assert values == [5.0, None]
    assert demanded == [("x", [1])]  # cached NULL row 2 is not missing work

    # Empty demand at the kernel: no column demand at all.
    assert drive(DemandKernel(), E("col", "cold"), [], {}) == ([], [])
    assert drive(DemandKernel(), E("lit", 3), [], {}) == ([], [])

    # Pause handoff: each resume advances the same walk and keeps its frames.
    branch = E("div", E("col", "a"),
               E("if", E("col", "c"), E("lit", 1), E("col", "absent")))
    values, demanded = drive(DemandKernel(), branch, [0, 1],
                             {"a": {0: 2, 1: 2}, "c": {0: True, 1: False},
                              "absent": {1: 2}})
    assert demanded == [("a", [0, 1]), ("c", [0, 1]), ("absent", [1])]
    assert values == [2.0, 1.0]

    # A failing node is not cached, but its finished child cells remain: a
    # retry reuses both child vectors and fails again without new reads.
    failing = E("div", E("col", "p"), E("col", "z"))
    kernel = DemandKernel()
    try:
        drive(kernel, failing, [0, 1], {"p": [1, 1], "z": [1, 0]})
    except ZeroDivisionError as exc:
        assert exc.args == ("division by zero",)
    else:
        raise AssertionError("zero division swallowed")
    try:
        result = kernel.evaluate(failing, [0, 1])
    except ZeroDivisionError as exc:
        assert exc.args == ("division by zero",)  # no new column demands
    else:
        raise AssertionError("cached child vectors lost")
    # Unknown operators fail from the operator table, not from graph walking.
    try:
        DemandKernel().evaluate(E("bogus", E("lit", 1)), [0])
    except ValueError as exc:
        assert exc.args == ("operator:bogus",)
    else:
        raise AssertionError("unknown operator accepted")

    # Local operator calls carry no Expr: only a name, payload, rows, vectors.
    rows = [0, 1, 2]
    assert vectors.step("col", "x", rows, {}) == vectors.NeedColumn(
        "x", rows)
    assert vectors.step("lit", 7, rows, {}).values == [7, 7, 7]
    assert isinstance(vectors.step("gt", None, rows, {}),
                      vectors.NeedChild)
    child = vectors.step("gt", None, rows, {0: [1, 2, 3]})
    assert (child.index, child.rows) == (1, rows)
    left = [True, False, None]
    short = vectors.step("and", None, rows, {0: left})
    assert (short.index, short.rows) == (1, [0, 2])
    assert vectors.step("and", None, rows,
                        {0: left, 1: [False, True]}).values == [
        False, False, None]
    short = vectors.step("or", None, rows, {0: left})
    assert (short.index, short.rows) == (1, [1, 2])
    branch = vectors.step("if", None, rows, {0: [True, False, None]})
    assert (branch.index, branch.rows) == (1, [0])
    branch = vectors.step("if", None, rows,
                          {0: [True, False, None], 1: [10]})
    assert (branch.index, branch.rows) == (2, [1, 2])
    assert vectors.step("div", None, [0, 1],
                        {0: [1, None], 1: [2, 0]}).values == [0.5, None]


def driver_checks():
    # The read driver works with a replaced column source and keeps laziness.
    source = Recording([{"x": [1, 2]}])
    x = E("col", "x")
    assert list(scan(source, E("lit", True), {"a": x, "b": x}, 1)) == [
        {"a": 1, "b": 1}, {"a": 2, "b": 2}]
    assert source.trace == [["group", 0], ["read", 0, "x", [0]],
                            ["read", 0, "x", [1]]]  # cache is per batch
    source = Recording([{"x": [1]}, {"x": [2]}])
    stream = scan(source, E("lit", True), {"v": E("col", "x")}, 1)
    assert next(stream) == {"v": 1}
    stream.close()
    assert source.trace == [["group", 0], ["read", 0, "x", [0]]]

    # The driver itself only reads through the source protocol; a source that
    # only implements groups/read is enough.
    class Bare:
        def __init__(self):
            self.calls = []

        def groups(self):
            return iter([(0, 2)])

        def read(self, group, name, rows):
            self.calls.append((group, name, list(rows)))
            return [10 for _ in rows]

    bare = Bare()
    assert list(scan(bare, E("lit", True), {"v": E("col", "any")}, 2)) == [
        {"v": 10}, {"v": 10}]
    assert bare.calls == [(0, "any", [0, 1])]


def _module_ast(name):
    return ast.parse(Path(name + ".py").read_text(encoding="utf-8"))


def _imported(module):
    names = set()
    for node in ast.walk(module):
        if isinstance(node, ast.Import):
            names.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            names.add((node.module or "").split(".")[0])
    return names


def _string_constants(module):
    return {node.value for node in ast.walk(module)
            if isinstance(node, ast.Constant) and isinstance(node.value, str)}


def _attributes(module):
    return {node.attr for node in ast.walk(module)
            if isinstance(node, ast.Attribute)}


def boundary_checks():
    scan_ast, driver_ast = _module_ast("scan"), _module_ast("driver")
    demand_ast, vectors_ast = _module_ast("demand"), _module_ast("vectors")

    # Dependencies may only point driver -> demand -> vectors.
    assert _imported(vectors_ast) == set()
    assert _imported(demand_ast) <= {"vectors"}
    assert _imported(driver_ast) <= {"demand", "vectors"}
    assert "columns" not in _imported(demand_ast) | _imported(driver_ast)
    assert "reference" not in (_imported(scan_ast) | _imported(driver_ast)
                               | _imported(demand_ast) | _imported(vectors_ast))
    for module in (scan_ast, driver_ast, demand_ast, vectors_ast):
        assert not ({"importlib", "__import__", "sys"} & _imported(module))
        assert not ({"importlib", "__import__", "modules"}
                    & _string_constants(module))

    # scan.py is a pure forwarder: one imported executor, one delegating def.
    funcs = [node for node in scan_ast.body
             if isinstance(node, ast.FunctionDef)]
    assert [node.name for node in funcs] == ["scan"]
    body = funcs[0].body
    assert len(body) == 2, body  # docstring + return
    assert isinstance(body[-1], ast.Return)
    assert isinstance(body[-1].value, ast.Call)
    assert _imported(scan_ast) == {"driver"}

    # demand.py does not branch on concrete operators or cell values.
    assert not ({"col", "lit", "eq", "gt", "div", "and", "or", "if"}
                & _string_constants(demand_ast))
    # driver does the I/O only through the source; the operator layer has none.
    assert not ({"read", "open", "source", "group"}
                & _attributes(vectors_ast))
    # driver.py does not walk or interpret expressions.
    assert not ({"op", "args"} & _attributes(driver_ast))
    assert not ({"col", "lit", "eq", "gt", "div", "and", "or", "if"}
                & _string_constants(driver_ast))
    assert "expr" not in (_imported(driver_ast) | _imported(demand_ast)
                          | _imported(vectors_ast))

    # Handoff signatures: no source/callback reaches the kernel, no Expr
    # reaches the operator.
    assert list(inspect.signature(DemandKernel.__init__).parameters) == ["self"]
    assert list(inspect.signature(DemandKernel.evaluate).parameters) == [
        "self", "node", "rows"]
    assert list(inspect.signature(DemandKernel.resume).parameters) == [
        "self", "paused", "values"]
    assert list(inspect.signature(vectors.step).parameters) == [
        "op", "payload", "rows", "children"]
    assert not ({"source", "read", "callback", "expr", "node"}
                & set(inspect.signature(vectors.step).parameters))


def structure():
    kernel_and_operator_checks()
    driver_checks()
    boundary_checks()
    print("STRUCTURE_OK")


if __name__ == "__main__":
    if sys.argv[1:] != ["verify"]:
        raise SystemExit("usage: python3 checks.py verify")
    regression()
    structure()
