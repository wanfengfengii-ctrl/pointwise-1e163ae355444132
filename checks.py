import json
import random
import sys
from columns import Columns
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


if __name__ == "__main__":
    if sys.argv[1:] != ["verify"]:
        raise SystemExit("usage: python3 checks.py verify")
    regression()
