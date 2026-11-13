"""Stateless local vector operators.

A local step receives only an operator name, its literal/column payload, the
requested row numbers and already-computed child vectors.  It never receives
expression nodes, never walks a graph, keeps no cache between calls and does
no I/O.  The demand kernel owns recursion and caching.
"""

PAYLOAD = "payload"
CHILD = "child"

_SHAPES = {
    "col": (PAYLOAD,),
    "lit": (PAYLOAD,),
    "eq": (CHILD, CHILD),
    "gt": (CHILD, CHILD),
    "div": (CHILD, CHILD),
    "and": (CHILD, CHILD),
    "or": (CHILD, CHILD),
    "if": (CHILD, CHILD, CHILD),
}


class _Exchange:
    """Shared value comparison for the small handoff records."""
    __slots__ = ()

    def __init__(self, *values):
        for slot, value in zip(self.__slots__, values):
            setattr(self, slot, value)

    def __eq__(self, other):
        return type(self) is type(other) and self._key() == other._key()

    def _key(self):
        return tuple(getattr(self, slot) for slot in self.__slots__)

    def __repr__(self):
        return "%s(%r)" % (type(self).__name__, self._key())


class Done(_Exchange):
    """This local step is complete; values align with the demanded rows."""
    __slots__ = ("values",)


class NeedChild(_Exchange):
    """Demand ``rows`` from child operand ``index`` before continuing."""
    __slots__ = ("index", "rows")


class NeedColumn(_Exchange):
    """Demand one explicit column read; only a leaf may ask for this."""
    __slots__ = ("column", "rows")


def shape(op):
    """Return the declarative operand shape for ``op``.

    Each entry is PAYLOAD (a literal or column name carried by the node) or
    CHILD (an operand evaluated by the demand kernel).
    """
    try:
        return _SHAPES[op]
    except KeyError:
        raise ValueError("operator:" + op) from None


def step(op, payload, rows, children):
    """Advance one local operator step.

    ``rows`` is the ordered row demand for this node and ``children`` maps
    operand index to an already-computed vector for that operand's demand.
    Returns Done, NeedChild or NeedColumn without mutating anything.
    """
    if op == "col":
        return NeedColumn(payload, list(rows))
    if op == "lit":
        return Done([payload] * len(rows))
    if op in ("eq", "gt", "div"):
        if 0 not in children:
            return NeedChild(0, list(rows))
        if 1 not in children:
            return NeedChild(1, list(rows))
        return Done(_compare(op, children[0], children[1]))
    if op in ("and", "or"):
        if 0 not in children:
            return NeedChild(0, list(rows))
        left = children[0]
        if 1 not in children:
            if op == "and":
                needed = [i for i, a in zip(rows, left) if a is not False]
            else:
                needed = [i for i, a in zip(rows, left) if a is not True]
            return NeedChild(1, needed)
        return Done(_logic(op, rows, left, children[1]))
    if op == "if":
        if 0 not in children:
            return NeedChild(0, list(rows))
        cond = children[0]
        if 1 not in children:
            return NeedChild(1, [i for i, c in zip(rows, cond)
                                if c is True])
        if 2 not in children:
            return NeedChild(2, [i for i, c in zip(rows, cond)
                                if c is not True])
        return Done(_branch(rows, cond, children[1], children[2]))
    raise ValueError("operator:" + op)


def _compare(op, left, right):
    values = []
    for a, b in zip(left, right):
        if a is None or b is None:
            values.append(None)
        elif op == "eq":
            values.append(a == b)
        elif op == "gt":
            values.append(a > b)
        else:
            values.append(a / b)
    return values


def _logic(op, rows, left, right):
    if op == "and":
        needed = [i for i, a in zip(rows, left) if a is not False]
    else:
        needed = [i for i, a in zip(rows, left) if a is not True]
    right_by_row = dict(zip(needed, right))
    values = []
    for i, a in zip(rows, left):
        if op == "and":
            if a is False:
                values.append(False)
            else:
                b = right_by_row[i]
                values.append(b if a is True else
                              (False if b is False else None))
        else:
            if a is True:
                values.append(True)
            else:
                b = right_by_row[i]
                values.append(b if a is False else
                              (True if b is True else None))
    return values


def _branch(rows, cond, yes, no):
    yes_rows = [i for i, c in zip(rows, cond) if c is True]
    no_rows = [i for i, c in zip(rows, cond) if c is not True]
    y = dict(zip(yes_rows, yes))
    n = dict(zip(no_rows, no))
    return [y[i] if c is True else n[i] for i, c in zip(rows, cond)]
