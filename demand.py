"""Shared-graph demand scheduling: row cache, missing work and pause handoff.

The kernel knows only graph structure (which operand is a payload and which
are children) and the declarative local-step protocol from ``vectors``.  It
never inspects an operator name, never looks at an individual cell value and
never talks to a source; a needed column leaves it as an explicit demand that
someone else must satisfy.
"""

from vectors import PAYLOAD, NeedChild, NeedColumn, shape, step

_NO_VALUE = object()


class Complete:
    """A finished node request. ``values`` follow the requested row order."""
    __slots__ = ("values",)

    def __init__(self, values):
        self.values = values


class Paused:
    """One outstanding column demand; resume it with the matching vector."""
    __slots__ = ("column", "rows", "frames")

    def __init__(self, column, rows, frames):
        self.column = column
        self.rows = list(rows)
        self.frames = frames


class _Frame:
    """Resumable state for one node request: cached cells and child vectors."""
    __slots__ = ("node", "requested", "cells", "missing", "children", "pending")


class DemandKernel:
    """Per-batch cell cache keyed by node identity, plus demand propagation."""

    def __init__(self):
        self._cells = {}

    def evaluate(self, node, rows):
        """Start a node request over ``rows``; return Complete or Paused."""
        requested = list(rows)
        return self._run([self._frame(node, requested)], _NO_VALUE)

    def resume(self, paused, values):
        """Supply the demanded column vector and continue the same walk."""
        frames = paused.frames
        frame = frames.pop()
        frame.cells.update(zip(frame.missing, values))
        frame.missing = []
        return self._run(frames, [frame.cells[i] for i in frame.requested])

    def _frame(self, node, requested):
        frame = _Frame()
        frame.node = node
        frame.requested = requested
        frame.cells = self._cells.setdefault(node, {})
        frame.missing = [i for i in requested if i not in frame.cells]
        frame.children = {}
        frame.pending = None
        return frame

    def _run(self, frames, value):
        while frames:
            frame = frames[-1]
            if value is not _NO_VALUE:
                frame.children[frame.pending] = value
                frame.pending = None
                value = _NO_VALUE
            if frame.missing:
                op = frame.node.op
                outcome = step(op, _payload(op, frame.node),
                               frame.missing, frame.children)
                if isinstance(outcome, NeedColumn):
                    return Paused(outcome.column, outcome.rows, frames)
                if isinstance(outcome, NeedChild):
                    frame.pending = outcome.index
                    frames.append(self._frame(
                        frame.node.args[outcome.index], outcome.rows))
                    continue
                frame.cells.update(zip(frame.missing, outcome.values))
                frame.missing = []
            result = [frame.cells[i] for i in frame.requested]
            frames.pop()
            value = result
        return Complete(value)


def _payload(op, node):
    """Structural payload: only a leaf shape carries a literal/column."""
    return node.args[0] if shape(op)[0] is PAYLOAD else None
