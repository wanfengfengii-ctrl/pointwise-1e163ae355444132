"""Compatibility entry point; the implementation lives in ``driver``."""

import driver


def scan(source, predicate, projections, batch_size):
    """Legacy name for the vector scan; forwards to the read driver."""
    return driver.scan(source, predicate, projections, batch_size)
