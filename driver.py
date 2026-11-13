"""Read driver: owns the source, groups, batches, column reads and delivery.

The driver follows only the demand kernel's exchange objects.  It never walks
an expression graph and never interprets an operator.
"""

from demand import DemandKernel, Paused


class _Resolver:
    def __init__(self, kernel, source, group):
        self.kernel = kernel
        self.source = source
        self.group = group

    def resolve(self, node, rows):
        result = self.kernel.evaluate(node, rows)
        while isinstance(result, Paused):
            values = self.source.read(self.group, result.column, result.rows)
            result = self.kernel.resume(result, values)
        return result.values


def scan(source, predicate, projections, batch_size):
    """Lazy batch scan over ``source``; yields projection dictionaries."""
    if type(batch_size) is not int or batch_size < 1:
        raise ValueError("batch_size")
    for group, size in source.groups():
        for start in range(0, size, batch_size):
            kernel = DemandKernel()
            resolver = _Resolver(kernel, source, group)
            rows = list(range(start, min(size, start + batch_size)))
            keep = [i for i, v in zip(rows, resolver.resolve(predicate, rows))
                    if v is True]
            output = {name: resolver.resolve(node, keep)
                      for name, node in projections.items()}
            for j in range(len(keep)):
                yield {name: values[j] for name, values in output.items()}
