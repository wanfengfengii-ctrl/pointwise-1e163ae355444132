def scan(source, predicate, projections, batch_size):
    """Legacy vector scan. Evaluation, missing-cell tracking and I/O share a closure."""
    if type(batch_size) is not int or batch_size < 1:
        raise ValueError("batch_size")
    for group, size in source.groups():
        for start in range(0, size, batch_size):
            cache = {}

            def evaluate(node, requested):
                cells = cache.setdefault(node, {})
                missing = [i for i in requested if i not in cells]
                if not missing:
                    return [cells[i] for i in requested]
                op, args = node.op, node.args
                if op == "col":
                    values = source.read(group, args[0], missing)
                elif op == "lit":
                    values = [args[0]] * len(missing)
                elif op in ("eq", "gt", "div"):
                    left = evaluate(args[0], missing)
                    right = evaluate(args[1], missing)
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
                elif op in ("and", "or"):
                    left = evaluate(args[0], missing)
                    needed = [i for i, a in zip(missing, left)
                              if (a is not False if op == "and" else a is not True)]
                    right = dict(zip(needed, evaluate(args[1], needed)))
                    values = []
                    for i, a in zip(missing, left):
                        if op == "and":
                            if a is False:
                                values.append(False)
                            else:
                                b = right[i]
                                values.append(b if a is True else
                                              (False if b is False else None))
                        else:
                            if a is True:
                                values.append(True)
                            else:
                                b = right[i]
                                values.append(b if a is False else
                                              (True if b is True else None))
                elif op == "if":
                    cond = evaluate(args[0], missing)
                    yes = [i for i, c in zip(missing, cond) if c is True]
                    no = [i for i, c in zip(missing, cond) if c is not True]
                    y = dict(zip(yes, evaluate(args[1], yes)))
                    n = dict(zip(no, evaluate(args[2], no)))
                    values = [y[i] if c is True else n[i]
                              for i, c in zip(missing, cond)]
                else:
                    raise ValueError("operator:" + op)
                cells.update(zip(missing, values))
                return [cells[i] for i in requested]

            rows = list(range(start, min(size, start + batch_size)))
            keep = [i for i, v in zip(rows, evaluate(predicate, rows)) if v is True]
            output = {name: evaluate(node, keep) for name, node in projections.items()}
            for j in range(len(keep)):
                yield {name: values[j] for name, values in output.items()}
