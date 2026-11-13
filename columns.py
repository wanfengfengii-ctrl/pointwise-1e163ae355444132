import json
from pathlib import Path


class Columns:
    def __init__(self, manifest):
        self.root = Path(manifest).parent
        self.spec = json.loads(Path(manifest).read_text(encoding="utf-8"))
        self.trace = []

    def groups(self):
        for i, group in enumerate(self.spec):
            self.trace.append(["group", i])
            yield i, group["rows"]

    def read(self, group, name, rows):
        self.trace.append(["read", group, name, list(rows)])
        spec = self.spec[group]["columns"]
        if name not in spec:
            raise KeyError("column:" + name)
        values = json.loads((self.root / spec[name]).read_text(encoding="utf-8"))
        return [values[i] for i in rows]
