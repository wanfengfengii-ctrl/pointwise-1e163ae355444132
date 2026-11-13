import json
from columns import Columns
from scan import scan
from samples import query

source = Columns("data/manifest.json")
predicate, projections = query()
print(json.dumps({"rows": list(scan(source, predicate, projections, 4)),
                  "trace": source.trace}, ensure_ascii=False, indent=2))
