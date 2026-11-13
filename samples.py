from expr import E


def query():
    gate, x = E("col", "gate"), E("col", "x")
    q = E("div", E("lit", 20), x)
    positive = E("gt", q, E("lit", 3))
    predicate = E("or", E("and", gate, positive), E("col", "fallback"))
    # q first receives rows 0,2,3, then 1; cached NULL at row 2 is not absent.
    projections = {
        "a": E("if", gate, q, E("lit", -1)),
        "b": E("if", E("col", "want"), q, E("lit", -2)),
        "q": q,
    }
    return predicate, projections


DATA = [{
    "gate": [True, False, None, True],
    "x": [2, 4, None, 5],
    "fallback": [False, True, True, False],
    "want": [False, True, True, False],
}, {
    "gate": [False, True],
    "x": [0, 2],
    "fallback": [False, False],
    "want": [True, True],
}]

EXPECTED = [
    {"a": 10.0, "b": -2, "q": 10.0},
    {"a": -1, "b": 5.0, "q": 5.0},
    {"a": -1, "b": None, "q": None},
    {"a": 4.0, "b": -2, "q": 4.0},
    {"a": 10.0, "b": 10.0, "q": 10.0},
]

TRACE = [
    ["group", 0],
    ["read", 0, "gate", [0, 1, 2, 3]],
    ["read", 0, "x", [0, 2, 3]],
    ["read", 0, "fallback", [1, 2]],
    ["read", 0, "want", [0, 1, 2, 3]],
    ["read", 0, "x", [1]],
    ["group", 1],
    ["read", 1, "gate", [0, 1]],
    ["read", 1, "x", [1]],
    ["read", 1, "fallback", [0]],
    ["read", 1, "want", [1]],
]
