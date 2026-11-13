class Expr:
    """Nodes use identity: reusing an instance means sharing its cached cells."""
    def __init__(self, op, *args):
        self.op, self.args = op, args


def E(op, *args):
    return Expr(op, *args)
