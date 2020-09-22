from astroid import node_classes


class TaintNode(node_classes.NodeNG):
    _astroid_fields = ("taint_tag",)

    _taint_fields = ()

    def __init__(self, lineno=None, col_offset=None, parent=None, taint_tag=None):
        super().__init__(lineno, col_offset, parent, taint_tag)

    def __repr__(self):
        return f'<TaintNode tag={self.taint_tag}>'


