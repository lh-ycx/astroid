from astroid import node_classes


class TaintNode(node_classes.NodeNG):
    _astroid_fields = ("taint_tag",)

    _taint_fields = ()

    def __init__(self, lineno=None, col_offset=None, parent=None, taint_tag=None):
        super().__init__(lineno, col_offset, parent, taint_tag)
        self._proxied = TaintInstance()

    def __repr__(self):
        return f'<TaintNode tag={self.taint_tag}>'

    def _infer(self, context=None, **kwargs):
        yield self

    def qname(self):
        return self.__class__.__name__

class TaintInstance():
    def __init__(self):
        pass

    def qname(self):
        return self.__class__.__name__