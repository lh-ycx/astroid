from astroid import node_classes


class TaintNode(node_classes.NodeNG):
    _astroid_fields = ("taint_tag",)

    _taint_fields = ()

    def __init__(self, lineno=None, col_offset=None, parent=None, taint_tag=None):
        super().__init__(lineno, col_offset, parent, taint_tag)
        # self._proxied = TaintInstance(self.taint_tag)
        # actually i cannot understand what does _proxied stand for
        self._proxied = TaintInstance

    def __repr__(self):
        return f'<TaintNode tag={self.taint_tag}>'

    def _infer(self, context=None, **kwargs):
        yield self

    def qname(self):
        return self.__class__.__name__

    def __add__(self, other):
        if isinstance(other, TaintInstance):
            return TaintNode(tag_agg_bool(self.taint_tag, self.taint_tag))
        return self

class TaintInstance():
    _all_bases_known = True

    newstyle = False

    def __init__(self, tag):
        self.taint_tag = tag

    @classmethod
    def qname(clz):
        return clz.__class__.__name__

    def __add__(self, other):
        if isinstance(other, TaintInstance):
            return TaintInstance(tag_agg_bool(self.taint_tag, self.taint_tag))
        return self


def tag_agg_bool(tag_1: bool, tag_2: bool):
    return tag_1 or tag_2