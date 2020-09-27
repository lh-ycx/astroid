class TaintInstance():
    def __init__(self, taint_tag):
        self.taint_tag = taint_tag

    def __bool__(self):
        return self.taint_tag

TaintInstance(True)
