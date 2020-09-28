import astroid
from astroid import *

input_file = "tests/test_taint_node.py"

df2col2taint = {
    "df": {"tainted_col": True, "untainted_col": False}
}

udf_names = ['simple_udf']


def build_from_dict(old_node, col2taint):
    ret_node = nodes.Dict(lineno=old_node.lineno, col_offset=old_node.col_offset, parent=old_node.parent)
    items = []
    for key in col2taint:
        node = nodes.Call(lineno=ret_node.lineno, parent=ret_node)
        node.postinit(
            func=Name(name='TaintInstance', parent=node),
            args=[Const(value=col2taint[key], parent=node)],
            keywords=None
        )
        items.append([nodes.Const(parent=ret_node, value=key), node])
    ret_node.postinit(items=items)
    return ret_node


def infer_my_custom_arg(node, context=None):
    # instrument the argument according to col2taint
    df_name = node.name
    col2taint = df2col2taint[df_name]
    new_node = build_from_dict(node, col2taint)
    new_node.parent = node.parent
    print(new_node.repr_tree())
    return iter((new_node, ))


def _looks_like_my_custom_arg(node):
    if isinstance(node.parent, nodes.Call) and isinstance(node.parent.func, nodes.Name):
        # print(node.parent.repr_tree())
        if node in node.parent.args:
            return node.parent.func.name in udf_names
    return False



MANAGER.register_transform(
    nodes.Name,
    inference_tip(infer_my_custom_arg),
    _looks_like_my_custom_arg,
)



def main():
    code = open(input_file, 'r').read()
    tree = astroid.extract_node(code)
    # tree.args[0].taint_tag = True
    # print(tree.repr_tree())
    # res = tree.inferred()
    res = tree.query()
    print(f"node ({tree.as_string()})'s dependency:")
    for r in res:
        print(r.repr_tree()) # Instance of debug.tests.extra.taint_instance.TaintInstance

    # try to get value from the instance
    # print(res.bool_value())


if __name__ == "__main__":
    main()