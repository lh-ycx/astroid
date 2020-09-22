import astroid
from astroid import *

input_file = "empty_udf.py"

df2col2taint = {
    "df": {"tainted_col": True, "untainted_col": False}
}

udf_names = ['simple_udf']


def build_from_dict(col2taint):
    ret_node = nodes.Dict()
    ret_node.items = [[nodes.Const(value=key), nodes.TaintNode(taint_tag=col2taint[key])] for key in col2taint]
    return ret_node


def infer_my_custom_arg(node, context=None):
    # instrument the argument according to col2taint
    df_name = node.name
    col2taint = df2col2taint[df_name]
    new_node = build_from_dict(col2taint)
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
    print(tree.repr_tree())
    print(tree.inferred()[0])


if __name__ == "__main__":
    main()