from astroid import *
import astroid
import ast
from ast import NodeVisitor

script_name = "tests/test_input.py"

# suppose we know the arguments
args = {
    "extracted_loc": "fake_extracted",
    "denied_loc": "fake_denied",
    "scrub_loc": "fake_scrub",
    "selected_cols": "fake_selected"
}

input_paths = []
input_nodes = []
output_paths = []
output_nodes = []

def is_input_call(node: astroid.Call):
    # can be extended to other input method
    if isinstance(node.func, astroid.Attribute) and node.func.attrname == 'json':
        if isinstance(node.func.expr, astroid.Attribute) and node.func.expr.attrname == 'read':
            return True
    return False

def is_output_call(node: astroid.Call):
    # can be extended to other output method
    if isinstance(node.func, astroid.Attribute) and node.func.attrname == 'save':
        return True
    return False

def catch_path_transform(node: astroid.Call):
    # extract the path node to infer
    if is_input_call(node):
        input_node = node.args[0]
        input_nodes.append(input_node)
    if is_output_call(node):
        output_node = node.args[0]
        output_nodes.append(output_node)
    return node


def variable2value(variable_name):
    return args[variable_name]


def is_arg(variable_name):
    return variable_name in args


def infer_custom_name(node: astroid.Name, context=None):
    return iter((astroid.Const(value=variable2value(node.name)),))


def _looks_like_my_custom_name(node: astroid.Name):
    return is_arg(node.name)

MANAGER.register_transform(
    astroid.Call,
    catch_path_transform
)

MANAGER.register_transform(
    astroid.Name,
    inference_tip(infer_custom_name),
    _looks_like_my_custom_name
)

def show_infer_results(results):
    assert isinstance(results, list)
    print("possible path value:")
    for res in results:
        if res is util.Uninferable:
            print("error: uninferable results")
        if isinstance(res,astroid.Const):
            print(f"{res.value}")
        else:
            print(f"unsupported node class: {res}")

if __name__ == '__main__':
    code = open(script_name, 'r').read()

    tree = astroid.parse(code)
    print(f"find {len(input_nodes)} input functions:")
    for node in input_nodes:
        print(f"[line No. {node.parent.lineno}] {node.parent.as_string()}")
        try:
            results = node.inferred()
            show_infer_results(results)
        except Exception as e:
            print("fail to infer")
            import traceback
            traceback.print_exc()
    print(f"find {len(output_nodes)} output functions:")
    for node in output_nodes:
        print(f"[line No. {node.parent.lineno}] {node.parent.as_string()}")
        try:
            results = node.inferred()
            show_infer_results(results)
        except Exception as e:
            print("fail to infer")
            import traceback
            traceback.print_exc()
