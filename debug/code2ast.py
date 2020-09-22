import astroid
from astroid import *

code = '''
{"a":1, "b":2}
'''
ast = astroid.parse(code)
print(ast.repr_tree())

