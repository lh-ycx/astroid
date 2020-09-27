import astroid
from astroid import *

code = '''
base64_string = ''.join(chr(x) for x in bytearray(row))
'''
ast = astroid.extract_node(code)
print(ast.repr_tree())

