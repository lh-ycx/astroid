import astroid
from astroid import *

code = '''
extracted = spark.read.json(extracted_loc + '/*', mode='DROPMALFORMED')
'''
ast = astroid.parse(code)
print(ast.repr_tree())

