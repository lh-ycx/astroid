import astroid
from astroid import *

code = '''
udf((lambda x: FeatureProcessor.ExtractEmailAddressesFromJson(x, useAdlSchema, isSingleAddress, returnNames)), StringType())
'''
ast = astroid.extract_node(code)
print(ast.repr_tree())

