from pyspark.sql.functions import udf
from pyspark.sql.types import *
from debug.tests.extra.taint_instance import TaintInstance

import base64

def decode_base64(row):
    return_val = "####"
    base64_string = ''.join(chr(x) for x in bytearray(row))
    return_val = base64.b64decode(base64_string).decode('utf-8')
    return return_val

decode_udf = udf(__(lambda z: decode_base64(z)), StringType())

docs_text = docs.withColumn("DecodedText", decode_udf(docs['FileContent']))