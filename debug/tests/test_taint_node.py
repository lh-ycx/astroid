from pyspark.sql.functions import udf
from pyspark.sql.types import *

import base64

def decode_base64(row_1, row_2, row_3):
    return_val = "####"
    base64_string = ''.join(chr(x) for x in bytearray(row_1))
    return_val = base64.b64decode(base64_string).decode('utf-8')
    return return_val

decode_udf = udf(lambda x,y,z: __(decode_base64(x,y,z)), StringType())

docs_text = docs.withColumn("DecodedText", decode_udf(docs['FileContent']))
