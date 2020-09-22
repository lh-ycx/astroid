'''
import base64
def decode_base64(row):
    return_val = "####"
    base64_string = ''.join(chr(x) for x in bytearray(row))
    try:
        return_val = base64.b64decode(base64_string).decode('utf-8')
    except:   
        pass 
    return return_val

c= decode_base64(123)
'''



a = 1
c = 1 + 2
c