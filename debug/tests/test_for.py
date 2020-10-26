from pyspark.sql.functions import udf
from pyspark.sql.types import *
import re


def remove_email_url(line):
    line = re.sub(r'\S+@\S+', '', line)
    line = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', line)
    line = re.sub(r'www[.](?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', line)
    return line


def clean_up(input):
    input_lines = input.split('\r\n')
    return_line = ''
    try:
        previous = ''
        for line in input_lines:
            line = remove_email_url(line)
            #line = remove_form_lines(line)
            #line = remove_phone_number(line)
            #line = remove_non_ascii(line)
            #line = remove_bullets(line)
            #line = remove_small_unformed(line)
            #line = remove_alpha(line)
            #line = replace_html_entities(line)
            #line = remove_near_dup(previous, line)
            #line = line.strip()
            if line != '':
                previous = line
                line += "\n"
                return_line += line
        return_line += '\n'
    except:
        pass
    return return_line


clean_up_udf = udf(lambda z: __(clean_up(z)), StringType())
