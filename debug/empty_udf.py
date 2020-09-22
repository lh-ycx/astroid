from pyspark.sql.functions import udf
from pyspark.sql import SparkSession, DataFrame

'''
from scipy import spatial

@udf
def distractor_beats_target(vectors):
    target, distractor, email = vectors

    cos_target = spatial.distance.cosine(np.array(target), np.array(email))
    cos_distractor = spatial.distance.cosine(np.array(distractor), np.array(email))
    return cos_target > cos_distractor
'''

some_path = 'aaa'

@udf
def simple_udf(df):
    return df["tainted_col"]
    # a = df["tainted_col"]
    # b = df["untainted_col"]
    # return a

spark = SparkSession.builder.appName(appName).getOrCreate()
df = spark.read.json(some_path)


simple_udf(df)