from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from math import log, floor, ceil
from datetime import datetime
from datetime import timedelta
from metrics_calculator import METRIC_SCHEMA, count_terms, count_docs, checking_for_future_dates, \
    creating_terms_in_bucket_metric, print_df
import sys

TERM_HISTOGRAM_FILENAME = "termHistogram"
DOC_HISTOGRAM_FILENMAE = "docHistogram"
IDF_FILENAME = "idf_feature"
STATS_FILENAME = "idf_stats"
STATUS_FILENAME = "status"
CERT_PATH = "/home/yarn/cert.pfx"
CERT_PWD = ""
OBJECTSTORE_CONFIG_NAME = ""

DATA_SOURCE = "UserMailbox"

FORCE_RUN = True
PRINT_HISTOGRAMS = False
USE_VALUE_BUCKET = False
WRITE_TO_OBJECTSTORE = True
LOOKBACK_DAYS_ON_COLDSTART = 7
DAYS = 84  # 12 weeks
BUCKETS = 20
PRINT_METRICS = False
RETAIN_DAYS_IN_THE_FUTURE = False

TODAY_STR = ""

# schema
DATE = "date"
TERM = "term"
NUM_DOCS = "numDocs"
NUM_DOCS_HAVE_TERM = "numDocsHaveTerm"
IDF_VALUE = "idfValue"
TENANT_ID = "tenantId"
TYPE = "type"
READ_MERGED_FILES = False

SUCCESS_DATE = "success_date"

metric_df = None
number_of_future_dates = 0


def generate_features(spark_session, tenant_info):
    '''
        The idf feature is built incrementally. The new data will be merged into existing idf feature.

        Data schema
        ---------
        new data: get from ADL-S pushed from Griffin TBA
        after deserialized the new data, it could be converted to this format:
        tenantId \t type \t date \t term \t numDocsHaveTerm
        this is for every term on date.

        tenantId \t type \t date \t numDocs
        this is for every user on date.

        intermediate data: kept per tenant for incremental build
        for each term: tenantId \t type \t term \t date \t numDocsHaveTerm
        for tenant: tenantId \t type \t date \t numDocs

        idf feature: used for serving
        tenantId \t type \t term \t numDocsHaveTerm \t numDocs \t idfValue

        Parameters
        ----------
        spark_session : SparkSession
            Spark session passed in from framework. Each tenant has its own session.

        tenant_info : TenantInfo
            object holding all tenant specific info.

        Computation logic
        ---------
        1. read the intermediate data if existing
        2. read the new data
        3. update the intermediate data
        4. get the idf feature from updated intermediate data

       Remark
       -------------------------------
       due to we don't include the userId in the data to avoid GDPR;
       thus parsing from raw data is called twice, one to get the term docuemnt frequency, another is to
       get the document frequency.
    '''

    MSIT_TENANT_ID = "72f988bf-86f1-41af-91ab-2d7cd011db47"
    CHEVRON_TENANT_ID = "fd799da1-bfc1-4234-a91c-72b3a1cb9e26"
    ADVENTHEALTH_TENANT_ID = "6ac36678-7785-476f-be03-b68b403734c2"

    intermediate_term_histogram_df, intermediate_doc_histogram_df = \
        get_histogram_data(
            spark_session,
            tenant_info)

    global FORCE_RUN
    global PRINT_HISTOGRAMS
    global READ_MERGED_FILES

    # MSIT tenant: count term and doc histogram df rows before new data is added, to be removed after investigation
    if not FORCE_RUN and PRINT_HISTOGRAMS:
        print(
            "SystemLog: computing counts for " + tenant_info.get_tenant_id() + " tenant for term and doc histogram before update with new data (intermediate data).")
        print("SystemLog: Number of rows in term_histogram_df before new data" + str(
            intermediate_term_histogram_df.rdd.countApprox(60 * 1000, 0.50)))
        print("SystemLog: Number of rows in doc_histogram_df before new data" + str(
            intermediate_doc_histogram_df.rdd.countApprox(60 * 1000, 0.50)))

    new_data_df = \
        create_dataframe_from_new_data(
            spark_session,
            tenant_info)

    if (new_data_df == None):
        return

    new_term_df = \
        transform_data_format_as_term_level(
            spark_session,
            new_data_df)

    new_doc_df = \
        transform_data_format_as_user_level(
            spark_session,
            new_data_df)

    term_histogram_df = \
        update_term_histogram(
            spark_session,
            intermediate_term_histogram_df,
            new_term_df)

    doc_histogram_df = \
        update_doc_histogram(
            spark_session,
            intermediate_doc_histogram_df,
            new_doc_df)

    # MSIT tenant: count term and doc histogram df rows after new data is added, to be removed after investigation
    if tenant_info.get_tenant_id() == MSIT_TENANT_ID or PRINT_HISTOGRAMS:
        print(
            "SystemLog: computing counts for " + tenant_info.get_tenant_id() + " tenant for term and doc histogram after update with new data.")
        print("SystemLog: Number of rows in term_histogram_df after new data" + str(
            term_histogram_df.rdd.countApprox(60 * 1000, 0.50)))
        print("SystemLog: Number of rows in doc_histogram_df after new data" + str(
            doc_histogram_df.rdd.countApprox(60 * 1000, 0.50)))

    tenant_idf = \
        aggregate_idf(
            spark_session,
            term_histogram_df,
            doc_histogram_df)

    # count stats for MSIT: to be removed after investigation of tenant shard idf discrepancies finishes
    if tenant_info.get_tenant_id() == MSIT_TENANT_ID or PRINT_HISTOGRAMS:
        print("SystemLog: computing histogram stats for " + tenant_info.get_tenant_id() + " tenant for tenant_idf.")
        compute_stats_and_output(tenant_idf)

    print("SystemLog: convert idf value to bucket id")

    global USE_VALUE_BUCKET

    if USE_VALUE_BUCKET:
        print("SystemLog: using idf values to make buckets")
        tenant_idf = convert_idf_score_to_values_buckets(tenant_idf)
    else:
        print("SystemLog: using n-tile to make buckets")
        tenant_idf = convert_idf_score_to_buckets(tenant_idf)

    # count stats per bucket for specific tenants after bucketizing: to be removed after investigation of tenant shard idf discrepancies finishes
    if tenant_info.get_tenant_id() == MSIT_TENANT_ID or PRINT_HISTOGRAMS:
        print(
            "SystemLog: computing histogram stats for" + tenant_info.get_tenant_id() + " tenant for tenant_idf after bucketing.")
        compute_stats_and_output(tenant_idf)

    global number_of_future_dates
    if PRINT_METRICS:
        new_term_count_metric = count_terms(spark_session, tenant_info, new_term_df, TERM,
                                            "TermCountInNewData")
        intermediate_term_count_metric = count_terms(spark_session, tenant_info, intermediate_term_histogram_df, TERM,
                                                     "TermCountInIntermediateData")
        new_doc_count_metric = count_docs(spark_session, tenant_info, new_doc_df, NUM_DOCS, "NewDocCount")
        intermediate_doc_count_metric = count_docs(spark_session, tenant_info, intermediate_doc_histogram_df,
                                                   NUM_DOCS, "IntermediateDocCount")
        total_term_count_agg_idf = count_terms(spark_session, tenant_info, tenant_idf, TERM,
                                               "TotalNumberOfAggregatedTerms")
        new_terms_before_date_filer = subtract_df(spark_session, tenant_info, df=new_term_df,
                                                  df_to_sub=intermediate_term_histogram_df, column_name=TERM,
                                                  metric_name="NumberOfNewTermsBeforeDateFilter")
        terms_lost_through_time_decay = subtract_df(spark_session, tenant_info, df=intermediate_term_histogram_df,
                                                    df_to_sub=tenant_idf, column_name=TERM,
                                                    metric_name="NumberOfTermsLostThroughTimeDecay")
        new_terms_in_aggregated_data = subtract_df(spark_session, tenant_info, df=tenant_idf,
                                                   df_to_sub=intermediate_term_histogram_df, column_name=TERM,
                                                   metric_name="NumberOfNewTermsInAggregatedData")
        future_dates = checking_for_future_dates(spark_session, tenant_info, doc_histogram_df, DATE,
                                                 TODAY_STR)
        bucket_metric = creating_terms_in_bucket_metric(spark_session, tenant_info, tenant_idf,
                                                        IDF_VALUE, TERM, BUCKETS)
        number_of_dates_in_doc_df = count_terms(spark_session, tenant_info, intermediate_doc_histogram_df,
                                                DATE, "TotalNumberOfDatesInDocDfPreFilter")

        metric_df = spark_session.createDataFrame([], METRIC_SCHEMA)

        metric_df = metric_df.union(new_term_count_metric)
        metric_df = metric_df.union(intermediate_term_count_metric)
        metric_df = metric_df.union(new_doc_count_metric)
        metric_df = metric_df.union(intermediate_doc_count_metric)
        metric_df = metric_df.union(total_term_count_agg_idf)
        metric_df = metric_df.union(new_terms_before_date_filer)
        metric_df = metric_df.union(terms_lost_through_time_decay)
        metric_df = metric_df.union(new_terms_in_aggregated_data)
        metric_df = metric_df.union(future_dates)
        metric_df = metric_df.union(bucket_metric)
        metric_df = metric_df.union(number_of_dates_in_doc_df)

        print_df(metric_df, "Printing Metrics for " + tenant_info.get_tenant_id(), metric_df.schema.names, 50)

        if RETAIN_DAYS_IN_THE_FUTURE:
            number_of_future_dates = future_dates.collect()[0]['metricValue']

    elif RETAIN_DAYS_IN_THE_FUTURE:
        future_dates = checking_for_future_dates(spark_session, tenant_info, intermediate_doc_histogram_df, DATE,
                                                 TODAY_STR)
        number_of_future_dates = future_dates.collect()[0]['metricValue']

    save_result(
        tenant_info,
        term_histogram_df,
        doc_histogram_df,
        tenant_idf)

    save_status(spark_session, tenant_info)

    global WRITE_TO_OBJECTSTORE
    if WRITE_TO_OBJECTSTORE:
        print("SystemLog: Writing to ObjectStore ...")
        upload_to_object_store(spark_session, tenant_idf)
        from opencensus.common.runtime_context import RuntimeContext
        RuntimeContext.current_span.add_attribute('detail', 'Complete Success')


def compute_stats_and_output(tenant_idf):
    '''
        compute the histogram of computed tenant idf and print as system log.
    '''
    global BUCKETS
    if tenant_idf is not None:
        print("SystemLog: Number of rows in tenant_idf: " + str(tenant_idf.rdd.countApprox(60 * 1000, 0.50)))
        (startValues, counts) = tenant_idf.select(IDF_VALUE).rdd.flatMap(lambda r: r).histogram(BUCKETS)
        for start, count in zip(startValues, counts):
            print("SystemLog: startValue: %s, count: %s" % (start, count))
        print("SystemLog: Number of startValues:" + str(len(startValues)))
        print("SystemLog: Number of counts (should match size of buckets with healthy data):" + str(len(counts)))
        if (len(startValues) > 1):
            print("SystemLog: last start value (serves as end of range):" + str(startValues[-1]))
    else:
        print("SystemLog: the idf is None")


def upload_to_object_store(spark_session, tenant_idf):
    '''
        read the computed idf output and upload to objectStore
    '''

    global CERT_PATH
    global CERT_PWD
    global OBJECTSTORE_CONFIG_NAME
    print("ObjectStoreConfig: " + OBJECTSTORE_CONFIG_NAME)
    java_data_frame = tenant_idf._jdf

    spark_context = spark_session._sc
    table_accessor = spark_context._jvm.com.microsoft.kmobjectstorejava.idf.TenantIdfTableAccessor()
    table_accessor.writeDataToTable(java_data_frame, CERT_PATH, CERT_PWD, OBJECTSTORE_CONFIG_NAME)


def get_histogram_data(spark_session, tenant_info):
    '''
        read the term histogram and doc histogram data from ADL-S.
        these histograms are generated by this job, for the incremental building.
    '''
    global DOC_HISTOGRAM_FILENMAE
    global TERM_HISTOGRAM_FILENAME
    global FORCE_RUN
    last_successed_date = get_last_success_date(spark_session, tenant_info)
    if not last_successed_date:
        print(
            "SystemLog: " + "tenantID: " + tenant_info.get_tenant_id() + " This is the first time to run. No last success date.")
        return (None, None)
    else:
        print(
            "SystemLog: " + "tenantID: " + tenant_info.get_tenant_id() + " the last successed date " + last_successed_date)

    if FORCE_RUN:
        print(
            "SystemLog: " + "tenantID: " + tenant_info.get_tenant_id() + " this is forced to run, by ignoring previous intermediate histogram.")
        return (None, None)

    # the histogram data only computed to the day before success date
    # for example, today we run a job, we only consumed the data until yesterday because we don't know if today's data is ready or not;
    # while we will store today as the success date.
    histogram_date_str = get_date_before_given_date(last_successed_date)
    print("SystemLog: " + "tenantID: " + tenant_info.get_tenant_id() + " histogram date is: " + histogram_date_str)

    doc_exists = tenant_info.has_tenant_file(
        get_folder_within_date(tenant_info, histogram_date_str, DOC_HISTOGRAM_FILENMAE, relative=True), spark_session)
    term_exists = tenant_info.has_tenant_file(
        get_folder_within_date(tenant_info, histogram_date_str, TERM_HISTOGRAM_FILENAME, relative=True), spark_session)
    if not doc_exists:
        print("SystemLog: " + "tenantID: " + tenant_info.get_tenant_id() + " the doc histogram missing!!!")

    if not term_exists:
        print("SystemLog: " + "tenantID: " + tenant_info.get_tenant_id() + " the term histogram missing!!!")

    if (not doc_exists) or (not term_exists):
        return (None, None)

    # histogram data exists, we load them
    term_schema = StructType([
        StructField(TENANT_ID, StringType(), True),
        StructField(TYPE, StringType(), True),
        StructField(TERM, StringType(), True),
        StructField(DATE, DateType(), True),
        StructField(NUM_DOCS_HAVE_TERM, LongType(), True)
    ])

    doc_schema = StructType([
        StructField(TENANT_ID, StringType(), True),
        StructField(TYPE, StringType(), True),
        StructField(DATE, DateType(), True),
        StructField(NUM_DOCS, LongType(), True)
    ])

    doc_path = get_folder_within_date(tenant_info, histogram_date_str, DOC_HISTOGRAM_FILENMAE, relative=False)
    term_path = get_folder_within_date(tenant_info, histogram_date_str, TERM_HISTOGRAM_FILENAME, relative=False)

    term_histogram_df = spark_session \
        .read \
        .json(term_path, schema=term_schema)

    doc_histogram_df = spark_session \
        .read \
        .json(doc_path, schema=doc_schema)

    return term_histogram_df, doc_histogram_df


def create_dataframe_from_new_data(spark_session, tenant_info):
    '''
        read the new data from ADL-S. These data is pushed by Griffin TBA, without being processed.
    '''
    dates = get_new_data_dates(spark_session, tenant_info)
    existed_dates = [date for date in dates if
                     tenant_info.has_tenant_file(get_date_folder_under_root(tenant_info, date, relative=True),
                                                 spark_session)]

    if len(existed_dates) == 0:
        dates_str = ",".join(dates)
        print(
            "SystemLog: " + "tenantID: " + tenant_info.get_tenant_id() + " not able to find any input files: " + dates_str)
        from opencensus.common.runtime_context import RuntimeContext
        RuntimeContext.current_span.add_attribute('detail', 'Input data is empty for: ' + dates_str)
        return None

    io_paths = [get_date_folder_under_root(tenant_info, date, relative=False) for date in existed_dates]
    new_data_df = spark_session \
        .read \
        .format('json') \
        .load(io_paths)

    if (new_data_df == None):
        io_paths_str = ",".join(io_paths)
        print("SystemLog: " + "tenantID: " + tenant_info.get_tenant_id() + " none rdd after loading: " + io_paths_str)
        from opencensus.common.runtime_context import RuntimeContext
        RuntimeContext.current_span.add_attribute('detail', 'Input RDD is None')

    return new_data_df


def get_new_data_dates(spark_session, tenant_info):
    '''
        based on last success date and today's date, create the list of folders which contain un-processed data.
    '''
    global TODAY_STR
    global FORCE_RUN
    last_successed_date = get_last_success_date(spark_session, tenant_info)
    if not last_successed_date:
        print("SystemLog: " + "tenantID: " + tenant_info.get_tenant_id() + " cold start to get the new data folders")
        return get_new_data_dates_on_cold_start(spark_session, tenant_info)

    if FORCE_RUN:
        print(
            "SystemLog: " + "tenantID: " + tenant_info.get_tenant_id() + " due to force to run, we will get the new data as cold start")
        return get_new_data_dates_on_cold_start(spark_session, tenant_info)

    start = datetime.strptime(last_successed_date, '%Y-%m-%d')
    end = datetime.strptime(TODAY_STR, '%Y-%m-%d')
    step = timedelta(days=1)
    dates = []
    # we only consume the data util yesterday; because we don't know if today's data is ready or not.
    while start < end:
        dates.append(start.strftime('%Y-%m-%d'))
        start += step

    return dates


def get_new_data_dates_on_cold_start(spark_session, tenant_info):
    dates = []
    global TODAY_STR
    global LOOKBACK_DAYS_ON_COLDSTART
    yesterday_str = get_date_before_given_date(TODAY_STR)
    start = datetime.strptime(yesterday_str, '%Y-%m-%d')
    for step in range(LOOKBACK_DAYS_ON_COLDSTART):
        date = start + timedelta(days=-step)
        dates.append(date.strftime('%Y-%m-%d'))

    return dates


def transform_data_format_as_term_level(spark_session, raw_data_df):
    '''
        convert the raw data format into the contract for the idf computation at term level

        contract:
        tenantId -- the tenant id
        type -- type of term, such as kp
        date -- the date time in day
        term -- the term for this user on date
        numDocsHaveTerm -- the number of docs which contains term
    '''
    import base64
    schema = StructType([
        StructField(TENANT_ID, StringType(), True),
        StructField(TYPE, StringType(), True),
        StructField(TERM, StringType(), True),
        StructField(DATE, StringType(), True),
        StructField(NUM_DOCS_HAVE_TERM, LongType(), True)
    ])

    if raw_data_df == None:
        print("SystemLog: new data is none!!!, can't get the term df.")
        return None

    cleaned_raw_term = spark_session \
        .createDataFrame(
        raw_data_df.rdd
            .map(lambda r: Row(tenantId=r.TenantId, type='kp', info=base64.b64decode(r.Properties)))
            .flatMap(lambda r: get_term_df(r)),
        schema)

    term_df = cleaned_raw_term \
        .select(TENANT_ID, TYPE, TERM, to_date(cleaned_raw_term[DATE]).alias(DATE), NUM_DOCS_HAVE_TERM)

    return term_df


def transform_data_format_as_user_level(spark_session, raw_data_df):
    '''
        convert the raw data format into the contract for the idf computation

        contract:
        tenantId -- the tenant id
        type -- type of term, such as kp
        date -- the date time in day
        numDocs -- the number of docs
    '''
    import base64
    schema = StructType([
        StructField(TENANT_ID, StringType(), True),
        StructField(TYPE, StringType(), True),
        StructField(DATE, StringType(), True),
        StructField(NUM_DOCS, LongType(), True)
    ])

    if raw_data_df == None:
        print("SystemLog: new data is none!!!, can't get the doc df.")
        return None

    cleaned_raw_term = spark_session \
        .createDataFrame(
        raw_data_df.rdd
            .map(lambda r: Row(tenantId=r.TenantId, type='kp', info=base64.b64decode(r.Properties)))
            .flatMap(lambda r: get_doc_count(r)),
        schema)

    term_df = cleaned_raw_term \
        .select(TENANT_ID, TYPE, to_date(cleaned_raw_term[DATE]).alias(DATE), NUM_DOCS)

    return term_df


def get_term_df(r):
    '''
        This function has the implicit dependency on the schema of IdfPerDay
    '''
    import json
    result = []
    if r.info:
        obj = json.loads(r.info.decode('utf-8'))
        for term in obj["Terms"]:
            result.append((r.tenantId, r.type, term, obj["Date"][:10], obj["Terms"][term]))

    return result


def get_doc_count(r):
    '''
        This function has the implicit dependency on the schema of IdfPerDay
    '''
    import json
    result = []
    if r.info:
        obj = json.loads(r.info.decode('utf-8'))
        result.append((r.tenantId, r.type, obj["Date"][:10], obj["DocumentsCount"]))

    return result


def update_term_histogram(spark_session, term_histogram_df, new_term_df):
    if new_term_df == None:
        print("SystemLog: new term df is none!!!")
        return term_histogram_df

    term_doc_count = new_term_df \
        .select(TENANT_ID, TYPE, TERM, DATE, NUM_DOCS_HAVE_TERM) \
        .groupby(TENANT_ID, TYPE, TERM, DATE) \
        .agg({NUM_DOCS_HAVE_TERM: "sum"}) \
        .toDF(TENANT_ID, TYPE, TERM, DATE, NUM_DOCS_HAVE_TERM)

    if term_histogram_df == None:
        return term_doc_count

    updated_term_histogram_df = term_histogram_df \
        .union(term_doc_count) \
        .groupby(TENANT_ID, TYPE, TERM, DATE) \
        .agg({NUM_DOCS_HAVE_TERM: "sum"}) \
        .toDF(TENANT_ID, TYPE, TERM, DATE, NUM_DOCS_HAVE_TERM)

    return updated_term_histogram_df


def update_doc_histogram(spark_session, doc_histogram_df, new_doc_df):
    if new_doc_df == None:
        print("SystemLog: new doc df is none!!!")
        return doc_histogram_df

    doc_count = new_doc_df \
        .select(TENANT_ID, TYPE, DATE, NUM_DOCS) \
        .groupby(TENANT_ID, TYPE, DATE) \
        .agg({NUM_DOCS: "sum"}) \
        .toDF(TENANT_ID, TYPE, DATE, NUM_DOCS)

    if doc_histogram_df == None:
        return doc_count

    updated_doc_histogram_df = doc_histogram_df \
        .union(doc_count) \
        .groupby(TENANT_ID, TYPE, DATE) \
        .agg({NUM_DOCS: "sum"}) \
        .toDF(TENANT_ID, TYPE, DATE, NUM_DOCS)

    return updated_doc_histogram_df


def aggregate_idf(spark_session, term_histogram_df, doc_histogram_df):
    '''
        get the idf feature from the term and doc histogram.

        v0: simply add all number of documents without time decay
    '''
    if term_histogram_df == None or doc_histogram_df == None:
        print("SystemLog: the histogram is none!!! cannot proceed to get the idf.")
        return None

    latest_term_histogram_df, latest_doc_histogram_df = \
        keep_latest_data(spark_session, term_histogram_df, doc_histogram_df)

    term_count_agg = latest_term_histogram_df \
        .groupby(TENANT_ID, TYPE, TERM) \
        .agg({NUM_DOCS_HAVE_TERM: "sum"}) \
        .toDF(TENANT_ID, TYPE, TERM, NUM_DOCS_HAVE_TERM)

    doc_count_agg = latest_doc_histogram_df \
        .select(NUM_DOCS) \
        .agg({NUM_DOCS: "sum"}) \
        .toDF(NUM_DOCS)

    term_count_with_doc_count = term_count_agg.crossJoin(doc_count_agg)

    rdd_with_idf = term_count_with_doc_count \
        .rdd \
        .filter(lambda r: r[NUM_DOCS_HAVE_TERM] != 0 and (r[NUM_DOCS] * 1.0 / r[NUM_DOCS_HAVE_TERM] > 0)) \
        .map(lambda r: Row(tenantId=r[TENANT_ID], type=r[TYPE], term=r[TERM], numDocsHaveTerm=r[NUM_DOCS_HAVE_TERM],
                           numDocs=r[NUM_DOCS], idfValue=log(r[NUM_DOCS] * 1.0 / r[NUM_DOCS_HAVE_TERM])))
    print("SystemLog: generate idf. Well done!!!")
    if rdd_with_idf.isEmpty():
        from opencensus.common.runtime_context import RuntimeContext
        RuntimeContext.current_span.add_attribute('detail', 'RDD with IDF is Empty')
    return spark_session.createDataFrame(rdd_with_idf)


def keep_latest_data(spark_session, term_histogram, doc_histogram):
    '''
        only keep the latest DAYS data
    '''
    global DAYS
    global RETAIN_DAYS_IN_THE_FUTURE
    global number_of_future_dates

    if RETAIN_DAYS_IN_THE_FUTURE:
        latest_dates_df = doc_histogram.select(DATE).sort(DATE, ascending=False).limit(DAYS + number_of_future_dates)
    else:
        latest_dates_df = doc_histogram.select(DATE).sort(DATE, ascending=False).limit(DAYS)

    latest_doc_histogram = doc_histogram \
        .join(latest_dates_df, doc_histogram[DATE] == latest_dates_df[DATE], "inner") \
        .select(doc_histogram[TENANT_ID], doc_histogram[TYPE], doc_histogram[DATE], doc_histogram[NUM_DOCS])

    latest_term_histogram = term_histogram \
        .join(latest_dates_df, term_histogram[DATE] == latest_dates_df[DATE], "inner") \
        .select(term_histogram[TENANT_ID], term_histogram[TYPE], term_histogram[DATE], term_histogram[TERM],
                term_histogram[NUM_DOCS_HAVE_TERM])

    return latest_term_histogram, latest_doc_histogram


def convert_idf_score_to_buckets(tenant_idf):
    global IDF_VALUE
    global BUCKETS
    over_all = Window.orderBy(IDF_VALUE)
    tenant_idf_with_bucket = tenant_idf.withColumn(IDF_VALUE, ntile(BUCKETS).over(over_all))
    print("SystemLog: Done converting to bucketized score")
    return tenant_idf_with_bucket


def convert_idf_score_to_values_buckets(tenant_idf):
    global IDF_VALUE
    global BUCKETS

    minMaxDict = tenant_idf.select(IDF_VALUE).groupby() \
        .agg(min(IDF_VALUE).alias('MinIDF'), max(IDF_VALUE).alias('MaxIDF')) \
        .first().asDict()

    min_idf = minMaxDict['MinIDF']
    max_idf = minMaxDict['MaxIDF']

    if isinstance(min_idf, float):
        print("SystemLog: Min idf value: ", min_idf)
    else:
        print("SystemLog: Corrupt data, min idf value not a float")

    if isinstance(max_idf, float):
        print("SystemLog: Max idf value: ", max_idf)
    else:
        print("SystemLog: Corrupt data, max idf value not a float")

    # min and max to floor and ceiling
    min_idf = floor(min_idf)
    max_idf = ceil(max_idf)

    range_per_bucket = (max_idf - min_idf) * 1.0 / (BUCKETS + 1)

    tenant_idf_with_bucket = tenant_idf.withColumn(IDF_VALUE,
                                                   ((tenant_idf[IDF_VALUE] - min_idf) / range_per_bucket).cast(
                                                       IntegerType()))
    print("SystemLog: Done converting to bucketized score using min and max idf values")

    return tenant_idf_with_bucket


def save_result(tenant_info, term_histogram_df, doc_histogram_df, tenant_idf):
    '''
        save the term and doc histogram for later incremental building.
        save the idf feature for online serving.
    '''
    global TODAY_STR
    global IDF_FILENAME
    global TERM_HISTOGRAM_FILENAME
    global DOC_HISTOGRAM_FILENMAE
    histogram_date_str = get_date_before_given_date(TODAY_STR)
    if term_histogram_df is not None:
        term_histogram_df \
            .write \
            .json(get_folder_within_date(tenant_info, histogram_date_str, TERM_HISTOGRAM_FILENAME, relative=False),
                  mode="overwrite")
    else:
        print("SystemLog: the term histogram is None")

    if doc_histogram_df is not None:
        doc_histogram_df \
            .write \
            .json(get_folder_within_date(tenant_info, histogram_date_str, DOC_HISTOGRAM_FILENMAE, relative=False),
                  mode="overwrite")
    else:
        print("SystemLog: the doc histogram is None")

    if tenant_idf is not None:
        tenant_idf \
            .write \
            .json(get_folder_under_root(tenant_info, IDF_FILENAME, relative=False), mode="overwrite")
    else:
        print("SystemLog: the idf is None")


def get_folder_within_date(tenant_info, date, filename, relative=True):
    '''
        get the file path by combining the tenant info, date and filename

        the folder is: root/2019/07/01/hist
    '''
    return get_date_folder_under_root(tenant_info, date, relative=relative) + "/" + filename


def get_date_folder_under_root(tenant_info, date, relative=True):
    '''
    get the path for given date;
    the date is string in the form: yyyy-mm-dd.

    the folder is: root/2019/07/01
    '''
    global READ_MERGED_FILES

    folder = get_root(tenant_info, relative=relative) + "/" + date.replace("-", "/")
    if READ_MERGED_FILES:
        return folder + "_merged"

    return folder


def get_folder_under_root(tenant_info, filename, relative=True):
    '''
        get the file path stored in the tenant folder

        the folder is: root/idf
    '''
    return get_root(tenant_info, relative=relative) + "/" + filename


def get_root(tenant_info, relative=True):
    '''
        the root folder where the idf and stats are stored.

        for example, the root folder is:
        tenant_id/UserMailbox

        which means the idf is computed from user mailbox
    '''
    global DATA_SOURCE
    if relative:
        return "/" + DATA_SOURCE
    else:
        return tenant_info.get_tenant_io_prefix() + DATA_SOURCE


def get_date_before_given_date(given_date):
    before_date = datetime.strptime(
        given_date, '%Y-%m-%d') + timedelta(days=-1)
    before_date_str = before_date.strftime('%Y-%m-%d')
    return before_date_str


def save_status(spark_session, tenant_info):
    status_schema = StructType([
        StructField(SUCCESS_DATE, StringType(), True)
    ])

    today_str_collect = [TODAY_STR]
    df_status = spark_session.createDataFrame([today_str_collect], status_schema)
    status_path = get_folder_under_root(tenant_info, STATUS_FILENAME, relative=False)
    df_status.repartition(1).write.json(status_path, mode='overwrite')


def get_last_success_date(spark_session, tenant_info):
    global STATUS_FILENAME

    if not tenant_info.has_tenant_file(get_folder_under_root(tenant_info, STATUS_FILENAME, relative=True),
                                       spark_session):
        return None

    status_path = get_folder_under_root(tenant_info, STATUS_FILENAME, relative=False)
    status_schema = StructType([
        StructField(SUCCESS_DATE, StringType(), True)
    ])
    status_df = spark_session \
        .read \
        .json(status_path, schema=status_schema)

    if status_df.rdd.isEmpty():
        return None
    else:
        return status_df.select(SUCCESS_DATE).rdd.flatMap(lambda x: x).collect()[0]


def parse_args(args):
    result = dict()
    size = len(args)
    for i in range(0, size - 1):
        if (args[i].startswith("--")) and i + 1 < size and not args[i + 1].startswith("--"):
            result[args[i].strip().lower()] = args[i + 1].strip()
    return result


def main():
    from aggregation_controller import run_for_tenants
    args_dict = parse_args(sys.argv)
    global TERM_HISTOGRAM_FILENAME
    global DOC_HISTOGRAM_FILENMAE
    global IDF_FILENAME
    global STATS_FILENAME
    global STATUS_FILENAME
    global DATA_SOURCE
    global FORCE_RUN
    global PRINT_HISTOGRAMS
    global USE_VALUE_BUCKET
    global WRITE_TO_OBJECTSTORE
    global OBJECTSTORE_CONFIG_NAME
    global LOOKBACK_DAYS_ON_COLDSTART
    global DAYS
    global BUCKETS
    global CERT_PATH
    global CERT_PWD
    global PRINT_METRICS
    global RETAIN_DAYS_IN_THE_FUTURE
    global READ_MERGED_FILES

    # should match the aether interface
    TERM_HISTOGRAM_FILENAME = args_dict.get(
        "--TermHist".lower(), "termHistogram")
    DOC_HISTOGRAM_FILENMAE = args_dict.get("--DocHist".lower(), "docHistogram")
    IDF_FILENAME = args_dict.get("--IdfFile".lower(), "idf_feature")
    STATS_FILENAME = args_dict.get("--ResultStats".lower(), "idf_stats")
    STATUS_FILENAME = args_dict.get("--StatusFile".lower(), "status")
    CERT_PATH = args_dict.get(
        "--CertPath".lower(), "/home/yarn/cert.pfx")
    CERT_PWD = args_dict.get("--CertPwd".lower(), "")
    DATA_SOURCE = args_dict.get("--DataSource".lower(), "UserMailbox")
    FORCE_RUN = args_dict.get("--ForceToRun".lower(), "True") in ['true', 'True', 'TRUE', 'yes', 'Yes', 'YES']
    PRINT_HISTOGRAMS = args_dict.get("--PrintHistograms".lower(), "False") in ['true', 'True', 'TRUE', 'yes', 'Yes',
                                                                               'YES']
    USE_VALUE_BUCKET = args_dict.get("--UseValueBucket".lower(), "False") in ['true', 'True', 'TRUE', 'yes', 'Yes',
                                                                              'YES']
    WRITE_TO_OBJECTSTORE = args_dict.get("--WriteToObjectstore".lower(), "True") in ['true', 'True', 'TRUE', 'yes',
                                                                                     'Yes', 'YES']
    OBJECTSTORE_CONFIG_NAME = args_dict.get("--ObjectstoreConfigName".lower(), "")
    LOOKBACK_DAYS_ON_COLDSTART = int(
        args_dict.get("--DaysOnColdStart".lower(), "7"))
    DAYS = int(args_dict.get("--RangeInDays".lower(), "84"))
    BUCKETS = int(args_dict.get("--Buckets".lower(), 20))
    PRINT_METRICS = args_dict.get("--printmetrics", "False") in ['true', 'True', 'TRUE', 'yes', 'Yes', 'YES']
    RETAIN_DAYS_IN_THE_FUTURE = args_dict.get("--retainfuturedays", "False") in ['true', 'True', 'TRUE', 'yes', 'Yes',
                                                                                 'YES']
    READ_MERGED_FILES = args_dict.get("--ReadMergedFiles".lower(), "False") in ['true', 'True', 'TRUE', 'yes', 'Yes',
                                                                                'YES']

    print("SystemLog: term_histogram_file: " + TERM_HISTOGRAM_FILENAME)
    print("SystemLog: doc_histogram_file: " + DOC_HISTOGRAM_FILENMAE)
    print("SystemLog: lookback days on cold start: " +
          str(LOOKBACK_DAYS_ON_COLDSTART))
    print("SystemLog: idf_file: " + IDF_FILENAME)
    print("SystemLog: stats_file: " + STATS_FILENAME)
    print("SystemLog: status file: " + STATUS_FILENAME)
    print("SystemLog: days: " + str(DAYS))
    print("SystemLog: buckets: " + str(BUCKETS))
    print("SystemLog: force_torun: " + str(FORCE_RUN))
    print("SystemLog: write_to_objectstore: " + str(WRITE_TO_OBJECTSTORE))
    print("SystemLog: print_metrics: " + str(PRINT_METRICS))
    print("SystemLog: read_merged_files: " + str(READ_MERGED_FILES))

    # initilizes global variables not passed from external
    global TODAY_STR
    TODAY_STR = datetime.today().strftime('%Y-%m-%d')
    print("SystemLog: today is: " + TODAY_STR)
    print("CertPath: " + CERT_PATH)

    run_for_tenants(generate_features)


if __name__ == '__main__':
    main()



