import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode_outer, upper, concat, col, lit
from .extra import *


def mgdc_privacy_scrubber_contact(extracted_loc, denied_loc, scrub_loc, selected_cols):
    '''
    Privacy scrubbing for Contact data.

    Parameters:
    extracted_loc (string): Location of the extracted Contact data ('adl://path_to_extracted_loc_folder').
    denied_loc (string): Location of the excluded users file ('adl://path_to_denied_loc_file').
    scrub_loc (string): Location of the scrubbed Contact data after processing ('adl://path_to_scrub_loc_folder').
    additional_cols (string): List of additional columns the user requested on their dataset ('["Col1", "Col2", "Col3"]')
    '''

    spark = SparkSession.builder.appName('mgdc_privacy_scrubber_contact').getOrCreate()

    ## Read JSON extracted file
    extracted = spark.read.json(extracted_loc + '/*', mode='DROPMALFORMED')

    # Getting the default column names as they come in the extracted file
    default_cols = ["Id", "ODataType", "puser", "ptenant", "pAdditionalInfo", "datarow", "userrow", "pagerow",
                    "rowinformation"]
    extracted_default_cols = [c for c in extracted.columns for def_col in default_cols if c.lower() == def_col.lower()]

    ## Standarize column names
    extracted = extracted.toDF(*[c.lower() for c in extracted.columns])

    ## Read denied list file
    denied_list = spark.read.text(denied_loc)
    denied_list = denied_list.na.drop(subset=["value"])

    ## Explode the EmailAddresses column in the above dataframe
    ## Explode_outer ensures that rows containing null values for the exploded column do not get removed
    exploded = extracted.select(extracted.id.alias("explodedId"),
                                explode_outer('emailaddresses').alias('EmailAddresses'))

    # Get rows that contain denied email addresses
    rows_to_remove = exploded.join(denied_list,
                                   [upper(exploded.EmailAddresses.Address) == upper(denied_list.value)]).select(
        "explodedId")

    # Remove rows with denied list email addresses from original and select appropiate columns (default + additional)
    selected_cols = eval(selected_cols)
    scrubbed = extracted.join(rows_to_remove, [rows_to_remove.explodedId == extracted.id], "left_anti").select(
        *(extracted_default_cols + selected_cols))

    # Write to ADLS account in JSON format - If folder already exists, it is overwritten
    # Since Spark is a distributed framework, it will automatically split the output into multiple files
    scrubbed.na.fill("").coalesce(1).write.format('json').mode('overwrite').save(scrub_loc)

    # create dataframe containing puser@ptenant as first column and number of extracted rows as second column from extracted data
    num_rows_per_user_in_extracted = extracted.select(
        concat(col("puser"), lit("@"), col("ptenant")).alias('extractedUser'))
    num_rows_per_user_in_extracted = num_rows_per_user_in_extracted.groupby('extractedUser').count()
    num_rows_per_user_in_extracted = num_rows_per_user_in_extracted.select('extractedUser',
                                                                           col('count').alias('extractedCount'))

    # create dataframe containing puser@ptenant as first column and number of extracted rows as second column from scrubbed data
    num_rows_per_user_in_scrubbed = scrubbed.select(
        concat(col("puser"), lit("@"), col("ptenant")).alias('scrubbedUser'))
    num_rows_per_user_in_scrubbed = num_rows_per_user_in_scrubbed.groupby('scrubbedUser').count()
    num_rows_per_user_in_scrubbed = num_rows_per_user_in_scrubbed.select('scrubbedUser',
                                                                         col('count').alias('scrubbedCount'))

    # create dataframe containing puser@ptenant as first column and number of scrubbed rows as second column
    num_skipped_rows_per_user = num_rows_per_user_in_extracted.join(num_rows_per_user_in_scrubbed, [
        num_rows_per_user_in_extracted.extractedUser == num_rows_per_user_in_scrubbed.scrubbedUser], "left")
    num_skipped_rows_per_user = num_skipped_rows_per_user.select(col('extractedUser'),
                                                                 (col('extractedCount') - col('scrubbedCount')).alias(
                                                                     'SkippedRows'))

    # Read user metadata file into dataframe, create new dataframe with new columns for scrubbing row info, and write to output metadata folder
    user_metadata = spark.read.json(extracted_loc + '/metadata/UserMetadata/*')
    updated_user_md = user_metadata.join(num_skipped_rows_per_user,
                                         [num_skipped_rows_per_user.extractedUser == user_metadata.UserId],
                                         "left").select(
        [col('AllRows'), col('Endpoint'), col('ExtractionEndTime'), col('ExtractionStartTime'), col('JobId'),
         col('TotalRows'), col('UserId'), col('WasDropped'), col('SkippedRows'),
         col('SkippedRows').alias('SkippedRowsByEmailAddress')])
    updated_user_md.coalesce(1).write.format('json').mode('overwrite').save(scrub_loc + '/metadata/UserMetadata/')

    # Copy over job metadata file to output metadata folder
    spark.read.json(extracted_loc + '/metadata/JobMetadata/*').coalesce(1).write.format('json').mode('overwrite').save(
        scrub_loc + '/metadata/JobMetadata/')

    # Return to main
    return
