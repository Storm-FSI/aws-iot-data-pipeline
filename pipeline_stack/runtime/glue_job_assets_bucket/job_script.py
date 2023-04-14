import datetime
import json
import sys

from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "windowSize",
        "selectedFields",
        "kinesisDB",
        "kinesisTable",
        "s3OutputBucket"
    ]
)
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Read configuration
param_window_size = args['windowSize']
param_selected_fields = json.loads(args['selectedFields'])
param_kinesis_db = args['kinesisDB']
param_kinesis_table = args['kinesisTable']
param_s3_output_bucket = args['s3OutputBucket']

# Script generated for node Kinesis Stream
dataframe_KinesisStream_node = glueContext.create_data_frame.from_catalog(
    database=param_kinesis_db,
    table_name=param_kinesis_table,
    additional_options={
        "startingPosition": "TRIM_HORIZON",
        "inferSchema": "true",
        "classification": "json",
        "maxFetchTimeInMs": 10000
    },
    transformation_ctx="dataframe_KinesisStream_node"
)

# Select fields from config file on AWS SSM and create new colums for each field
def select_fields_from_kinesisDF(df, selected_fields):
    selected_df = df.select("*", *[eval(v).alias(k) for k,v in selected_fields.items()])
    return selected_df


def processBatch(data_frame, batchId):
    if data_frame.count() > 0:
        KinesisStream_node1 = DynamicFrame.fromDF(
            data_frame, glueContext, "from_data_frame"
        )

        # Convert kinesisStream to Dataframe
        kinesis_df = KinesisStream_node1.toDF()

        # Create a dictionary of selected fields based on configuration
        selected_fields = {}
        for field in param_selected_fields['selected-fields']:
            for col_name, col_path in field.items():
                selected_fields[col_name] = col_path

        # Select fields from the DF
        selected_kinesisDF = select_fields_from_kinesisDF(kinesis_df, selected_fields)

        # Remove the old columns from DF and keep the new columns
        columns_to_keep = [col_name for field in param_selected_fields['selected-fields'] for col_name, _ in field.items()]
        selected_kinesisDF = selected_kinesisDF.select(columns_to_keep)

        # Create a DynamicFrame for the new selected fields Dataframe
        selected_kinesisFields_dyf = DynamicFrame.fromDF(selected_kinesisDF, glueContext, "dyf")

        now = datetime.datetime.now()
        year = now.year
        month = now.month
        day = now.day
        hour = now.hour

        # Script generated for node S3 bucket
        S3bucket_node_path = (
                "s3://"
                + param_s3_output_bucket
                + "/ingest_year="
                + "{:0>4}".format(str(year))
                + "/ingest_month="
                + "{:0>2}".format(str(month))
                + "/ingest_day="
                + "{:0>2}".format(str(day))
                + "/ingest_hour="
                + "{:0>2}".format(str(hour))
                + "/"
        )

        S3bucket_node = glueContext.write_dynamic_frame.from_options(
            frame=selected_kinesisFields_dyf,
            connection_type="s3",
            format="json",
            connection_options={"path": S3bucket_node_path, "partitionKeys": []},
            transformation_ctx="S3bucket_node"
        )


glueContext.forEachBatch(
    frame=dataframe_KinesisStream_node,
    batch_function=processBatch,
    options={
        "windowSize": param_window_size,
        "checkpointLocation": args["TempDir"] + "/" + args["JOB_NAME"] + "/checkpoint/"
    }
)
job.commit()
