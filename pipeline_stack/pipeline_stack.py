from pathlib import Path
from string import Template

import aws_cdk.aws_cloudwatch as cloudwatch
import aws_cdk.aws_glue as glue
import aws_cdk.aws_iam as iam
import aws_cdk.aws_kinesis as kinesis
import aws_cdk.aws_s3 as s3
from aws_cdk import Duration
from aws_cdk import RemovalPolicy
from aws_cdk import Stack
from constructs import Construct
from pipeline_constructs.glue.etl_job import EtlJob
from pipeline_constructs.glue.glue_kinesis_database import KinesisDatabase
from pipeline_constructs.glue.glue_output_database import OutputDatabase
from pipeline_constructs.ssm.string_parameters import StringParameters


class PipelineStack(Stack):

    def __init__(
            self,
            scope: Construct,
            construct_id: str,
            organization: str,
            environment: str,
            application: str,
            **kwargs
    ):
        super().__init__(scope, construct_id, **kwargs)

        dirpath = Path(__file__).parent.resolve()

        # Context variables
        kinesis_shard_count = self.node.try_get_context("kinesis-shard-count")
        job_worker_type = self.node.try_get_context("job-worker-type")
        job_number_of_workers = self.node.try_get_context("job-number-of-workers")
        job_max_concurrent_runs = self.node.try_get_context("job-max-concurrent-runs")
        job_window_size = self.node.try_get_context("job-window-size")

        # Prefix for resource names
        prefix = "{}-{}-{}".format(organization, environment, application)

        # Postfix for globally unique resource names, e.g. S3 buckets
        postfix = "{}-{}".format(self.account, self.region)

        # SSM String Parameters
        kinesis_data_stream_name = "{}-stream".format(prefix)
        ssm_param_job_selected_fields_name = "{}-job-selected-fields".format(prefix)
        selected_fields_json_string = Path(dirpath, 'config', 'kpis', 'kpi_sample.json').read_text()
        s3_output_bucket_name = "{}-output-bucket-{}".format(prefix, postfix)
        glue_output_database_name = "{}-output-database".format(prefix)
        glue_output_table_name = "{}-output-table".format(prefix)

        ssm_string_parameters = StringParameters(
            self,
            "SsmStringParameters",
            kinesis_data_stream_name,
            ssm_param_job_selected_fields_name,
            selected_fields_json_string,
            s3_output_bucket_name,
            glue_output_database_name,
            glue_output_table_name
        )

        # S3 Output Bucket
        s3_output_bucket = s3.Bucket(
            self,
            "S3OutputBucket",
            bucket_name=s3_output_bucket_name,
            block_public_access=s3.BlockPublicAccess(
                block_public_acls=True,
                block_public_policy=True,
                ignore_public_acls=True,
                restrict_public_buckets=True
            ),
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            versioned=False,
            removal_policy=RemovalPolicy.RETAIN
        )

        # Glue Output Database
        glue_output_database = OutputDatabase(
            self,
            "GlueOutputDatabase",
            db_name=glue_output_database_name,
            table_name=glue_output_table_name,
            bucket_name=s3_output_bucket.bucket_name
        )

        glue_output_database.node.add_dependency(s3_output_bucket)

        # S3 Athena Query Results Bucket
        s3_athena_query_results_bucket = s3.Bucket(
            self,
            "S3AthenaQueryResultsBucket",
            bucket_name="{}-athena-results-{}".format(prefix, postfix),
            block_public_access=s3.BlockPublicAccess(
                block_public_acls=True,
                block_public_policy=True,
                ignore_public_acls=True,
                restrict_public_buckets=True
            ),
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            versioned=False,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )

        s3_athena_query_results_bucket.add_to_resource_policy(
            permission=iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                principals=[
                    iam.ServicePrincipal('athena.amazonaws.com')
                ],
                actions=[
                    "s3:ListBucket",
                    "s3:*Object*"
                ],
                resources=[
                    s3_athena_query_results_bucket.bucket_arn,
                    "{}/*".format(s3_athena_query_results_bucket.bucket_arn)
                ],
                conditions={
                    "StringEquals": {"aws:SourceAccount": self.account}
                }
            )
        )

        # Kinesis Data Stream
        kinesis_data_stream = kinesis.Stream(
            self,
            "KinesisDataStream",
            stream_name=kinesis_data_stream_name,
            stream_mode=kinesis.StreamMode.PROVISIONED,
            shard_count=kinesis_shard_count,
            encryption=kinesis.StreamEncryption.UNENCRYPTED,
            retention_period=Duration.hours(24)
        )

        # Glue Kinesis Database
        glue_kinesis_database = KinesisDatabase(
            self,
            "GlueKinesisDatabase",
            db_name="{}-kinesis-database".format(prefix),
            table_name="{}-kinesis-table".format(prefix),
            data_stream_name=kinesis_data_stream.stream_name,
            data_stream_arn=kinesis_data_stream.stream_arn
        )

        glue_kinesis_database.node.add_dependency(kinesis_data_stream)

        # Glue ETL Job
        glue_job = EtlJob(
            self,
            "GlueEtlJob",
            job_name="{}-job".format(prefix),
            worker_type=job_worker_type,
            number_of_workers=job_number_of_workers,
            max_concurrent_runs=job_max_concurrent_runs,
            job_bucket_name="{}-jobassets-{}".format(prefix, postfix),
            job_bucket_deployment_path=str(Path(dirpath, 'runtime', 'glue_job_assets_bucket')),
            script_location="s3://{}-jobassets-{}/job_script.py".format(prefix, postfix),
            job_params={
                "--windowSize": job_window_size,
                "--kinesisDB": glue_kinesis_database.database.database_input.name,
                "--kinesisTable": glue_kinesis_database.table.table_input.name,
                "--selectedFields": selected_fields_json_string,
                "--s3OutputBucket": s3_output_bucket.bucket_name
            },
            kinesis_stream_arn=kinesis_data_stream.stream_arn,
            output_bucket_arn=s3_output_bucket.bucket_arn,
        )

        glue_job.node.add_dependency(glue_kinesis_database)
        glue_job.node.add_dependency(s3_output_bucket)

        # Cloudwatch Dashboard
        dashboard = cloudwatch.CfnDashboard(
            self,
            "Dashboard",
            dashboard_name=prefix,
            dashboard_body=Template(Path(dirpath, 'config', 'dashboards', 'dashboard_sample.json').read_text())
            .substitute(
                region=self.region,
                kinesis_data_stream_name=kinesis_data_stream.stream_name,
                glue_job_name=glue_job.job.name,
                s3_output_bucket_name=s3_output_bucket.bucket_name,
            )
        )

        dashboard.node.add_dependency(kinesis_data_stream)
        dashboard.node.add_dependency(glue_job)
        dashboard.node.add_dependency(s3_output_bucket)