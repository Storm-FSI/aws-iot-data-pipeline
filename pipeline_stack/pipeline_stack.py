from pathlib import Path

import aws_cdk.aws_ec2 as ec2
import aws_cdk.aws_glue as glue
import aws_cdk.aws_iam as iam
import aws_cdk.aws_kinesis as kinesis
import aws_cdk.aws_s3 as s3
from aws_cdk import Duration
from aws_cdk import RemovalPolicy
from aws_cdk import Stack
from constructs import Construct

from pipeline_constructs.ec2.vpc import Vpc
from pipeline_constructs.glue.etl_job import EtlJob
from pipeline_constructs.glue.glue_kinesis_database import KinesisDatabase
from pipeline_constructs.glue.glue_output_database import OutputDatabase
from pipeline_constructs.redshift.redshift_cluster import RedshiftCluster
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
        redshift_db_name = self.node.try_get_context("redshift-db-name")
        redshift_table_name = self.node.try_get_context("redshift-table-name")
        redshift_admin_user = self.node.try_get_context("redshift-admin-user")
        redshift_admin_password = self.node.try_get_context("redshift-admin-password")
        redshift_node_type = self.node.try_get_context("redshift-node-type")
        redshift_number_of_nodes = self.node.try_get_context("redshift-number-of-nodes")

        # Prefix for resource names
        prefix = "{}-{}-{}".format(organization, environment, application)

        # Postfix for global resources names, e.g. S3 buckets
        postfix = "{}-{}".format(self.account, self.region)

        # SSM String Parameters
        kinesis_data_stream_name = "{}-stream".format(prefix)
        ssm_param_job_selected_fields_name = "{}-job-selected-fields".format(prefix)
        selected_fields_json_string = Path(dirpath, 'config', 'selected_fields.json').read_text()
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

        # VPC
        vpc = Vpc(
            self,
            "Vpc",
            vpc_name="{}-vpc".format(prefix)
        )

        # Redshift Cluster
        redshift_cluster = RedshiftCluster(
            self,
            "RedshiftCluster",
            cluster_identifier="{}-redshift-cluster".format(prefix),
            db_name=redshift_db_name,
            master_username=redshift_admin_user,
            master_password=redshift_admin_password,
            node_type=redshift_node_type,
            number_of_nodes=redshift_number_of_nodes,
            subnet_ids=vpc.vpc.select_subnets(subnet_type=ec2.SubnetType.PRIVATE_ISOLATED).subnet_ids,
            security_group_ids=[
                vpc.security_group.security_group_id
            ]
        )

        redshift_cluster.node.add_dependency(vpc)

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

        # Glue Redshift Connection
        glue_redshift_connection = glue.CfnConnection(
            self,
            "GlueConnectionRedshift",
            catalog_id=self.account,
            connection_input=glue.CfnConnection.ConnectionInputProperty(
                name="{}-redshift-connection".format(prefix),
                connection_type="JDBC",
                connection_properties={
                    "JDBC_ENFORCE_SSL": "false",
                    "JDBC_CONNECTION_URL": "jdbc:redshift://{}:{}/{}".format(
                        redshift_cluster.cluster.attr_endpoint_address,
                        redshift_cluster.cluster.attr_endpoint_port,
                        redshift_db_name
                    ),
                    "USERNAME": redshift_admin_user,
                    "PASSWORD": redshift_admin_password
                },
                physical_connection_requirements=glue.CfnConnection.PhysicalConnectionRequirementsProperty(
                    subnet_id=vpc.vpc.select_subnets(subnet_type=ec2.SubnetType.PRIVATE_ISOLATED).subnet_ids[0],
                    security_group_id_list=[
                        vpc.security_group.security_group_id
                    ],
                    # Currently this field must be populated, but it will be deprecated in the future.
                    availability_zone=vpc.vpc.select_subnets(subnet_type=ec2.SubnetType.PRIVATE_ISOLATED).subnets[
                        0].availability_zone
                )
            )
        )

        glue_redshift_connection.node.add_dependency(vpc)
        glue_redshift_connection.node.add_dependency(redshift_cluster)

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
                "--kinesisDB": glue_kinesis_database.database.database_input.name,
                "--kinesisTable": glue_kinesis_database.table.table_input.name,
                "--selectedFields": selected_fields_json_string,
                "--s3OutputBucket": s3_output_bucket.bucket_name,
                "--glueRedshiftConnection": glue_redshift_connection.connection_input.name,
                "--redshiftDB": redshift_cluster.cluster.db_name,
                "--redshiftTable": redshift_table_name
            },
            kinesis_stream_arn=kinesis_data_stream.stream_arn,
            output_bucket_arn=s3_output_bucket.bucket_arn,
            connections=[
                glue_redshift_connection.connection_input.name
            ]
        )

        glue_job.node.add_dependency(glue_kinesis_database)
        glue_job.node.add_dependency(s3_output_bucket)
        glue_job.node.add_dependency(glue_redshift_connection)
