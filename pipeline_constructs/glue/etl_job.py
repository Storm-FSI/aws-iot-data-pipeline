import aws_cdk.aws_glue as glue
import aws_cdk.aws_iam as iam
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_s3_deployment as s3deploy
from aws_cdk import RemovalPolicy
from aws_cdk import Stack
from constructs import Construct


class EtlJob(Construct):

    def __init__(
            self,
            scope: Construct,
            construct_id: str,
            job_name: str,
            worker_type: str,
            number_of_workers: int,
            max_concurrent_runs: int,
            job_bucket_name: str,
            job_bucket_deployment_path: str,
            script_location: str,
            job_params: dict,
            kinesis_stream_arn: str,
            output_bucket_arn: str
    ):
        super().__init__(scope, construct_id)

        job_assets_bucket = s3.Bucket(
            self,
            "S3JobAssetsBucket",
            bucket_name=job_bucket_name,
            block_public_access=s3.BlockPublicAccess(
                block_public_acls=False,
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

        job_assets_bucket.add_to_resource_policy(
            permission=iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                principals=[
                    iam.ServicePrincipal('glue.amazonaws.com')
                ],
                actions=[
                    "s3:ListBucket",
                    "s3:*Object*"
                ],
                resources=[
                    job_assets_bucket.bucket_arn,
                    "{}/*".format(job_assets_bucket.bucket_arn)
                ],
                conditions={
                    "StringEquals": {"aws:SourceAccount": Stack.of(self).account}
                }
            )
        )

        job_assets_bucket_deployment = s3deploy.BucketDeployment(
            self,
            "S3JobAssetsBucketDeployment",
            destination_bucket=job_assets_bucket,
            sources=[
                s3deploy.Source.asset(job_bucket_deployment_path)
            ]
        )

        job_role = iam.Role(
            self,
            "IamStreamingJobRole",
            role_name=job_name,
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")
            ],
            inline_policies={
                "AmazonKinesisGetRecordsPermission": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "kinesis:GetShardIterator",
                                "kinesis:GetRecords"
                            ],
                            resources=[
                                kinesis_stream_arn
                            ]
                        ),
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "kinesis:ListStreams",
                                "kinesis:ListShards"
                            ],
                            resources=[
                                "*"
                            ]
                        )
                    ]
                ),
                "AmazonS3ReadWriteObjectsPermission": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "s3:ListBucket",
                                "s3:*Object*"
                            ],
                            resources=[
                                job_assets_bucket.bucket_arn,
                                "{}/*".format(job_assets_bucket.bucket_arn),
                                output_bucket_arn,
                                "{}/*".format(output_bucket_arn)
                            ]
                        )
                    ]
                )
            }
        )

        self.job = glue.CfnJob(
            self,
            "Job",
            name=job_name,
            role=job_role.role_name,
            glue_version="3.0",
            command=glue.CfnJob.JobCommandProperty(
                name="gluestreaming",
                script_location=script_location,
                python_version="3"
            ),
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=max_concurrent_runs
            ),
            default_arguments={
                "--enable-metrics": "true",
                "--enable-spark-ui": "true",
                "--spark-event-logs-path": "s3://{}/sparkHistoryLogs/".format(job_assets_bucket.bucket_name),
                "--enable-job-insights": "false",
                "--enable-glue-datacatalog": "true",
                "--enable-continuous-cloudwatch-log": "true",
                "--job-bookmark-option": "job-bookmark-disable",
                "--job-language": "python",
                "--TempDir": "s3://{}/temporary/".format(job_assets_bucket.bucket_name)
            } | job_params,
            max_retries=3,
            worker_type=worker_type,
            number_of_workers=number_of_workers,
            execution_class="STANDARD"
        )

        self.job.node.add_dependency(job_assets_bucket_deployment)
        self.job.node.add_dependency(job_role)
