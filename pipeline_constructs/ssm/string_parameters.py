import aws_cdk.aws_ssm as ssm
from constructs import Construct


class StringParameters(Construct):

    def __init__(
            self,
            scope: Construct,
            construct_id: str,
            kinesis_data_stream_name: str,
            glue_job_selected_fields_name: str,
            glue_job_selected_fields_value: str,
            s3_output_bucket_name: str,
            glue_output_database_name: str,
            glue_output_table_name: str
    ):
        super().__init__(scope, construct_id)

        ssm_parameter_kinesis_data_stream = ssm.StringParameter(
            self,
            "SsmStringParameterKinesisDataStream",
            parameter_name=kinesis_data_stream_name,
            tier=ssm.ParameterTier.STANDARD,
            data_type=ssm.ParameterDataType.TEXT,
            string_value=kinesis_data_stream_name
        )

        ssm_parameter_glue_job_config = ssm.StringParameter(
            self,
            "SsmStringParameterGlueJobConfig",
            parameter_name=glue_job_selected_fields_name,
            tier=ssm.ParameterTier.STANDARD,
            data_type=ssm.ParameterDataType.TEXT,
            string_value=glue_job_selected_fields_value
        )

        ssm_parameter_s3_output_bucket = ssm.StringParameter(
            self,
            "SsmStringParameterS3OutputBucket",
            parameter_name=s3_output_bucket_name,
            tier=ssm.ParameterTier.STANDARD,
            data_type=ssm.ParameterDataType.TEXT,
            string_value=s3_output_bucket_name
        )

        ssm_parameter_glue_output_database = ssm.StringParameter(
            self,
            "SsmStringParameterGlueOutputDatabase",
            parameter_name=glue_output_database_name,
            tier=ssm.ParameterTier.STANDARD,
            data_type=ssm.ParameterDataType.TEXT,
            string_value=glue_output_database_name
        )

        ssm_parameter_glue_output_table = ssm.StringParameter(
            self,
            "SsmStringParameterGlueOutputTable",
            parameter_name=glue_output_table_name,
            tier=ssm.ParameterTier.STANDARD,
            data_type=ssm.ParameterDataType.TEXT,
            string_value=glue_output_table_name
        )
