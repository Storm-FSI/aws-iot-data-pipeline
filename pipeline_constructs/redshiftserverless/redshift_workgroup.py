import aws_cdk.aws_redshiftserverless as redshiftserverless
from constructs import Construct


class RedshiftWorkgroup(Construct):

    def __init__(
            self,
            scope: Construct,
            construct_id: str,
            workgroup_name: str,
            namespace_name: str,
            base_capacity: int,
            security_group_ids: [str],
            subnet_ids: [str]
    ):
        super().__init__(scope, construct_id)

        self.workgroup = redshiftserverless.CfnWorkgroup(
            self,
            "Workgroup",
            workgroup_name=workgroup_name,
            namespace_name=namespace_name,
            base_capacity=base_capacity,
            config_parameters=[
                redshiftserverless.CfnWorkgroup.ConfigParameterProperty(
                    parameter_key="auto_mv",
                    parameter_value="true"
                ),
                redshiftserverless.CfnWorkgroup.ConfigParameterProperty(
                    parameter_key="datestyle",
                    parameter_value="ISO, MDY"
                ),
                redshiftserverless.CfnWorkgroup.ConfigParameterProperty(
                    parameter_key="enable_case_sensitive_identifier",
                    parameter_value="false"
                ),
                redshiftserverless.CfnWorkgroup.ConfigParameterProperty(
                    parameter_key="enable_user_activity_logging",
                    parameter_value="true"
                ),
                redshiftserverless.CfnWorkgroup.ConfigParameterProperty(
                    parameter_key="query_group",
                    parameter_value="default"
                ),
                redshiftserverless.CfnWorkgroup.ConfigParameterProperty(
                    parameter_key="search_path",
                    parameter_value="$user, public"
                ),
                redshiftserverless.CfnWorkgroup.ConfigParameterProperty(
                    parameter_key="max_query_execution_time",
                    parameter_value="14400"
                )
            ],
            enhanced_vpc_routing=False,
            publicly_accessible=False,
            security_group_ids=security_group_ids,
            subnet_ids=subnet_ids
        )
