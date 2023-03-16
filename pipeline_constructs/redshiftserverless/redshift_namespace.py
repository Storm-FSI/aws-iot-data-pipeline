import aws_cdk.aws_iam as iam
import aws_cdk.aws_redshiftserverless as redshiftserverless
from constructs import Construct


class RedshiftNamespace(Construct):

    def __init__(
            self,
            scope: Construct,
            construct_id: str,
            namespace_name: str,
            admin_username: str,
            admin_password: str,
    ):
        super().__init__(scope, construct_id)

        iam_role_full_access = iam.Role(
            self,
            "IamRoleFullAccess",
            role_name="{}-full-access".format(namespace_name),
            assumed_by=iam.ServicePrincipal("redshift-serverless.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonRedshiftAllCommandsFullAccess")
            ]
        )

        self.namespace = redshiftserverless.CfnNamespace(
            self,
            "Namespace",
            namespace_name=namespace_name,
            admin_username=admin_username,
            admin_user_password=admin_password,
            iam_roles=[
                iam_role_full_access.role_arn
            ]
        )

        self.namespace.node.add_dependency(iam_role_full_access)
