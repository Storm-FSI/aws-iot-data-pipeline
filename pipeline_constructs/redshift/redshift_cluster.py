import aws_cdk.aws_iam as iam
import aws_cdk.aws_redshift as redshift
from constructs import Construct


class RedshiftCluster(Construct):

    def __init__(
            self,
            scope: Construct,
            construct_id: str,
            cluster_identifier: str,
            db_name: str,
            master_username: str,
            master_password: str,
            node_type: str,
            number_of_nodes: int,
            security_group_ids: [str],
            subnet_ids: [str]
    ):
        super().__init__(scope, construct_id)

        # Cluster IAM role
        iam_role_full_access = iam.Role(
            self,
            "IamRoleFullAccess",
            role_name="{}-full-access".format(cluster_identifier),
            assumed_by=iam.ServicePrincipal("redshift.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonRedshiftAllCommandsFullAccess")
            ]
        )

        # Cluster Subnet Group
        cluster_subnet_group = redshift.CfnClusterSubnetGroup(
            self,
            "RedshiftClusterSubnetGroup",
            description="Subnet group for {}".format(cluster_identifier),
            subnet_ids=subnet_ids
        )

        # Cluster
        cluster_type = "single-node" if number_of_nodes == 1 else "multi-node"

        self.cluster = redshift.CfnCluster(
            self,
            "RedshiftCluster",
            cluster_identifier=cluster_identifier,
            cluster_type=cluster_type,
            db_name=db_name,
            master_username=master_username,
            master_user_password=master_password,
            node_type=node_type,
            number_of_nodes=number_of_nodes,
            port=5439,
            vpc_security_group_ids=security_group_ids,
            cluster_subnet_group_name=cluster_subnet_group.attr_cluster_subnet_group_name,
            iam_roles=[
                iam_role_full_access.role_arn
            ],
            allow_version_upgrade=True,
            publicly_accessible=False,
            encrypted=False,
            enhanced_vpc_routing=False
        )

        self.cluster.node.add_dependency(iam_role_full_access)
        self.cluster.node.add_dependency(cluster_subnet_group)
