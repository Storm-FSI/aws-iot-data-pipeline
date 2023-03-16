import aws_cdk.aws_ec2 as ec2
from aws_cdk import Stack
from constructs import Construct


class Vpc(Construct):

    def __init__(
            self,
            scope: Construct,
            construct_id: str,
            vpc_name: str
    ):
        super().__init__(scope, construct_id)

        self.vpc = ec2.Vpc(
            self,
            "Vpc",
            vpc_name=vpc_name,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="private_isolated",
                    subnet_type=ec2.SubnetType.PRIVATE_ISOLATED
                )
            ],
            nat_gateways=0,
            gateway_endpoints={
                "S3": ec2.GatewayVpcEndpointOptions(
                    service=ec2.GatewayVpcEndpointAwsService.S3
                )
            }
        )

        self.security_group = ec2.SecurityGroup(
            self,
            "SecurityGroup",
            vpc=self.vpc,
            allow_all_outbound=True
        )

        self.security_group.connections.allow_internally(
            port_range=ec2.Port.all_tcp()
        )

        self.security_group.node.add_dependency(self.vpc)

        iep_kinesis_streams = ec2.CfnVPCEndpoint(
            self,
            "InterfaceEndpointKinesisStreams",
            service_name="com.amazonaws.{}.kinesis-streams".format(Stack.of(self).region),
            vpc_endpoint_type="Interface",
            vpc_id=self.vpc.vpc_id,
            private_dns_enabled=True,
            subnet_ids=self.vpc.select_subnets(subnet_type=ec2.SubnetType.PRIVATE_ISOLATED).subnet_ids,
            security_group_ids=[
                self.security_group.security_group_id
            ]
        )

        iep_kinesis_streams.node.add_dependency(self.vpc)
        iep_kinesis_streams.node.add_dependency(self.security_group)
