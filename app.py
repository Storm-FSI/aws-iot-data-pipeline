from aws_cdk import App
from aws_cdk import Environment

from pipeline_stack.pipeline_stack import PipelineStack

app = App()

ACCOUNT = app.node.try_get_context("account")
REGION = app.node.try_get_context("region")

ORGANIZATION = app.node.try_get_context("organization")
ENVIRONMENT = app.node.try_get_context("environment")
APPLICATION = app.node.try_get_context("application")

pipeline_stack = PipelineStack(
    app,
    "{}-{}-{}".format(ORGANIZATION, ENVIRONMENT, APPLICATION),
    organization=ORGANIZATION,
    environment=ENVIRONMENT,
    application=APPLICATION,
    # env=cdk.Environment(account=os.getenv('CDK_DEFAULT_ACCOUNT'), region=os.getenv('CDK_DEFAULT_REGION')),
    env=Environment(account=ACCOUNT, region=REGION)
)

app.synth()
