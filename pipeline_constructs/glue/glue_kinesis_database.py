import aws_cdk.aws_glue as glue
from aws_cdk import Stack
from constructs import Construct


class KinesisDatabase(Construct):

    def __init__(
            self,
            scope: Construct,
            construct_id: str,
            db_name: str,
            table_name: str,
            data_stream_name: str,
            data_stream_arn: str
    ):
        super().__init__(scope, construct_id)

        self.database = glue.CfnDatabase(
            self,
            "Database",
            catalog_id=Stack.of(self).account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=db_name
            )
        )

        self.table = glue.CfnTable(
            self,
            "TableDataStream",
            catalog_id=Stack.of(self).account,
            database_name=self.database.database_input.name,
            table_input=glue.CfnTable.TableInputProperty(
                name=table_name,
                description="The table containing the input data from the Kinesis data stream {}".format(
                    data_stream_name),
                retention=0,
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    columns=None,
                    location=data_stream_name,
                    input_format='org.apache.hadoop.mapred.TextInputFormat',
                    output_format='org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                    compressed=False,
                    number_of_buckets=0,
                    serde_info=glue.CfnTable.SerdeInfoProperty(
                        serialization_library='org.openx.data.jsonserde.JsonSerDe',
                    ),
                    sort_columns=None,
                    parameters={
                        "streamARN": data_stream_arn,
                        "typeOfData": "kinesis"
                    },
                    stored_as_sub_directories=False
                ),
                partition_keys=None,
                parameters={
                    "classification": "json"
                }
            )
        )

        self.table.node.add_dependency(self.database)
