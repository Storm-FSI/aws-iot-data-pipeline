import aws_cdk.aws_glue as glue
from aws_cdk import Stack
from constructs import Construct


class OutputDatabase(Construct):

    def __init__(
            self,
            scope: Construct,
            construct_id: str,
            db_name: str,
            table_name: str,
            bucket_name: str
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
            "GlueOutputTable",
            catalog_id=Stack.of(self).account,
            database_name=self.database.database_input.name,
            table_input=glue.CfnTable.TableInputProperty(
                name=table_name,
                description="The table containing the output data from the S3 bucket {}".format(bucket_name),
                retention=0,
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    columns=[
                        {
                            "name": "msg_mid",
                            "type": "int"
                        },
                        {
                            "name": "msg_id",
                            "type": "string"
                        },
                        {
                            "name": "msg_timestamp",
                            "type": "string"
                        },
                        {
                            "name": "msg_sender",
                            "type": "string"
                        },
                        {
                            "name": "msg_topic",
                            "type": "string"
                        },
                        {
                            "name": "msg_destinantion",
                            "type": "string"
                        },
                        {
                            "name": "msg_creationtimestamp",
                            "type": "string"
                        },
                        {
                            "name": "msg_starttimestamp",
                            "type": "string"
                        },
                        {
                            "name": "msg_endtimestamp",
                            "type": "string"
                        },
                        {
                            "name": "msg_version",
                            "type": "string"
                        },
                        {
                            "name": "che_id",
                            "type": "int"
                        },
                        {
                            "name": "che_name",
                            "type": "string"
                        },
                        {
                            "name": "che_number",
                            "type": "int"
                        },
                        {
                            "name": "che_type",
                            "type": "string"
                        },
                        {
                            "name": "che_family",
                            "type": "string"
                        },
                        {
                            "name": "che_brand",
                            "type": "string"
                        },
                        {
                            "name": "che_model",
                            "type": "string"
                        },
                        {
                            "name": "che_on_status_timestamp",
                            "type": "string"
                        },
                        {
                            "name": "che_on_status_value",
                            "type": "string"
                        },
                        {
                            "name": "che_control_id",
                            "type": "int"
                        },
                        {
                            "name": "che_control_modespreader_status_timestamp",
                            "type": "string"
                        },
                        {
                            "name": "che_control_modespreader_status_value",
                            "type": "string"
                        },
                        {
                            "name": "che_spreader_id",
                            "type": "int"
                        },
                        {
                            "name": "che_spreader_locked_status_timestamp",
                            "type": "string"
                        },
                        {
                            "name": "che_spreader_locked_status_value",
                            "type": "string"
                        },
                        {
                            "name": "che_spreader_unlocked_status_timestamp",
                            "type": "string"
                        },
                        {
                            "name": "che_spreader_unlocked_status_value",
                            "type": "string"
                        },
                        {
                            "name": "che_hoist_id",
                            "type": "int"
                        },
                        {
                            "name": "che_hoist_hoisting_height_timestamp",
                            "type": "string"
                        },
                        {
                            "name": "che_hoist_hoisting_height_value",
                            "type": "double"
                        },
                        {
                            "name": "che_hoist_weight_gross_value",
                            "type": "double"
                        },
                        {
                            "name": "che_trolley_id",
                            "type": "int"
                        },
                        {
                            "name": "che_trolley_trolleying_reach_timestamp",
                            "type": "string"
                        },
                        {
                            "name": "che_trolley_trolleying_reach_value",
                            "type": "double"
                        },
                        {
                            "name": "che_trolley_trolleying_reach_reference",
                            "type": "string"
                        }

                    ],
                    location="s3://{}/".format(bucket_name),
                    input_format='org.apache.hadoop.mapred.TextInputFormat',
                    output_format='org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                    compressed=False,
                    number_of_buckets=-1,
                    serde_info=glue.CfnTable.SerdeInfoProperty(
                        serialization_library='org.openx.data.jsonserde.JsonSerDe',
                        parameters={
                            "serialization.format": "1",
                            "paths": "che_brand,che_control_id,che_control_modespreader_status_timestamp,che_control_modespreader_status_value,che_family,che_hoist_hoisting_height_timestamp,che_hoist_hoisting_height_value,che_hoist_id,che_hoist_weight_gross_value,che_id,che_model,che_name,che_number,che_on_status_timestamp,che_on_status_value,che_spreader_id,che_spreader_locked_status_timestamp,che_spreader_locked_status_value,che_spreader_unlocked_status_timestamp,che_spreader_unlocked_status_value,che_trolley_id,che_trolley_trolleying_reach_reference,che_trolley_trolleying_reach_timestamp,che_trolley_trolleying_reach_value,che_type,msg_creationtimestamp,msg_destinantion,msg_endtimestamp,msg_id,msg_mid,msg_sender,msg_starttimestamp,msg_timestamp,msg_topic,msg_version"
                        }
                    ),
                    sort_columns=None,
                    skewed_info=None,
                    stored_as_sub_directories=False
                ),
                partition_keys=None,
                parameters={
                    "sizeKey": "30059192",
                    "objectCount": "118",
                    "UPDATED_BY_CRAWLER": "euro-twinsim-datastore-crawler",
                    "CrawlerSchemaSerializerVersion": "1.0",
                    "recordCount": "28763",
                    "transient_lastDdlTime": "1676995269",
                    "averageRecordSize": "1040",
                    "exclusions": "[\"s3://{bucket}/**.csv\",\"s3://{bucket}/**.txt\",\"s3://{bucket}/**.csv.metadata\",\"s3://{bucket}/**/Unsaved/**\"]".format(
                        bucket=bucket_name),
                    "CrawlerSchemaDeserializerVersion": "1.0",
                    "compressionType": "none",
                    "classification": "json",
                    "EXTERNAL": "TRUE",
                    "typeOfData": "file"
                }
            )
        )

        self.table.node.add_dependency(self.database)
