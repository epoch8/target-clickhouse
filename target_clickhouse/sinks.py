"""Clickhouse target sink class, which handles writing streams."""

from __future__ import annotations

from singer_sdk.sinks import RecordSink
from singer_sdk.target_base import Target

import clickhouse_connect
import datetime

from target_clickhouse.catalog import COLUMNS_MAPPING
from target_clickhouse.catalog import DDL__CREATE_TABLE


class ClickhouseSink(RecordSink):
    """Clickhouse target sink class."""
    
    def __init__(self, target, stream_name, schema, key_properties):
        super().__init__(target, stream_name, schema, key_properties)

        self.client = self.get_client(target)
        self.target_schema = target.config.get("target_schema")
        self.upload_at = target.config.get("upload_at") == "true"
        self.create_table()

    def get_client(self, target: Target) -> clickhouse_connect.driver.client:
        client_kwargs = {}

        client_kwargs["host"] = target.config.get("host")
        client_kwargs["port"] = target.config.get("port")
        client_kwargs["username"] = target.config.get("username")
        client_kwargs["password"] = target.config.get("password")

        if target.config.get("secure"):
            client_kwargs["secure"] = target.config.get("secure") == "true"

        if target.config.get("ca_cert"):
            client_kwargs["ca_cert"] = target.config.get("ca_cert")

        if target.config.get("send_receive_timeout"):
            client_kwargs["send_receive_timeout"] = target.config.get("send_receive_timeout")

        return clickhouse_connect.get_client(**client_kwargs)
    
    def create_table(self):
        columns = ",\n\t".join(
            [
                f"`{key}` Nullable({COLUMNS_MAPPING[value['type']]})"
                for key, value
                in self.schema["properties"].items()
            ]
        )

        partition_by = ""

        if self.upload_at:
            columns = columns + ",\n\t`upload_at` DateTime64(0, 'UTC')"
            partition_by = "PARTITION BY toDate(upload_at)"

        ddl__create_table = DDL__CREATE_TABLE.format(
            target_schema=self.target_schema,
            table_name=self.stream_name,
            columns=columns,
            partition_by=partition_by,
            order_by=",".join([f"`{key_property}`" for key_property in self._key_properties]),
        )

        self.client.command(ddl__create_table)

    def process_record(self, record: dict, context: dict) -> None:
        """Process the record.

        Args:
            record: Individual record in the stream.
            context: Stream partition or context dictionary.
        """
        if self.upload_at:
            record["upload_at"] = f"{datetime.datetime.now()}"

        data = [[*record.values()]]
    
        self.client.insert(
            table=self.stream_name,
            data=data,
            column_names=[*record.keys()],
            database=self.target_schema,
        )
