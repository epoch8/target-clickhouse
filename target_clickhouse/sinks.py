"""Clickhouse target sink class, which handles writing streams."""

from __future__ import annotations

from singer_sdk.sinks import RecordSink

import clickhouse_connect
import datetime
import json

from target_clickhouse.catalog import COLUMNS_MAPPING
from target_clickhouse.catalog import DDL__TRUNCATE_TABLE
from target_clickhouse.catalog import DDL__CREATE_TABLE
from target_clickhouse.catalog import DML__OPTIMIZE_TABLE


class ClickhouseSink(RecordSink):
    """Clickhouse target sink class."""
    
    def __init__(self, target, stream_name, schema, key_properties):
        super().__init__(target, stream_name, schema, key_properties)

        self.client = self.get_client()
        self.target_schema = self.config.get("target_schema")
        self.upload_at = self.config.get("upload_at") == "true"

        self.table_config = {}
        self.get_table_config()

        if self.config.get("replication_method") == "truncate":
            self.truncate_table()

        self.create_table()
    

    def get_client(self) -> clickhouse_connect.driver.client:
        client_kwargs = {}

        client_kwargs["host"] = self.config.get("host")
        client_kwargs["port"] = self.config.get("port")
        client_kwargs["username"] = self.config.get("username")
        client_kwargs["password"] = self.config.get("password")

        if self.config.get("secure"):
            client_kwargs["secure"] = self.config.get("secure") == "true"

        if self.config.get("ca_cert"):
            client_kwargs["ca_cert"] = self.config.get("ca_cert")

        if self.config.get("send_receive_timeout"):
            client_kwargs["send_receive_timeout"] = self.config.get("send_receive_timeout")

        return clickhouse_connect.get_client(**client_kwargs)
    

    def get_table_config(self) -> None:
        if table_config_file := self.config.get("table_config"):
            with open(table_config_file, "r") as file:
                self.table_config = json.load(file).get("streams", {}).get(self.stream_name)
    

    def truncate_table(self) -> None:
        ddl__truncate_table = DDL__TRUNCATE_TABLE.format(
            target_schema=self.target_schema,
            table_name=self.stream_name,
        )

        self.client.command(ddl__truncate_table)
    

    def create_table(self) -> None:
        columns = []

        for key, value in self.schema["properties"].items():
            column = "`{column_name}` {column_mode}{column_type}"

            column_mode = (
                self.table_config.get("force_fields", {}).get(key, {}).get("mode")
                if self.table_config.get("force_fields", {}).get(key, {}).get("mode")
                else ""
            )

            column_type = (
                self.table_config.get("force_fields", {}).get(key, {}).get("type")
                if self.table_config.get("force_fields", {}).get(key, {}).get("type")
                else COLUMNS_MAPPING[value["type"]]
            )

            if column_mode:
                column_type = f"({column_type})"

            columns.append(
                column.format(
                    column_name=key,
                    column_mode=column_mode,
                    column_type=column_type,
                )
            )

        if self.upload_at:
            columns.append("`upload_at` DateTime64(0, 'UTC')")

        columns = ",\n\t".join(columns)

        engine = self.table_config.get("engine") or "MergeTree()"

        partition_by = (
            f"PARTITION BY {self.table_config.get('partition_type')}(`{self.table_config.get('partition_by')}`)"
            if self.table_config.get("partition_type") and self.table_config.get("partition_by")
            else ""
        )
        
        order_by = (
            ", ".join([f"`{order_by_column}`" for order_by_column in self.table_config.get("order_by")])
            if self.table_config.get("order_by")
            else ", ".join([f"`{key_property}`" for key_property in self._key_properties])
        )

        settings = (
            f"\nSETTINGS {self.table_config.get('settings')}"
            if self.table_config.get("settings")
            else ""
        )

        ddl__create_table = DDL__CREATE_TABLE.format(
            target_schema=self.target_schema,
            table_name=self.stream_name,
            columns=columns,
            engine=engine,
            partition_by=partition_by,
            order_by=order_by,
            settings=settings,
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
        column_names=[*record.keys()]
    
        self.client.insert(
            table=self.stream_name,
            data=data,
            column_names=column_names,
            database=self.target_schema,
        )
    

    def optimize_table(self) -> None:
        order_by = (
            [f"{order_by_column}" for order_by_column in self.table_config.get("order_by")]
            if self.table_config.get("order_by")
            else [f"{key_property}" for key_property in self._key_properties]
        )

        partition_by = [self.table_config.get("partition_by")] if self.table_config.get("partition_by") else []
        deduplicate = self.table_config.get("deduplicate") if self.table_config.get("deduplicate") else []

        deduplicate = set(order_by + partition_by + deduplicate)
        deduplicate = ", ".join([f"`{column}`" for column in deduplicate])

        dml__optimize_table = DML__OPTIMIZE_TABLE.format(
            target_schema=self.target_schema,
            table_name=self.stream_name,
            deduplicate=deduplicate,
        )

        self.client.command(dml__optimize_table)
    

    def clean_up(self) -> None:
        """Perform any clean up actions required at end of a stream.

        Implementations should ensure that clean up does not affect resources
        that may be in use from other instances of the same sink. Stream name alone
        should not be relied on, it's recommended to use a uuid as well.
        """
        self.logger.debug("Cleaning up %s", self.stream_name)
        self.record_counter_metric.exit()

        if self.config.get("optimize") == "true":
            self.optimize_table()
