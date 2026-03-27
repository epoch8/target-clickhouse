"""Clickhouse target class."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.target_base import Target

from target_clickhouse.sinks import (
    ClickhouseSink,
)


class TargetClickhouse(Target):
    """Target for Clickhouse."""

    name = "target-clickhouse"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "host",
            th.StringType(nullable=False),
            secret=True,
            required=True,
            title="Database host",
            description="Database host",
        ),
        th.Property(
            "port",
            th.StringType(nullable=False),
            secret=True,
            required=True,
            title="Database connection port",
            description="Database connection port",
        ),
        th.Property(
            "username",
            th.StringType(nullable=False),
            secret=True,
            required=True,
            title="Database user",
            description="Database user",
        ),
        th.Property(
            "password",
            th.StringType(nullable=False),
            secret=True,
            required=True,
            title="Username password",
            description="Username password",
        ),
        th.Property(
            "target_schema",
            th.StringType(nullable=False),
            secret=False,
            required=False,
            title="The default target database schema",
            description="The default target database schema name to use for all streams.",
            default="default",
        ),
        th.Property(
            "secure",
            th.StringType(nullable=False),
            secret=False,
            required=False,
            title="Should the connection be secure",
            description="String values \"true\" or \"false\" are only allowed.",
            default="false",
        ),
        th.Property(
            "ca_cert",
            th.StringType(nullable=False),
            secret=False,
            required=False,
            title="",
            description="",
        ),
        th.Property(
            "send_receive_timeout",
            th.IntegerType,
            secret=False,
            required=False,
            title="",
            description="",
        ),
        th.Property(
            "upload_at",
            th.StringType(nullable=False),
            secret=False,
            required=False,
            title="Add column \"upload_at\"",
            description="String values \"true\" or \"false\" are only allowed.",
            default="false",
        ),
        th.Property(
            "table_config",
            th.StringType(nullable=False),
            secret=False,
            required=False,
            title="",
            description="",
        ),
        th.Property(
            "replication_method",
            th.StringType(nullable=False),
            secret=False,
            required=False,
            title="Replication method",
            description="String values \"append\" or \"truncate\" are only allowed.",
            default="false",
        ),
        th.Property(
            "optimize",
            th.StringType(nullable=False),
            secret=False,
            required=False,
            title="Optimize tables",
            description="Run 'OPTIMIZE TABLE' after data insert. String values \"true\" or \"false\" are only allowed.",
            default="false",
        ),
    ).to_dict()

    default_sink_class = ClickhouseSink


if __name__ == "__main__":
    TargetClickhouse.cli()
