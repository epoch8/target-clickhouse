COLUMNS_MAPPING = {
    "integer": "Int32",
    "string": "String",
    "boolean": "Bool",
    "datetime": "DateTime64(0, 'UTC')",
}


DDL__CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS `{target_schema}`.`{table_name}`
(
    {columns}
)
ENGINE = MergeTree()
{partition_by}
ORDER BY ({order_by});
"""
