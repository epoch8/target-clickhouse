COLUMNS_MAPPING = {
    "integer": "Int32",
    "string": "String",
    "boolean": "Bool",
    "datetime": "DateTime64(0, 'UTC')",
}


DDL__TRUNCATE_TABLE = """
TRUNCATE TABLE IF EXISTS `{target_schema}`.`{table_name}`;
"""


DDL__CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS `{target_schema}`.`{table_name}`
(
    {columns}
)
ENGINE = {engine}
{partition_by}
ORDER BY ({order_by}){settings};
"""


DML__OPTIMIZE_TABLE = """
OPTIMIZE TABLE `{target_schema}`.`{table_name}` FINAL DEDUPLICATE BY {deduplicate};
"""
