from enum import Enum


class Constants(Enum):
    """
    General constants for configuration values and data mappings.
    """
    RECORD_COLUMNS = "columns"
    NAME = "name"
    MAPPING = "mapping"
    SQL_TYPE = "sql_type"
    BI_PREFIX = "bi_"
    DECIMAL_TYPE = "DECIMAL"
    VARCHAR_TYPE = "VARCHAR(10)"
    VARCHAR_TYPE_256 = "VARCHAR(256)"
    DATA_PREFIX = "$.data."
    BEFORE_IMAGE_PREFIX = "$.before-image."

class HudiConstants(Enum):
    """
    Hudi-specific constants for configuration and Hive sync.
    """
    HUDI_FORMAT = "org.apache.hudi"
    TABLE_NAME = "hoodie.table.name"
    RECORDKEY_FIELD_OPT_KEY = "hoodie.datasource.write.recordkey.field"
    PRECOMBINE_FIELD_OPT_KEY = "hoodie.datasource.write.precombine.field"
    OPERATION_OPT_KEY = "hoodie.datasource.write.operation"
    DELETE_OPERATION_OPT_VAL = "delete"
    UPSERT_PARALLELISM = "hoodie.upsert.shuffle.parallelism"
    HIVE_SYNC_ENABLED_OPT_KEY = "hoodie.datasource.hive_sync.enable"
    HIVE_TABLE_OPT_KEY = "hoodie.datasource.hive_sync.table"
    HIVE_DATABASE_OPT_KEY = "hoodie.datasource.hive_sync.database"
    PARTITIONPATH_FIELD_OPT_KEY = "hoodie.datasource.write.partitionpath.field"
    KEEP_LATEST_COMMITS = "KEEP_LATEST_COMMITS"
    bolHiveSync = "true"
    glue_db_prefix = "ENTERPRISEANALYTICS_EDH_"


class ConstantFields(Enum):
    """
    Field-specific constants that store metadata for each column in the database.
    """
    change_seq = {
        "name": "change_seq",
        "mapping": "$.data.CHANGESEQ",
        "sql_type": "VARCHAR(256)"
    }
    h_user = {
        "name": "h_user",
        "mapping": "$.data.h_user",
        "sql_type": "VARCHAR(256)"
    }
    COL_timestamp = {
        "name": "COL_timestamp",
        "mapping": "$.metadata.timestamp",
        "sql_type": "VARCHAR(256)"
    }
    COL_recordtype = {
        "name": "COL_recordtype",
        "mapping": "$.metadata.record-type",
        "sql_type": "VARCHAR(256)"
    }
    operation = {
        "name": "operation",
        "mapping": "$.metadata.operation",
        "sql_type": "VARCHAR(256)"
    }

    @classmethod
    def get_field(cls, field_name):
        """
        Get the field constant dictionary by name.
        """
        return cls[field_name].value if field_name in cls.__members__ else None
