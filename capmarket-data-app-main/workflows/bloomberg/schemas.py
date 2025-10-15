from typing import List

from pyspark.sql.types import *

SOURCE_NAME_TRADES = "trades"
SOURCE_NAME_QUOTES = "quotes"


def action_request():
    """
    Corporate action schema. Usually returned as bloomberg file, this schema contains nested objects and collection.
    Parsing this bloomberg file without schema results in terrible output (since each record theoretically contains
    different corporate events - those needs to be parsed as map and collection)
    :return: a defined Schema for corporate actions
    """
    return StructType([
        StructField("IDENTIFIER", StringType(), nullable=True),
        StructField("ID_BB_COMPANY", LongType(), nullable=True),
        StructField("ID_BB_SECURITY", IntegerType(), nullable=True),
        StructField("RC", IntegerType(), nullable=True),
        StructField("ACTION_ID", LongType(), nullable=True),
        StructField("CP_ACTION_MNEMONIC", StringType(), nullable=True),
        StructField("CP_ACTION_FLAG", StringType(), nullable=True),
        StructField("ID_BB_GLOBAL_COMPANY_NAME", StringType(), nullable=True),
        StructField("CA_SECURITY_IDENTIFIER_TYPE", StringType(), nullable=True),
        StructField("CA_SECURITY_IDENTIFIER", StringType(), nullable=True),
        StructField("CURRENCY", StringType(), nullable=True),
        StructField("MARKET_SECTOR_DES", StringType(), nullable=True),
        StructField("ID_BB_UNIQUE", StringType(), nullable=True),
        StructField("CP_ANN_DATE", DateType(), nullable=True, metadata={'format': 'dd/MM/yyyy'}),
        StructField("CP_EFFECTIVE_DATE", DateType(), nullable=True, metadata={'format': 'dd/MM/yyyy'}),
        StructField("CP_AMENDMENT_DATE", DateType(), nullable=True, metadata={'format': 'dd/MM/yyyy'}),
        StructField("ID_BB_GLOBAL", StringType(), nullable=True),
        StructField("ID_BB_GLOBAL_COMPANY", StringType(), nullable=True),
        StructField("ID_BB_SEC_NUM_DES", StringType(), nullable=True),
        StructField("FEED_SOURCE", StringType(), nullable=True),
        StructField("NUM_FIELDS", StringType(), nullable=True),
        StructField("ACTION_FIELDS", MapType(StringType(), StringType()), nullable=True)
    ])


def tick_quote_request():
    """
    Often available through parquet, this structure may be relevant for CSV files. Although CSV could be read as-is
    using Spark, it is more than best practice to define record types (such as long or decimal) rather than inferring
    :return: a defined Schema for quotes data
    """
    return StructType([
        StructField("SECURITY", StringType(), nullable=True),
        StructField("TICK_SEQUENCE_NUMBER", LongType(), nullable=True),
        StructField("TICK_TYPE", StringType(), nullable=True),
        StructField("EVT_QUOTE_BID_TIME", TimestampType(), nullable=True,
                    metadata={'format': "yyyy-MM-dd'T'HH:mm:ss.SSSZ"}),
        StructField("EVT_QUOTE_BID_PRICE", DecimalType(38, 6), nullable=True),
        StructField("EVT_QUOTE_BID_SIZE", LongType(), nullable=True),
        StructField("EVT_QUOTE_BID_CONDITION_CODE", StringType(), nullable=True),
        StructField("EVT_QUOTE_BID_LOCAL_EXCH_SRC", StringType(), nullable=True),
        StructField("UPFRONT_QUOTED_BID_PRICE", DecimalType(38, 6), nullable=True),
        StructField("EVT_QUOTE_ASK_TIME", TimestampType(), nullable=True,
                    metadata={'format': "yyyy-MM-dd'T'HH:mm:ss.SSSZ"}),
        StructField("EVT_QUOTE_ASK_PRICE", DecimalType(38, 6), nullable=True),
        StructField("EVT_QUOTE_ASK_SIZE", LongType(), nullable=True),
        StructField("EVT_QUOTE_ASK_CONDITION_CODE", StringType(), nullable=True),
        StructField("EVT_QUOTE_ASK_LOCAL_EXCH_SRC", StringType(), nullable=True),
        StructField("UPFRONT_QUOTED_ASK_PRICE", DecimalType(38, 6), nullable=True)
    ])


def tick_trade_request():
    """
    Often available through parquet, this structure may be relevant for CSV files. Although CSV could be read as-is
    using Spark, it is more than best practice to define record types (such as long or decimal) rather than inferring
    :return: a defined Schema for trades data
    """
    return StructType([
        StructField("SECURITY", StringType(), nullable=True),
        StructField("TICK_SEQUENCE_NUMBER", LongType(), nullable=True),
        StructField("TICK_TYPE", StringType(), nullable=True),
        StructField("EVT_TRADE_TIME", TimestampType(), nullable=True,
                    metadata={'format': "yyyy-MM-dd'T'HH:mm:ss.SSSZ"}),
        StructField("TRADE_REPORTED_TIME", TimestampType(), nullable=True,
                    metadata={'format': "yyyy-MM-dd'T'HH:mm:ss.SSSZ"}),
        StructField("EVT_TRADE_EXECUTION_TIME", TimestampType(), nullable=True,
                    metadata={'format': "yyyy-MM-dd'T'HH:mm:ss.SSSZ"}),
        StructField("EVT_TRADE_IDENTIFIER", StringType(), nullable=True),
        StructField("EVENT_ORIGINAL_TRADE_ID", LongType(), nullable=True),
        StructField("EVENT_ORIGINAL_TRADE_TIME", TimestampType(), nullable=True,
                    metadata={'format': "yyyy-MM-dd'T'HH:mm:ss.SSSZ"}),
        StructField("EVT_TRADE_PRICE", DecimalType(38, 6), nullable=True),
        StructField("EVT_TRADE_SIZE", LongType(), nullable=True),
        StructField("EVT_TRADE_LOCAL_EXCH_SOURCE", StringType(), nullable=True),
        StructField("EVT_TRADE_CONDITION_CODE", StringType(), nullable=True),
        StructField("EVT_TRADE_BUY_BROKER", StringType(), nullable=True),
        StructField("EVT_TRADE_SELL_BROKER", StringType(), nullable=True),
        StructField("TRACE_RPT_PARTY_SIDE_LAST_TRADE", StringType(), nullable=True),
        StructField("EVT_TRADE_RPT_PARTY_TYP", StringType(), nullable=True),
        StructField("EVT_TRADE_BIC", StringType(), nullable=True),
        StructField("EVT_TRADE_MIC", StringType(), nullable=True),
        StructField("EVT_TRADE_ESMA_TRADE_FLAGS", StringType(), nullable=True),
        StructField("EVT_TRADE_AGGRESSOR", StringType(), nullable=True),
        StructField("EVT_TRADE_RPT_CONTRA_TYP", StringType(), nullable=True),
        StructField("EVT_TRADE_REMUNERATION", StringType(), nullable=True),
        StructField("EVT_TRADE_ATS_INDICATOR", StringType(), nullable=True)
    ])


def data_request(additional_fields: List[str]):
    """
    Standard structure for data requests. Response are mostly driven by additional fields, but contains predefined
    columns mapped here. Additional fields can be provided, but will be mapped as string object for now. Ideally,
    those will be fetched from bloomberg API and mapped to their corresponding types.
    :param additional_fields: the fields requested by end user
    :return: a predefined schema for a data request
    """
    fields = [
        StructField("DL_REQUEST_ID", StringType(), nullable=True),
        StructField("DL_REQUEST_NAME", StringType(), nullable=True),
        StructField("DL_SNAPSHOT_START_TIME", TimestampType(), nullable=True,
                    metadata={'format': "yyyy-MM-dd'T'HH:mm:ss"}),
        StructField("DL_SNAPSHOT_TZ", StringType(), nullable=True),
        StructField("IDENTIFIER", StringType(), nullable=True),
        StructField("RC", IntegerType(), nullable=True)
    ]
    fields.extend(
        [StructField(additional_field, StringType(), nullable=True) for additional_field in additional_fields])
    return StructType(fields)


def history_request(additional_fields: List[str]):
    """
    Standard structure for historical requests. Response are mostly driven by additional fields, but contains predefined
    columns mapped here. Additional fields can be provided, but will be mapped as string object for now. Ideally,
    those will be fetched from bloomberg API and mapped to their corresponding types.
    :param additional_fields: the fields requested by end user
    :return: a predefined schema for a data request
    """
    fields = [
        StructField("_c0", StringType(), nullable=True),
        StructField("_c1", IntegerType(), nullable=True),
        StructField("_c2", IntegerType(), nullable=True),
        StructField("_c3", DateType(), nullable=True, metadata={'format': "dd/MM/yyyy"})
    ]
    fields.extend(
        [StructField(additional_field, StringType(), nullable=True) for additional_field in additional_fields])
    return StructType(fields)
