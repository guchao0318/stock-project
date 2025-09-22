# Elasticsearch索引映射配置
# 股票基础数据索引字段定义

STOCK_INDEX_NAME = "stock_basic_data"

# 股票基础数据索引映射
STOCK_MAPPING = {
    "mappings": {
        "properties": {
            "stock_code": {
                "type": "keyword",
                "doc_values": True
            },
            "stock_name": {
                "type": "text",
                "analyzer": "ik_max_word",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "market_type": {
                "type": "keyword",
                "doc_values": True
            },
            # "current_price": {
            #     "type": "double"
            # },
            # "change_percent": {
            #     "type": "double"
            # },
            # "change_amount": {
            #     "type": "double"
            # },
            # "volume": {
            #     "type": "long"
            # },
            # "turnover": {
            #     "type": "double"
            # },
            # "amplitude": {
            #     "type": "double"
            # },
            # "highest_price": {
            #     "type": "double"
            # },
            # "lowest_price": {
            #     "type": "double"
            # },
            # "opening_price": {
            #     "type": "double"
            # },
            # "previous_close": {
            #     "type": "double"
            # },
            # "volume_ratio": {
            #     "type": "double"
            # },
            # "pe_ratio": {
            #     "type": "double"
            # },
            # "pb_ratio": {
            #     "type": "double"
            # },
            # "total_market_value": {
            #     "type": "double"
            # },
            # "circulating_market_value": {
            #     "type": "double"
            # },
            "update_time": {
                "type": "date",
                "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
            },
            "data_source": {
                "type": "keyword"
            }
        }
    },
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "refresh_interval": "30s"
    }
}

# 九转策略索引配置
NINE_TURN_STRATEGY_INDEX = {
    "index_name": "nine_turn_strategy",
    "alias_name": "nine_turn_strategy_alias",
    "mapping": {
        "mappings": {
            "properties": {
                "date": {
                    "type": "date",
                    "format": "yyyy-MM-dd"
                },
                "stock_code": {
                    "type": "keyword"
                },
                "market_type": {
                    "type": "keyword"
                },
                "created_at": {
                    "type": "date"
                }
            }
        },
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "refresh_interval": "1s"
        }
    }
}

# 索引别名配置
INDEX_ALIASES = {
    "stock_basic_alias": "stock_basic_data",
    "stock_daily_alias": "stock_daily_data",
    "nine_turn_strategy_alias": "nine_turn_strategy"
}

STOCK_ALIAS = "stock_data"

# 日线数据索引配置
DAILY_INDEX_NAME = "stock_daily_data"
DAILY_ALIAS = "stock_daily"

# 日线数据索引映射
DAILY_MAPPING = {
    "mappings": {
        "properties": {
            "date": {
                "type": "date",
                "format": "yyyy-MM-dd"
            },
            "stock_code": {
                "type": "keyword"
            },
            "market_type": {
                "type": "keyword"
            },
            "open_price": {
                "type": "integer",
                "doc_values": True
            },
            "close_price": {
                "type": "integer",
                "doc_values": True
            },
            "high_price": {
                "type": "integer",
                "doc_values": True
            },
            "low_price": {
                "type": "integer",
                "doc_values": True
            },
            "prev_close_price": {
                "type": "integer",
                "doc_values": True
            },
            "volume": {
                "type": "long",
                "doc_values": True
            },
            "amount": {
                "type": "long",
                "doc_values": True
            },
            "change_percent": {
                "type": "integer",
                "doc_values": True
            },
            "change_amount": {
                "type": "integer",
                "doc_values": True
            },
            "turnover_rate": {
                "type": "integer",
                "doc_values": True
            },
            "update_time": {
                "type": "date",
                "format": "yyyy-MM-dd HH:mm:ss"
            },
            "data_source": {
                "type": "keyword"
            }
        }
    },
    "settings": {
        "number_of_shards": 3,
        "number_of_replicas": 1,
        "refresh_interval": "30s"
    },
    "aliases": {
        DAILY_ALIAS: {}
    }
}