# 系统配置文件
import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# Elasticsearch配置
ES_CONFIG = {
    'host': os.getenv('ES_HOST', 'localhost'),
    'port': int(os.getenv('ES_PORT', 9200)),
    'username': os.getenv('ES_USERNAME', ''),
    'password': os.getenv('ES_PASSWORD', ''),
    'use_ssl': os.getenv('ES_USE_SSL', 'false').lower() == 'true',
    'verify_certs': os.getenv('ES_VERIFY_CERTS', 'false').lower() == 'true',
    'timeout': int(os.getenv('ES_TIMEOUT', 30)),
    'scheme': 'http'
}

# 数据采集配置
DATA_COLLECTION_CONFIG = {
    "batch_size": 100,
    "retry_times": 3,
    "retry_delay": 5,
    "timeout": 30
}

# Kafka配置
KAFKA_CONFIG = {
    "BOOTSTRAP_SERVERS": ["localhost:9092"],
    "STOCK_TOPIC": "stock_realtime_data",
    "STRATEGY_TOPIC": "nine_turn_strategy",
    "ACKS": "all",
    "RETRIES": 3,
    "BATCH_SIZE": 16384,
    "LINGER_MS": 10,
    "BUFFER_MEMORY": 33554432,
    "MAX_REQUEST_SIZE": 1048576
}

# Flink配置
FLINK_CONFIG = {
    "JOB_MANAGER_HOST": "localhost",
    "JOB_MANAGER_PORT": 8081,
    "PARALLELISM": 1,
    "CHECKPOINT_INTERVAL": 60000,
    "RESTART_STRATEGY": "fixed-delay",
    "RESTART_ATTEMPTS": 3,
    "RESTART_DELAY": 10000
}

# 日志配置
LOG_CONFIG = {
    'level': os.getenv('LOG_LEVEL', 'INFO'),
    'file_path': os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs', 'stock_collector.log'),
    'max_size': '10 MB',
    'backup_count': 5,
    'format': '{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} | {message}'
}

# 股票市场配置
MARKET_CONFIG = {
    'sz_market': 'SZ',  # 深圳市场标识
    'sh_market': 'SH',  # 上海市场标识
}

# 数据字段映射
FIELD_MAPPING = {
    '代码': 'stock_code',
    '名称': 'stock_name', 
    '最新价': 'current_price',
    '涨跌幅': 'change_percent',
    '涨跌额': 'change_amount',
    '成交量': 'volume',
    '成交额': 'turnover',
    '振幅': 'amplitude',
    '最高': 'highest_price',
    '最低': 'lowest_price',
    '今开': 'opening_price',
    '昨收': 'previous_close',
    '量比': 'volume_ratio',
    '市盈率-动态': 'pe_ratio',
    '市净率': 'pb_ratio',
    '总市值': 'total_market_value',
    '流通市值': 'circulating_market_value'
}