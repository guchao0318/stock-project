# 股票数据采集系统

基于AKshare和Elasticsearch的股票数据自动采集系统，每天定时获取深圳和上海A股的基础数据并存储到Elasticsearch中。

## 功能特性

- 🕘 **定时采集**: 每天9:10分自动执行数据采集任务
- 📊 **数据源**: 使用AKshare获取深圳A股(`stock_sz_a_spot_em`)和上海A股(`stock_sh_a_spot_em`)数据
- 🗄️ **数据存储**: 将数据存储到Elasticsearch，支持数据覆盖更新
- 📝 **日志记录**: 完整的日志记录和错误处理机制
- 🔧 **灵活配置**: 支持环境变量配置和自定义索引字段
- 🚀 **多种运行模式**: 支持守护进程、手动执行、状态查询等模式

## 系统架构

```
stock-project/
├── src/
│   ├── data_collector/          # 数据采集模块
│   │   └── stock_collector.py   # 股票数据采集器
│   ├── data_storage/            # 数据存储模块
│   │   └── es_storage.py        # Elasticsearch存储器
│   ├── scheduler/               # 调度器模块
│   │   └── task_scheduler.py    # 定时任务调度器
│   └── utils/                   # 工具模块
│       └── logger_config.py     # 日志配置
├── config/                      # 配置文件
│   ├── settings.py              # 系统配置
│   └── es_mapping.py            # ES索引映射
├── logs/                        # 日志目录
├── main.py                      # 主程序入口
├── requirements.txt             # 依赖包列表
└── .env.example                 # 环境变量模板
```

## 数据字段

系统采集的股票数据包含以下字段：

| 字段名 | 类型 | 描述 |
|--------|------|------|
| stock_code | keyword | 股票代码 |
| stock_name | text | 股票名称 |
| market_type | keyword | 市场类型(SZ/SH) |
| current_price | double | 当前价格 |
| change_percent | double | 涨跌幅(%) |
| change_amount | double | 涨跌额 |
| volume | long | 成交量 |
| turnover | double | 成交额 |
| amplitude | double | 振幅 |
| highest_price | double | 最高价 |
| lowest_price | double | 最低价 |
| opening_price | double | 开盘价 |
| previous_close | double | 昨收价 |
| volume_ratio | double | 量比 |
| pe_ratio | double | 市盈率 |
| pb_ratio | double | 市净率 |
| total_market_value | double | 总市值 |
| circulating_market_value | double | 流通市值 |
| update_time | date | 更新时间 |
| data_source | keyword | 数据源 |

## 安装和配置

### 1. 环境要求

- Python 3.8+
- Elasticsearch 8.0+

### 2. 安装依赖

```bash
# 克隆项目
git clone <repository-url>
cd stock-project

# 创建虚拟环境
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# 或
.venv\Scripts\activate     # Windows

# 安装依赖
pip install -r requirements.txt
```

### 3. 配置环境变量

```bash
# 复制环境变量模板
cp .env.example .env

# 编辑配置文件
vim .env
```

环境变量配置示例：

```bash
# Elasticsearch配置
ES_HOST=localhost
ES_PORT=9200
ES_USERNAME=
ES_PASSWORD=
ES_USE_SSL=false
ES_VERIFY_CERTS=false
ES_TIMEOUT=30

# 日志配置
LOG_LEVEL=INFO

# 其他配置
TIMEZONE=Asia/Shanghai
```

### 4. 启动Elasticsearch

确保Elasticsearch服务正在运行：

```bash
# 使用Docker启动Elasticsearch
docker run -d \
  --name elasticsearch \
  -p 9200:9200 \
  -p 9300:9300 \
  -e "discovery.type=single-node" \
  -e "ES_JAVA_OPTS=-Xms512m -Xmx512m" \
  elasticsearch:8.11.0
```

## 使用方法

### 命令行选项

```bash
python main.py [选项]

选项:
  --run-once           立即执行一次数据采集
  --daemon             以守护进程模式运行
  --status             查看系统状态
  --schedule-time      设置调度时间 (格式: HH:MM)
  --help               显示帮助信息
```

### 使用示例

#### 1. 手动执行一次数据采集

```bash
python main.py --run-once
```

#### 2. 启动守护进程（推荐）

```bash
python main.py --daemon
```

#### 3. 查看系统状态

```bash
python main.py --status
```

#### 4. 修改调度时间

```bash
python main.py --schedule-time 09:30
```

### 后台运行

在生产环境中，建议使用进程管理工具：

#### 使用nohup

```bash
nohup python main.py --daemon > /dev/null 2>&1 &
```

#### 使用systemd (Linux)

创建服务文件 `/etc/systemd/system/stock-collector.service`：

```ini
[Unit]
Description=Stock Data Collector
After=network.target

[Service]
Type=simple
User=your-username
WorkingDirectory=/path/to/stock-project
Environment=PATH=/path/to/stock-project/.venv/bin
ExecStart=/path/to/stock-project/.venv/bin/python main.py --daemon
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

启动服务：

```bash
sudo systemctl daemon-reload
sudo systemctl enable stock-collector
sudo systemctl start stock-collector
sudo systemctl status stock-collector
```

## 数据查询

### 使用Elasticsearch API查询

```bash
# 查询所有股票数据
curl -X GET "localhost:9200/stock_basic_data/_search?pretty"

# 查询特定股票
curl -X GET "localhost:9200/stock_basic_data/_search?pretty" -H 'Content-Type: application/json' -d'
{
  "query": {
    "term": {
      "stock_code": "000001"
    }
  }
}'

# 查询深圳市场股票
curl -X GET "localhost:9200/stock_basic_data/_search?pretty" -H 'Content-Type: application/json' -d'
{
  "query": {
    "term": {
      "market_type": "SZ"
    }
  }
}'
```

### 使用Python查询

```python
from src.data_storage.es_storage import ElasticsearchStorage

# 创建存储实例
storage = ElasticsearchStorage()

# 查询所有数据
all_stocks = storage.search_stock(size=100)

# 查询特定股票
stock = storage.search_stock(stock_code='000001')

# 查询特定市场
sz_stocks = storage.search_stock(market_type='SZ', size=50)

# 获取数据总量
count = storage.get_stock_count()
print(f"总数据量: {count}条")
```

## 日志管理

系统日志存储在 `logs/stock_collector.log` 文件中，支持自动轮转：

- 单个日志文件最大10MB
- 保留最近5个日志文件
- 自动压缩历史日志

查看日志：

```bash
# 查看最新日志
tail -f logs/stock_collector.log

# 查看错误日志
grep "ERROR" logs/stock_collector.log

# 查看今天的日志
grep "$(date +%Y-%m-%d)" logs/stock_collector.log
```

## 监控和告警

### 系统监控

可以通过以下方式监控系统运行状态：

1. **日志监控**: 监控错误日志和异常信息
2. **数据监控**: 检查每日数据更新情况
3. **进程监控**: 确保守护进程正常运行

### 健康检查脚本

```bash
#!/bin/bash
# health_check.sh

# 检查进程是否运行
if pgrep -f "main.py --daemon" > /dev/null; then
    echo "Stock collector is running"
else
    echo "Stock collector is not running"
    # 可以在这里添加重启逻辑
fi

# 检查最近的数据更新
python -c "
from src.data_storage.es_storage import ElasticsearchStorage
storage = ElasticsearchStorage()
count = storage.get_stock_count()
print(f'Current data count: {count}')
storage.close()
"
```

## 故障排除

### 常见问题

1. **连接Elasticsearch失败**
   - 检查ES服务是否启动
   - 验证连接配置
   - 检查网络连接

2. **数据采集失败**
   - 检查网络连接
   - 验证AKshare版本
   - 查看详细错误日志

3. **定时任务不执行**
   - 检查系统时间
   - 验证调度配置
   - 查看进程状态

### 调试模式

设置日志级别为DEBUG：

```bash
export LOG_LEVEL=DEBUG
python main.py --run-once
```

## 开发和扩展

### 添加新的数据源

1. 在 `src/data_collector/stock_collector.py` 中添加新的采集方法
2. 更新 `config/es_mapping.py` 中的字段映射
3. 修改 `config/settings.py` 中的配置

### 自定义索引字段

编辑 `config/es_mapping.py` 文件，添加或修改字段定义：

```python
STOCK_MAPPING = {
    "mappings": {
        "properties": {
            # 添加新字段
            "new_field": {
                "type": "keyword"
            }
        }
    }
}
```

## 许可证

MIT License

## 贡献

欢迎提交Issue和Pull Request！

## 联系方式

如有问题，请通过以下方式联系：

- 提交Issue
- 发送邮件

---

**注意**: 本系统仅用于学习和研究目的，请遵守相关法律法规和数据使用协议。