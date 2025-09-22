# 股票数据HTTP API使用指南

## 概述

本项目提供了HTTP API接口，允许通过Web服务的方式调用股票历史数据采集功能。API基于Flask框架构建，提供RESTful风格的接口。

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 启动API服务器

```bash
# 方式1: 使用main.py启动
python main.py --start-api

# 方式2: 使用专用启动脚本
python start_api_server.py

# 方式3: 使用gunicorn启动(生产环境)
gunicorn -w 4 -b 0.0.0.0:5000 start_api_server:app
```

服务器默认运行在 `http://localhost:5000`

### 3. 测试API

```bash
# 运行测试脚本
python test_api.py
```

## API接口文档

### 1. 健康检查

**接口**: `GET /health`

**描述**: 检查API服务器状态

**响应示例**:
```json
{
    "status": "healthy",
    "timestamp": "2024-01-15T10:30:00Z",
    "version": "1.0.0"
}
```

### 2. 获取股票代码列表

**接口**: `GET /api/v1/stocks/codes`

**描述**: 从Elasticsearch获取所有股票代码

**参数**:
- `limit` (可选): 返回数量限制，默认1000

**请求示例**:
```bash
curl "http://localhost:5000/api/v1/stocks/codes?limit=10"
```

**响应示例**:
```json
{
    "success": true,
    "data": ["000001", "000002", "000004"],
    "total": 3,
    "message": "获取股票代码成功"
}
```

### 3. 采集历史数据

**接口**: `POST /api/v1/history/collect`

**描述**: 采集指定股票的历史行情数据

**请求参数**:
```json
{
    "start_date": "2024-01-01",     // 必需: 开始日期 (YYYY-MM-DD)
    "end_date": "2024-01-31",       // 必需: 结束日期 (YYYY-MM-DD)
    "stock_codes": ["000001", "000002"], // 可选: 股票代码列表，不提供则采集全部
    "adjust_flag": "qfq"            // 可选: 复权方式 (qfq/hfq/none)，默认qfq
}
```

**请求示例**:
```bash
curl -X POST "http://localhost:5000/api/v1/history/collect" \
     -H "Content-Type: application/json" \
     -d '{
         "start_date": "2024-01-01",
         "end_date": "2024-01-05",
         "stock_codes": ["000001", "000002"],
         "adjust_flag": "qfq"
     }'
```

**响应示例**:
```json
{
    "success": true,
    "data": {
        "total": 2,
        "success": 2,
        "failed": 0,
        "start_date": "2024-01-01",
        "end_date": "2024-01-05",
        "adjust_flag": "qfq"
    },
    "message": "历史数据采集完成"
}
```

## 使用Python客户端

项目提供了Python客户端类 `StockAPIClient`，方便在Python代码中调用API。

```python
from src.api.api_client import StockAPIClient

# 创建客户端
client = StockAPIClient(base_url="http://localhost:5000")

# 健康检查
health = client.health_check()
print(f"服务状态: {health['status']}")

# 获取股票代码
codes = client.get_stock_codes(limit=10)
print(f"股票代码: {codes}")

# 采集历史数据
result = client.collect_history_data(
    start_date="2024-01-01",
    end_date="2024-01-05",
    stock_codes=["000001", "000002"],
    adjust_flag="qfq"
)
print(f"采集结果: {result}")
```

## 错误处理

API使用标准HTTP状态码和JSON格式返回错误信息：

```json
{
    "success": false,
    "error": "参数验证失败",
    "details": "start_date字段是必需的"
}
```

常见错误码：
- `400`: 请求参数错误
- `500`: 服务器内部错误
- `503`: 服务不可用

## 配置说明

### 服务器配置

- **端口**: 默认5000，可在代码中修改
- **主机**: 默认0.0.0.0（所有接口），生产环境建议限制
- **线程**: 支持多线程处理请求

### 数据库配置

API依赖Elasticsearch存储，确保ES服务正常运行并配置正确的连接参数。

## 生产环境部署

### 使用Gunicorn

```bash
# 安装gunicorn
pip install gunicorn

# 启动服务
gunicorn -w 4 -b 0.0.0.0:5000 --timeout 300 start_api_server:app
```

### 使用Nginx反向代理

```nginx
server {
    listen 80;
    server_name your-domain.com;
    
    location / {
        proxy_pass http://127.0.0.1:5000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    }
}
```

## 注意事项

1. **数据量控制**: 大批量数据采集可能耗时较长，建议分批处理
2. **并发限制**: 避免同时发起过多采集请求
3. **错误重试**: 网络异常时建议实现重试机制
4. **日志监控**: 生产环境建议配置详细的日志和监控

## 故障排除

### 常见问题

1. **服务启动失败**
   - 检查端口是否被占用
   - 确认依赖包已正确安装
   - 查看错误日志

2. **数据采集失败**
   - 检查Elasticsearch连接
   - 确认股票代码格式正确
   - 查看采集器日志

3. **响应超时**
   - 减少单次采集的数据量
   - 增加超时时间配置
   - 检查网络连接

### 日志查看

```bash
# 查看应用日志
tail -f logs/stock_data_*.log

# 查看API访问日志
tail -f logs/api_access.log
```