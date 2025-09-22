# Elasticsearch数据存储模块
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from typing import List, Dict, Optional
from loguru import logger
import json
from datetime import datetime

from config.settings import ES_CONFIG, DATA_COLLECTION_CONFIG
from config.es_mapping import STOCK_INDEX_NAME, STOCK_MAPPING, STOCK_ALIAS


class ElasticsearchStorage:
    """Elasticsearch数据存储器"""
    
    def __init__(self):
        self.es_client = None
        self.batch_size = DATA_COLLECTION_CONFIG['batch_size']
        self._connect()
    
    def _connect(self):
        """连接Elasticsearch"""
        try:
            # 构建连接配置
            es_config = {
                'hosts': [{'scheme': ES_CONFIG['scheme'], 'host': ES_CONFIG['host'], 'port': ES_CONFIG['port']}],
                'request_timeout': ES_CONFIG['timeout']
            }
            
            # 如果配置了用户名和密码
            if ES_CONFIG['username'] and ES_CONFIG['password']:
                es_config['basic_auth'] = (ES_CONFIG['username'], ES_CONFIG['password'])
            
            # SSL配置
            if ES_CONFIG['use_ssl']:
                es_config['use_ssl'] = True
                es_config['verify_certs'] = ES_CONFIG['verify_certs']
            
            self.es_client = Elasticsearch(**es_config)
            
            # 测试连接
            if self.es_client.ping():
                logger.info("Elasticsearch连接成功")
            else:
                logger.error("Elasticsearch连接失败")
                
        except Exception as e:
            logger.error(f"连接Elasticsearch异常: {str(e)}")
            self.es_client = None
    
    def _ensure_index_exists(self) -> bool:
        """确保索引存在"""
        try:
            if not self.es_client:
                logger.error("Elasticsearch客户端未连接")
                return False
            
            # 检查索引是否存在
            if not self.es_client.indices.exists(index=STOCK_INDEX_NAME):
                logger.info(f"创建索引: {STOCK_INDEX_NAME}")
                
                # 创建索引
                response = self.es_client.indices.create(
                    index=STOCK_INDEX_NAME,
                    body=STOCK_MAPPING
                )
                
                if response.get('acknowledged'):
                    logger.info(f"索引{STOCK_INDEX_NAME}创建成功")
                else:
                    logger.error(f"索引{STOCK_INDEX_NAME}创建失败")
                    return False
            
            # 创建或更新别名
            if not self.es_client.indices.exists_alias(name=STOCK_ALIAS):
                self.es_client.indices.put_alias(
                    index=STOCK_INDEX_NAME,
                    name=STOCK_ALIAS
                )
                logger.info(f"别名{STOCK_ALIAS}创建成功")
            
            return True
            
        except Exception as e:
            logger.error(f"确保索引存在异常: {str(e)}")
            return False
    
    def _prepare_bulk_data(self, stock_data: List[Dict]) -> List[Dict]:
        """准备批量插入数据"""
        bulk_data = []
        
        for stock in stock_data:
            # 使用股票代码作为文档ID，实现覆盖更新
            doc_id = stock.get('stock_code')
            if not doc_id:
                logger.warning(f"股票数据缺少代码: {stock}")
                continue
            
            # 构建批量操作文档
            action = {
                '_index': STOCK_INDEX_NAME,
                '_id': doc_id,
                '_source': stock
            }
            bulk_data.append(action)
        
        return bulk_data
    
    def clear_index(self) -> bool:
        """清空索引数据"""
        try:
            if not self.es_client:
                logger.error("Elasticsearch客户端未连接")
                return False
            
            # 删除所有文档
            response = self.es_client.delete_by_query(
                index=STOCK_INDEX_NAME,
                body={"query": {"match_all": {}}}
            )
            
            deleted_count = response.get('deleted', 0)
            logger.info(f"清空索引完成，删除{deleted_count}条记录")
            return True
            
        except Exception as e:
            logger.error(f"清空索引异常: {str(e)}")
            return False
    
    def save_stock_data(self, stock_data: List[Dict], clear_before_save: bool = True) -> bool:
        """保存股票数据到Elasticsearch"""
        try:
            if not self.es_client:
                logger.error("Elasticsearch客户端未连接")
                return False
            
            if not stock_data:
                logger.warning("没有数据需要保存")
                return True
            
            # 确保索引存在
            if not self._ensure_index_exists():
                return False
            
            # 如果需要清空原有数据
            if clear_before_save:
                logger.info("清空原有数据")
                self.clear_index()
            
            # 准备批量数据
            bulk_data = self._prepare_bulk_data(stock_data)
            
            if not bulk_data:
                logger.warning("没有有效的数据需要保存")
                return True
            
            logger.info(f"开始批量保存{len(bulk_data)}条股票数据")
            
            # 分批处理
            success_count = 0
            error_count = 0
            
            for i in range(0, len(bulk_data), self.batch_size):
                batch = bulk_data[i:i + self.batch_size]
                
                try:
                    # 执行批量操作
                    success, failed = bulk(
                        self.es_client,
                        batch,
                        index=STOCK_INDEX_NAME,
                        refresh=True
                    )
                    
                    success_count += success
                    error_count += len(failed)
                    
                    logger.info(f"批次{i//self.batch_size + 1}: 成功{success}条, 失败{len(failed)}条")
                    
                    # 记录失败的文档
                    for fail_doc in failed:
                        logger.error(f"保存失败: {fail_doc}")
                        
                except Exception as e:
                    logger.error(f"批量保存异常: {str(e)}")
                    error_count += len(batch)
            
            logger.info(f"数据保存完成: 成功{success_count}条, 失败{error_count}条")
            
            # 刷新索引
            self.es_client.indices.refresh(index=STOCK_INDEX_NAME)
            
            return error_count == 0
            
        except Exception as e:
            logger.error(f"保存股票数据异常: {str(e)}")
            return False
    
    def get_stock_count(self) -> int:
        """获取股票数据总数"""
        try:
            if not self.es_client:
                return 0
            
            response = self.es_client.count(index=STOCK_INDEX_NAME)
            return response.get('count', 0)
            
        except Exception as e:
            logger.error(f"获取股票数据总数异常: {str(e)}")
            return 0
    
    def search_stock(self, stock_code: str = None, market_type: str = None, size: int = 10) -> List[Dict]:
        """搜索股票数据"""
        try:
            if not self.es_client:
                return []
            
            # 构建查询条件
            query = {"match_all": {}}
            
            if stock_code or market_type:
                must_conditions = []
                
                if stock_code:
                    must_conditions.append({"term": {"stock_code": stock_code}})
                
                if market_type:
                    must_conditions.append({"term": {"market_type": market_type}})
                
                query = {
                    "bool": {
                        "must": must_conditions
                    }
                }
            
            # 执行搜索
            response = self.es_client.search(
                index=STOCK_INDEX_NAME,
                body={
                    "query": query,
                    "size": size,
                    "sort": [{"update_time": {"order": "desc"}}]
                }
            )
            
            # 提取结果
            results = []
            for hit in response['hits']['hits']:
                results.append(hit['_source'])
            
            return results
            
        except Exception as e:
            logger.error(f"搜索股票数据异常: {str(e)}")
            return []
    
    def get_all_stock_codes(self, market_type: str = None) -> List[str]:
        """从stock_basic_data索引获取所有股票代码
        
        Args:
            market_type (str, optional): 市场类型过滤 (SH/SZ)
            
        Returns:
            List[str]: 股票代码列表
        """
        try:
            if not self.es_client:
                logger.error("Elasticsearch客户端未连接")
                return []
            
            # 构建查询条件
            query = {"match_all": {}}
            if market_type:
                query = {
                    "bool": {
                        "must": [{"term": {"market_type": market_type}}]
                    }
                }
            
            # 使用scroll API获取所有数据
            stock_codes = []
            scroll_size = 1000
            
            # 初始搜索
            response = self.es_client.search(
                index="stock_basic_data",
                body={
                    "query": query,
                    "size": scroll_size,
                    "_source": ["stock_code", "market_type"],
                    "sort": [{"stock_code": {"order": "asc"}}]
                },
                scroll='2m'
            )
            
            # 处理第一批结果
            for hit in response['hits']['hits']:
                source = hit['_source']
                if 'stock_code' in source:
                    stock_codes.append(source['stock_code'])
            
            # 获取scroll_id
            scroll_id = response.get('_scroll_id')
            
            # 继续滚动获取剩余数据
            while scroll_id and len(response['hits']['hits']) > 0:
                response = self.es_client.scroll(
                    scroll_id=scroll_id,
                    scroll='2m'
                )
                
                for hit in response['hits']['hits']:
                    source = hit['_source']
                    if 'stock_code' in source:
                        stock_codes.append(source['stock_code'])
                
                scroll_id = response.get('_scroll_id')
            
            # 清理scroll
            if scroll_id:
                try:
                    self.es_client.clear_scroll(scroll_id=scroll_id)
                except Exception:
                    pass
            
            logger.info(f"从ES获取到 {len(stock_codes)} 个股票代码")
            return list(set(stock_codes))  # 去重
            
        except Exception as e:
            logger.error(f"获取股票代码列表异常: {str(e)}")
            return []
    
    def close(self):
        """关闭连接"""
        if self.es_client:
            self.es_client.close()
            logger.info("Elasticsearch连接已关闭")


if __name__ == "__main__":
    # 测试代码
    storage = ElasticsearchStorage()
    
    # 测试数据
    test_data = [
        {
            'stock_code': '000001',
            'stock_name': '平安银行',
            'market_type': 'SZ',
            'current_price': 10.50,
            'change_percent': 2.5,
            'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'data_source': 'test'
        }
    ]
    
    # 保存测试数据
    result = storage.save_stock_data(test_data)
    print(f"保存结果: {result}")
    
    # 查询数据
    count = storage.get_stock_count()
    print(f"总数据量: {count}")
    
    # 搜索数据
    search_result = storage.search_stock(stock_code='000001')
    print(f"搜索结果: {search_result}")
    
    storage.close()