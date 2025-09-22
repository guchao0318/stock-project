#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
日线数据存储模块

用于将股票日线历史数据存储到Elasticsearch中
"""

import sys
import os
from datetime import datetime
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import ConnectionError, RequestError

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.settings import ES_CONFIG
from config.es_mapping import DAILY_INDEX_NAME, DAILY_MAPPING, DAILY_ALIAS
from utils.logger_config import get_logger


class DailyDataStorage:
    """日线数据存储类"""
    
    def __init__(self):
        """初始化Elasticsearch连接"""
        self.logger = get_logger('daily_storage')
        self.es_client = None
        self.index_name = DAILY_INDEX_NAME
        self.alias_name = DAILY_ALIAS
        
        try:
            self.es_client =  Elasticsearch(
                hosts=[f"{ES_CONFIG['scheme']}://{ES_CONFIG['host']}:{ES_CONFIG['port']}"],
                request_timeout=ES_CONFIG['timeout'],
                # scheme=ES_CONFIG['scheme'],
                # max_retries=ES_CONFIG['max_retries'],
                retry_on_timeout=True
            )
            
            # 测试连接
            if self.es_client.ping():
                self.logger.info("Elasticsearch连接成功")
                self._ensure_index_exists()
            else:
                self.logger.error("Elasticsearch连接失败")
                self.es_client = None
                
        except Exception as e:
            self.logger.error(f"初始化Elasticsearch连接失败: {str(e)}")
            self.es_client = None
    
    def _ensure_index_exists(self):
        """确保索引存在，如果不存在则创建"""
        try:
            if not self.es_client.indices.exists(index=self.index_name):
                self.logger.info(f"创建日线数据索引: {self.index_name}")
                self.es_client.indices.create(
                    index=self.index_name,
                    body=DAILY_MAPPING
                )
                self.logger.info(f"日线数据索引 {self.index_name} 创建成功")
            else:
                self.logger.info(f"日线数据索引 {self.index_name} 已存在")
                
        except Exception as e:
            self.logger.error(f"创建日线数据索引失败: {str(e)}")
    
    def save_daily_data(self, data_list, batch_size=1000):
        """
        批量保存日线数据到Elasticsearch
        
        Args:
            data_list (list): 日线数据列表
            batch_size (int): 批量处理大小
            
        Returns:
            bool: 保存是否成功
        """
        if not self.es_client:
            self.logger.error("Elasticsearch连接不可用")
            return False
        
        if not data_list:
            self.logger.warning("没有数据需要保存")
            return True
        
        try:
            self.logger.info(f"开始保存日线数据，共 {len(data_list)} 条记录")
            
            # 准备批量插入的数据
            actions = []
            for data in data_list:
                # 生成文档ID，使用股票代码+日期作为唯一标识
                doc_id = f"{data['stock_code']}_{data['date']}"
                
                action = {
                    "_index": self.index_name,
                    "_id": doc_id,
                    "_source": data
                }
                actions.append(action)
            
            # 批量插入数据
            success_count = 0
            error_count = 0
            
            for i in range(0, len(actions), batch_size):
                batch_actions = actions[i:i + batch_size]
                
                try:
                    # 使用bulk API批量插入
                    response = helpers.bulk(
                        self.es_client,
                        batch_actions,
                        index=self.index_name,
                        timeout='60s',
                        max_retries=3
                    )
                    
                    batch_success = response[0]
                    success_count += batch_success
                    
                    if response[1]:  # 有错误
                        error_count += len(response[1])
                        self.logger.warning(f"批次 {i//batch_size + 1} 部分数据保存失败: {len(response[1])} 条")
                    
                    self.logger.info(f"批次 {i//batch_size + 1} 保存完成: 成功 {batch_success} 条")
                    
                except Exception as e:
                    error_count += len(batch_actions)
                    self.logger.error(f"批次 {i//batch_size + 1} 保存失败: {str(e)}")
            
            # 刷新索引
            self.es_client.indices.refresh(index=self.index_name)
            
            self.logger.info(f"日线数据保存完成: 成功 {success_count} 条, 失败 {error_count} 条")
            return error_count == 0
            
        except Exception as e:
            self.logger.error(f"保存日线数据失败: {str(e)}")
            return False
    
    def search_daily_data(self, stock_code=None, start_date=None, end_date=None, 
                         market_type=None, size=100, sort_by_date=True):
        """
        查询日线数据
        
        Args:
            stock_code (str): 股票代码
            start_date (str): 开始日期，格式 'YYYY-MM-DD'
            end_date (str): 结束日期，格式 'YYYY-MM-DD'
            market_type (str): 市场类型
            size (int): 返回结果数量
            sort_by_date (bool): 是否按日期排序
            
        Returns:
            list: 查询结果列表
        """
        if not self.es_client:
            self.logger.error("Elasticsearch连接不可用")
            return []
        
        try:
            # 构建查询条件
            query_conditions = []
            
            if stock_code:
                query_conditions.append({"term": {"stock_code": stock_code}})
            
            if market_type:
                query_conditions.append({"term": {"market_type": market_type}})
            
            if start_date or end_date:
                date_range = {}
                if start_date:
                    date_range["gte"] = start_date
                if end_date:
                    date_range["lte"] = end_date
                query_conditions.append({"range": {"date": date_range}})
            
            # 构建查询体
            if query_conditions:
                query = {"bool": {"must": query_conditions}}
            else:
                query = {"match_all": {}}
            
            # 构建排序
            sort_config = []
            if sort_by_date:
                sort_config.append({"date": {"order": "desc"}})
                sort_config.append({"stock_code": {"order": "asc"}})
            
            # 执行查询
            search_body = {
                "query": query,
                "size": size
            }
            
            if sort_config:
                search_body["sort"] = sort_config
            
            response = self.es_client.search(
                index=self.index_name,
                body=search_body
            )
            
            # 处理查询结果
            results = []
            for hit in response['hits']['hits']:
                result = hit['_source']
                result['_id'] = hit['_id']
                results.append(result)
            
            self.logger.info(f"查询日线数据完成，返回 {len(results)} 条记录")
            return results
            
        except Exception as e:
            self.logger.error(f"查询日线数据失败: {str(e)}")
            return []
    
    def get_daily_data_count(self, stock_code=None, start_date=None, end_date=None):
        """
        获取日线数据总数
        
        Args:
            stock_code (str): 股票代码
            start_date (str): 开始日期
            end_date (str): 结束日期
            
        Returns:
            int: 数据总数
        """
        if not self.es_client:
            return 0
        
        try:
            # 构建查询条件
            query_conditions = []
            
            if stock_code:
                query_conditions.append({"term": {"stock_code": stock_code}})
            
            if start_date or end_date:
                date_range = {}
                if start_date:
                    date_range["gte"] = start_date
                if end_date:
                    date_range["lte"] = end_date
                query_conditions.append({"range": {"date": date_range}})
            
            if query_conditions:
                query = {"bool": {"must": query_conditions}}
            else:
                query = {"match_all": {}}
            
            response = self.es_client.count(
                index=self.index_name,
                body={"query": query}
            )
            
            return response['count']
            
        except Exception as e:
            self.logger.error(f"获取日线数据总数失败: {str(e)}")
            return 0
    
    def delete_daily_data(self, stock_code=None, start_date=None, end_date=None):
        """
        删除日线数据
        
        Args:
            stock_code (str): 股票代码
            start_date (str): 开始日期
            end_date (str): 结束日期
            
        Returns:
            bool: 删除是否成功
        """
        if not self.es_client:
            self.logger.error("Elasticsearch连接不可用")
            return False
        
        try:
            # 构建删除查询条件
            query_conditions = []
            
            if stock_code:
                query_conditions.append({"term": {"stock_code": stock_code}})
            
            if start_date or end_date:
                date_range = {}
                if start_date:
                    date_range["gte"] = start_date
                if end_date:
                    date_range["lte"] = end_date
                query_conditions.append({"range": {"date": date_range}})
            
            if not query_conditions:
                self.logger.error("删除操作需要指定至少一个条件")
                return False
            
            query = {"bool": {"must": query_conditions}}
            
            # 执行删除
            response = self.es_client.delete_by_query(
                index=self.index_name,
                body={"query": query}
            )
            
            deleted_count = response.get('deleted', 0)
            self.logger.info(f"删除日线数据完成，删除 {deleted_count} 条记录")
            return True
            
        except Exception as e:
            self.logger.error(f"删除日线数据失败: {str(e)}")
            return False
    
    def get_stock_date_range(self, stock_code):
        """
        获取指定股票的数据日期范围
        
        Args:
            stock_code (str): 股票代码
            
        Returns:
            dict: 包含最早和最晚日期的字典
        """
        if not self.es_client:
            return {}
        
        try:
            # 查询最早日期
            min_query = {
                "query": {"term": {"stock_code": stock_code}},
                "aggs": {
                    "min_date": {"min": {"field": "date"}},
                    "max_date": {"max": {"field": "date"}}
                },
                "size": 0
            }
            
            response = self.es_client.search(
                index=self.index_name,
                body=min_query
            )
            
            aggs = response.get('aggregations', {})
            min_date = aggs.get('min_date', {}).get('value_as_string')
            max_date = aggs.get('max_date', {}).get('value_as_string')
            
            return {
                'min_date': min_date,
                'max_date': max_date,
                'total_count': response['hits']['total']['value']
            }
            
        except Exception as e:
            self.logger.error(f"获取股票 {stock_code} 日期范围失败: {str(e)}")
            return {}
    
    def close(self):
        """关闭Elasticsearch连接"""
        if self.es_client:
            self.es_client.close()
            self.logger.info("Elasticsearch连接已关闭")


if __name__ == "__main__":
    """测试日线数据存储"""
    storage = DailyDataStorage()
    
    # 测试数据
    test_data = [
        {
            'date': '2025-08-17',
            'stock_code': 'TEST001',
            'market_type': '深圳A股',
            'open_price': 1050,
            'close_price': 1080,
            'high_price': 1100,
            'low_price': 1040,
            'prev_close_price': 0,
            'volume': 100000000,
            'amount': 108000000000,
            'change_percent': 286,
            'change_amount': 30,
            'turnover_rate': 250,
            'update_time': '2025-01-01 10:00:00',
            'data_source': 'test'
        },
        {
            'date': '2025-08-18',
            'stock_code': 'TEST001',
            'market_type': '深圳A股',
            'open_price': 1080,
            'close_price': 1120,
            'high_price': 1150,
            'low_price': 1070,
            'prev_close_price': 1080,
            'volume': 120000000,
            'amount': 134400000000,
            'change_percent': 370,
            'change_amount': 40,
            'turnover_rate': 300,
            'update_time': '2025-01-01 10:00:00',
            'data_source': 'test'
        }
    ]
    
    print("测试保存日线数据...")
    save_result = storage.save_daily_data(test_data)
    print(f"保存结果: {save_result}")
    
    print("\n测试查询日线数据...")
    search_result = storage.search_daily_data(stock_code='TEST001')
    print(f"查询结果: {len(search_result)} 条记录")
    for item in search_result:
        print(f"  {item['date']} - {item['stock_code']}: 收盘价 {item['close_price']/100:.2f}")
    
    print("\n测试获取数据总数...")
    count = storage.get_daily_data_count(stock_code='TEST001')
    print(f"数据总数: {count}")
    
    print("\n测试获取日期范围...")
    date_range = storage.get_stock_date_range('TEST001')
    print(f"日期范围: {date_range}")
    
    print("\n清理测试数据...")
    delete_result = storage.delete_daily_data(stock_code='TEST001')
    print(f"删除结果: {delete_result}")
    
    storage.close()