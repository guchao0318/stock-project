#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
九转策略数据存储模块
负责将符合九转策略条件的股票信息存储到Elasticsearch中
"""

import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
from elasticsearch import Elasticsearch
from config.settings import ELASTICSEARCH_CONFIG
from config.es_mapping import ES_MAPPINGS

logger = logging.getLogger(__name__)

class NineTurnStorage:
    """九转策略数据存储类"""
    
    def __init__(self):
        """初始化存储客户端"""
        self.es_client = None
        self.index_name = ES_MAPPINGS['nine_turn_strategy']['index']
        self.connect()
    
    def connect(self):
        """连接到Elasticsearch"""
        try:
            self.es_client = Elasticsearch(
                hosts=[f"{ELASTICSEARCH_CONFIG['host']}:{ELASTICSEARCH_CONFIG['port']}"],
                timeout=ELASTICSEARCH_CONFIG.get('timeout', 30),
                max_retries=ELASTICSEARCH_CONFIG.get('max_retries', 3),
                retry_on_timeout=True
            )
            
            # 测试连接
            if self.es_client.ping():
                logger.info("成功连接到Elasticsearch")
                self._ensure_index_exists()
            else:
                logger.error("无法连接到Elasticsearch")
                raise ConnectionError("Elasticsearch连接失败")
                
        except Exception as e:
            logger.error(f"连接Elasticsearch时发生错误: {e}")
            raise
    
    def _ensure_index_exists(self):
        """确保索引存在"""
        try:
            if not self.es_client.indices.exists(index=self.index_name):
                # 创建索引
                mapping_config = ES_MAPPINGS['nine_turn_strategy']
                self.es_client.indices.create(
                    index=self.index_name,
                    body={
                        'mappings': mapping_config['mapping'],
                        'settings': mapping_config['settings']
                    }
                )
                logger.info(f"创建索引: {self.index_name}")
            else:
                logger.info(f"索引已存在: {self.index_name}")
                
        except Exception as e:
            logger.error(f"创建索引时发生错误: {e}")
            raise
    
    def save_strategy_signal(self, stock_data: Dict[str, Any]) -> bool:
        """保存九转策略信号
        
        Args:
            stock_data: 股票数据，包含日期、股票代码、市场类型等信息
            
        Returns:
            bool: 保存是否成功
        """
        try:
            # 构建文档数据
            doc = {
                'date': stock_data.get('date', datetime.now().strftime('%Y-%m-%d')),
                'stock_code': stock_data.get('stock_code'),
                'market_type': stock_data.get('market_type'),
                'signal_type': 'nine_turn_buy',  # 九转买入信号
                'current_price': stock_data.get('current_price'),
                'compare_price': stock_data.get('compare_price'),  # 对比的第1日收盘价
                'price_change': stock_data.get('price_change'),  # 价格变化
                'price_change_pct': stock_data.get('price_change_pct'),  # 价格变化百分比
                'created_at': datetime.now().isoformat(),
                'strategy_name': 'nine_turn_strategy',
                'confidence': stock_data.get('confidence', 0.8)  # 策略置信度
            }
            
            # 生成文档ID（避免重复）
            doc_id = f"{doc['date']}_{doc['stock_code']}_{doc['market_type']}"
            
            # 保存到Elasticsearch
            response = self.es_client.index(
                index=self.index_name,
                id=doc_id,
                body=doc
            )
            
            if response.get('result') in ['created', 'updated']:
                logger.info(f"成功保存九转策略信号: {doc['stock_code']} - {doc['date']}")
                return True
            else:
                logger.warning(f"保存九转策略信号失败: {response}")
                return False
                
        except Exception as e:
            logger.error(f"保存九转策略信号时发生错误: {e}")
            return False
    
    def batch_save_strategy_signals(self, signals_data: List[Dict[str, Any]]) -> Dict[str, int]:
        """批量保存九转策略信号
        
        Args:
            signals_data: 策略信号数据列表
            
        Returns:
            Dict[str, int]: 保存结果统计
        """
        if not signals_data:
            return {'success': 0, 'failed': 0}
        
        success_count = 0
        failed_count = 0
        
        try:
            # 准备批量操作数据
            bulk_data = []
            
            for signal_data in signals_data:
                doc = {
                    'date': signal_data.get('date', datetime.now().strftime('%Y-%m-%d')),
                    'stock_code': signal_data.get('stock_code'),
                    'market_type': signal_data.get('market_type'),
                    'signal_type': 'nine_turn_buy',
                    'current_price': signal_data.get('current_price'),
                    'compare_price': signal_data.get('compare_price'),
                    'price_change': signal_data.get('price_change'),
                    'price_change_pct': signal_data.get('price_change_pct'),
                    'created_at': datetime.now().isoformat(),
                    'strategy_name': 'nine_turn_strategy',
                    'confidence': signal_data.get('confidence', 0.8)
                }
                
                doc_id = f"{doc['date']}_{doc['stock_code']}_{doc['market_type']}"
                
                bulk_data.extend([
                    {'index': {'_index': self.index_name, '_id': doc_id}},
                    doc
                ])
            
            # 执行批量操作
            if bulk_data:
                response = self.es_client.bulk(body=bulk_data)
                
                # 统计结果
                for item in response.get('items', []):
                    if 'index' in item:
                        if item['index'].get('status') in [200, 201]:
                            success_count += 1
                        else:
                            failed_count += 1
                            logger.warning(f"批量保存失败项: {item}")
                
                logger.info(f"批量保存九转策略信号完成: 成功 {success_count}, 失败 {failed_count}")
            
        except Exception as e:
            logger.error(f"批量保存九转策略信号时发生错误: {e}")
            failed_count = len(signals_data)
        
        return {'success': success_count, 'failed': failed_count}
    
    def query_strategy_signals(self, 
                             date_from: Optional[str] = None,
                             date_to: Optional[str] = None,
                             stock_code: Optional[str] = None,
                             market_type: Optional[str] = None,
                             limit: int = 100) -> List[Dict[str, Any]]:
        """查询九转策略信号
        
        Args:
            date_from: 开始日期
            date_to: 结束日期
            stock_code: 股票代码
            market_type: 市场类型
            limit: 返回数量限制
            
        Returns:
            List[Dict[str, Any]]: 策略信号列表
        """
        try:
            # 构建查询条件
            query = {'bool': {'must': []}}
            
            if date_from or date_to:
                date_range = {}
                if date_from:
                    date_range['gte'] = date_from
                if date_to:
                    date_range['lte'] = date_to
                query['bool']['must'].append({'range': {'date': date_range}})
            
            if stock_code:
                query['bool']['must'].append({'term': {'stock_code': stock_code}})
            
            if market_type:
                query['bool']['must'].append({'term': {'market_type': market_type}})
            
            # 如果没有查询条件，查询所有
            if not query['bool']['must']:
                query = {'match_all': {}}
            
            # 执行查询
            response = self.es_client.search(
                index=self.index_name,
                body={
                    'query': query,
                    'sort': [{'created_at': {'order': 'desc'}}],
                    'size': limit
                }
            )
            
            # 提取结果
            signals = []
            for hit in response.get('hits', {}).get('hits', []):
                signal_data = hit['_source']
                signal_data['_id'] = hit['_id']
                signals.append(signal_data)
            
            logger.info(f"查询到 {len(signals)} 条九转策略信号")
            return signals
            
        except Exception as e:
            logger.error(f"查询九转策略信号时发生错误: {e}")
            return []
    
    def get_health_status(self) -> Dict[str, Any]:
        """获取存储健康状态
        
        Returns:
            Dict[str, Any]: 健康状态信息
        """
        try:
            # 检查连接状态
            if not self.es_client or not self.es_client.ping():
                return {
                    'status': 'unhealthy',
                    'message': 'Elasticsearch连接失败',
                    'timestamp': datetime.now().isoformat()
                }
            
            # 检查索引状态
            index_stats = self.es_client.indices.stats(index=self.index_name)
            doc_count = index_stats['indices'][self.index_name]['total']['docs']['count']
            
            return {
                'status': 'healthy',
                'index_name': self.index_name,
                'document_count': doc_count,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            return {
                'status': 'unhealthy',
                'message': f'健康检查失败: {str(e)}',
                'timestamp': datetime.now().isoformat()
            }
    
    def close(self):
        """关闭连接"""
        if self.es_client:
            self.es_client.close()
            logger.info("九转策略存储连接已关闭")


if __name__ == '__main__':
    # 测试代码
    logging.basicConfig(level=logging.INFO)
    
    storage = NineTurnStorage()
    
    # 测试保存策略信号
    test_signal = {
        'date': '2024-01-15',
        'stock_code': '000001',
        'market_type': 'SZ',
        'current_price': 1250,  # 12.50元 * 100
        'compare_price': 1200,  # 12.00元 * 100
        'price_change': 50,     # 0.50元 * 100
        'price_change_pct': 417, # 4.17% * 100
        'confidence': 0.85
    }
    
    success = storage.save_strategy_signal(test_signal)
    print(f"保存测试信号: {'成功' if success else '失败'}")
    
    # 测试查询
    signals = storage.query_strategy_signals(limit=5)
    print(f"查询到 {len(signals)} 条策略信号")
    
    # 测试健康状态
    health = storage.get_health_status()
    print(f"健康状态: {health}")
    
    storage.close()