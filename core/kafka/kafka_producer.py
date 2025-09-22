#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Kafka生产者模块

发送股票行情数据到Kafka
"""

import json
import time
from typing import Dict, Optional
from kafka import KafkaProducer as Producer
from kafka.errors import KafkaError
import sys
import os

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils.logger_config import get_logger
from config.settings import KAFKA_CONFIG


class KafkaProducer:
    """Kafka生产者"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.producer = None
        self.topic = KAFKA_CONFIG.get('STOCK_TOPIC', 'stock_realtime_data')
        self._connect()
    
    def _connect(self):
        """连接Kafka"""
        try:
            self.producer = Producer(
                bootstrap_servers=KAFKA_CONFIG.get('BOOTSTRAP_SERVERS', ['localhost:9092']),
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks=KAFKA_CONFIG.get('ACKS', 'all'),
                retries=KAFKA_CONFIG.get('RETRIES', 3),
                batch_size=KAFKA_CONFIG.get('BATCH_SIZE', 16384),
                linger_ms=KAFKA_CONFIG.get('LINGER_MS', 10),
                buffer_memory=KAFKA_CONFIG.get('BUFFER_MEMORY', 33554432),
                max_request_size=KAFKA_CONFIG.get('MAX_REQUEST_SIZE', 1048576)
            )
            self.logger.info("Kafka生产者连接成功")
            
        except Exception as e:
            self.logger.error(f"Kafka生产者连接失败: {str(e)}")
            raise
    
    def send_stock_data(self, stock_data: Dict, key: Optional[str] = None) -> bool:
        """发送股票数据到Kafka
        
        Args:
            stock_data (Dict): 股票数据
            key (str, optional): 消息键，默认使用股票代码
            
        Returns:
            bool: 发送是否成功
        """
        if not self.producer:
            self.logger.error("Kafka生产者未连接")
            return False
        
        try:
            # 如果没有指定key，使用股票代码作为key
            if key is None:
                key = stock_data.get('stock_code', '')
            
            # 发送消息
            future = self.producer.send(
                topic=self.topic,
                value=stock_data,
                key=key
            )
            
            # 等待发送结果（可选，用于确保消息发送成功）
            record_metadata = future.get(timeout=10)
            
            self.logger.debug(
                f"消息发送成功 - Topic: {record_metadata.topic}, "
                f"Partition: {record_metadata.partition}, "
                f"Offset: {record_metadata.offset}, "
                f"Stock: {stock_data.get('stock_code', 'Unknown')}"
            )
            
            return True
            
        except KafkaError as e:
            self.logger.error(f"Kafka发送失败: {str(e)}")
            return False
        except Exception as e:
            self.logger.error(f"发送股票数据失败: {str(e)}")
            return False
    
    def send_stock_data_batch(self, stock_data_list: list) -> Dict:
        """批量发送股票数据到Kafka
        
        Args:
            stock_data_list (list): 股票数据列表
            
        Returns:
            Dict: 发送结果统计
        """
        result = {
            'total': len(stock_data_list),
            'success': 0,
            'failed': 0
        }
        
        if not self.producer:
            self.logger.error("Kafka生产者未连接")
            result['failed'] = result['total']
            return result
        
        try:
            for stock_data in stock_data_list:
                if self.send_stock_data(stock_data):
                    result['success'] += 1
                else:
                    result['failed'] += 1
            
            # 确保所有消息都发送完成
            self.producer.flush()
            
            self.logger.info(
                f"批量发送完成 - 总计: {result['total']}, "
                f"成功: {result['success']}, 失败: {result['failed']}"
            )
            
        except Exception as e:
            self.logger.error(f"批量发送股票数据失败: {str(e)}")
            result['failed'] = result['total'] - result['success']
        
        return result
    
    def send_strategy_signal(self, strategy_data: Dict) -> bool:
        """发送策略信号到Kafka
        
        Args:
            strategy_data (Dict): 策略数据
            
        Returns:
            bool: 发送是否成功
        """
        strategy_topic = KAFKA_CONFIG.get('STRATEGY_TOPIC', 'nine_turn_strategy')
        
        if not self.producer:
            self.logger.error("Kafka生产者未连接")
            return False
        
        try:
            key = strategy_data.get('stock_code', '')
            
            future = self.producer.send(
                topic=strategy_topic,
                value=strategy_data,
                key=key
            )
            
            record_metadata = future.get(timeout=10)
            
            self.logger.info(
                f"策略信号发送成功 - Stock: {strategy_data.get('stock_code', 'Unknown')}, "
                f"Date: {strategy_data.get('date', 'Unknown')}"
            )
            
            return True
            
        except Exception as e:
            self.logger.error(f"发送策略信号失败: {str(e)}")
            return False
    
    def get_producer_metrics(self) -> Dict:
        """获取生产者指标
        
        Returns:
            Dict: 生产者指标信息
        """
        if not self.producer:
            return {}
        
        try:
            metrics = self.producer.metrics()
            
            # 提取关键指标
            key_metrics = {
                'record_send_total': 0,
                'record_error_total': 0,
                'batch_size_avg': 0,
                'request_latency_avg': 0
            }
            
            for metric_name, metric_value in metrics.items():
                if 'record-send-total' in str(metric_name):
                    key_metrics['record_send_total'] += metric_value.value
                elif 'record-error-total' in str(metric_name):
                    key_metrics['record_error_total'] += metric_value.value
                elif 'batch-size-avg' in str(metric_name):
                    key_metrics['batch_size_avg'] = metric_value.value
                elif 'request-latency-avg' in str(metric_name):
                    key_metrics['request_latency_avg'] = metric_value.value
            
            return key_metrics
            
        except Exception as e:
            self.logger.error(f"获取生产者指标失败: {str(e)}")
            return {}
    
    def health_check(self) -> bool:
        """健康检查
        
        Returns:
            bool: 连接是否正常
        """
        try:
            if not self.producer:
                return False
            
            # 发送测试消息
            test_data = {
                'type': 'health_check',
                'timestamp': time.time(),
                'message': 'Kafka producer health check'
            }
            
            future = self.producer.send(
                topic='health_check',
                value=test_data
            )
            
            future.get(timeout=5)
            return True
            
        except Exception as e:
            self.logger.warning(f"Kafka健康检查失败: {str(e)}")
            return False
    
    def close(self):
        """关闭Kafka生产者"""
        try:
            if self.producer:
                self.producer.flush()  # 确保所有消息都发送完成
                self.producer.close()
                self.logger.info("Kafka生产者已关闭")
        except Exception as e:
            self.logger.error(f"关闭Kafka生产者失败: {str(e)}")


if __name__ == "__main__":
    # 测试代码
    producer = KafkaProducer()
    
    try:
        # 测试发送单条数据
        test_data = {
            'date': '2025-01-17',
            'stock_code': '000001',
            'market_type': 'SZ',
            'open_price': 1000,
            'close_price': 1050,
            'high_price': 1080,
            'low_price': 990,
            'volume': 1000000,
            'amount': 105000000
        }
        
        print("测试发送单条数据...")
        success = producer.send_stock_data(test_data)
        print(f"发送结果: {'成功' if success else '失败'}")
        
        # 测试健康检查
        print("\n测试健康检查...")
        health = producer.health_check()
        print(f"健康状态: {'正常' if health else '异常'}")
        
        # 获取指标
        print("\n获取生产者指标...")
        metrics = producer.get_producer_metrics()
        print(f"指标: {metrics}")
        
    except Exception as e:
        print(f"测试失败: {str(e)}")
    
    finally:
        producer.close()
        print("测试完成")