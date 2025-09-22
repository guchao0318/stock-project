#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flink九转策略处理模块

从Kafka读取股票行情数据，实现九转策略逻辑
"""

import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction, ProcessFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.time import Time
import sys
import os

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils.logger_config import get_logger
from core.data_storage.daily_storage import DailyDataStorage
from core.data_storage.strategy_storage import StrategyStorage
from config.settings import KAFKA_CONFIG, FLINK_CONFIG


class StockDataProcessor(ProcessFunction):
    """股票数据处理器"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.daily_storage = None
        self.strategy_storage = None
    
    def open(self, runtime_context):
        """初始化"""
        self.daily_storage = DailyDataStorage()
        self.strategy_storage = StrategyStorage()
        self.logger.info("股票数据处理器初始化完成")
    
    def process_element(self, value, ctx, out):
        """处理股票数据
        
        Args:
            value: 股票数据JSON字符串
            ctx: 处理上下文
            out: 输出收集器
        """
        try:
            # 解析JSON数据
            stock_data = json.loads(value)
            
            # 执行九转策略逻辑
            strategy_result = self._execute_nine_turn_strategy(stock_data)
            
            if strategy_result:
                # 输出策略结果
                out.collect(json.dumps(strategy_result, ensure_ascii=False))
                self.logger.info(
                    f"九转策略触发 - 股票: {stock_data.get('stock_code')}, "
                    f"日期: {stock_data.get('date')}"
                )
            
        except Exception as e:
            self.logger.error(f"处理股票数据失败: {str(e)}")
    
    def _execute_nine_turn_strategy(self, current_data: Dict) -> Optional[Dict]:
        """执行九转策略逻辑
        
        Args:
            current_data (Dict): 当前股票数据
            
        Returns:
            Optional[Dict]: 策略结果，如果触发策略则返回数据，否则返回None
        """
        try:
            stock_code = current_data.get('stock_code')
            current_date = current_data.get('date')
            current_price = current_data.get('close_price', 0)  # 当前最新价（已乘以100）
            
            if not stock_code or not current_date or current_price <= 0:
                return None
            
            # 从ES获取前4日行情数据（按日期正序排列）
            historical_data = self._get_historical_data(stock_code, current_date, 4)
            
            if not historical_data or len(historical_data) == 0:
                self.logger.debug(f"股票 {stock_code} 没有足够的历史数据")
                return None
            
            # 获取第1条数据的收盘价（最早的一条）
            first_close_price = historical_data[0].get('close_price', 0)
            
            if first_close_price <= 0:
                return None
            
            # 九转策略逻辑：当前最新价 > 前4日第1条数据的收盘价
            if current_price > first_close_price:
                strategy_data = {
                    'date': current_date,
                    'stock_code': stock_code,
                    'market_type': current_data.get('market_type'),
                    'current_price': current_price,
                    'compare_price': first_close_price,
                    'price_diff': current_price - first_close_price,
                    'price_diff_percent': round((current_price - first_close_price) / first_close_price * 10000, 2),  # 百分比*100
                    'created_at': datetime.now().isoformat()
                }
                
                # 保存到ES九转策略索引
                self._save_strategy_result(strategy_data)
                
                return strategy_data
            
            return None
            
        except Exception as e:
            self.logger.error(f"执行九转策略失败: {str(e)}")
            return None
    
    def _get_historical_data(self, stock_code: str, current_date: str, days: int) -> List[Dict]:
        """获取历史数据
        
        Args:
            stock_code (str): 股票代码
            current_date (str): 当前日期
            days (int): 获取天数
            
        Returns:
            List[Dict]: 历史数据列表，按日期正序排列
        """
        try:
            # 计算查询的结束日期（当前日期的前一天）
            current_dt = datetime.strptime(current_date, '%Y-%m-%d')
            end_date = (current_dt - timedelta(days=1)).strftime('%Y-%m-%d')
            
            # 从ES查询历史数据
            historical_data = self.daily_storage.search_daily_data(
                stock_code=stock_code,
                size=days,
                sort_by_date=True,
                ascending=True,  # 正序排列
                end_date=end_date
            )
            
            return historical_data if historical_data else []
            
        except Exception as e:
            self.logger.error(f"获取历史数据失败: {str(e)}")
            return []
    
    def _save_strategy_result(self, strategy_data: Dict):
        """保存策略结果到ES
        
        Args:
            strategy_data (Dict): 策略数据
        """
        try:
            if self.strategy_storage:
                self.strategy_storage.save_strategy_data(strategy_data)
            
        except Exception as e:
            self.logger.error(f"保存策略结果失败: {str(e)}")
    
    def close(self):
        """关闭资源"""
        try:
            if self.daily_storage:
                self.daily_storage.close()
            if self.strategy_storage:
                self.strategy_storage.close()
        except Exception as e:
            self.logger.error(f"关闭资源失败: {str(e)}")


class NineTurnStrategyJob:
    """九转策略Flink作业"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.env = None
        self.table_env = None
    
    def setup_environment(self):
        """设置Flink环境"""
        try:
            # 创建流执行环境
            self.env = StreamExecutionEnvironment.get_execution_environment()
            
            # 设置并行度
            self.env.set_parallelism(FLINK_CONFIG.get('PARALLELISM', 1))
            
            # 设置检查点
            self.env.enable_checkpointing(FLINK_CONFIG.get('CHECKPOINT_INTERVAL', 60000))
            
            # 设置重启策略
            restart_strategy = FLINK_CONFIG.get('RESTART_STRATEGY', 'fixed-delay')
            if restart_strategy == 'fixed-delay':
                self.env.set_restart_strategy(
                    restart_strategy,
                    FLINK_CONFIG.get('RESTART_ATTEMPTS', 3),
                    Time.milliseconds(FLINK_CONFIG.get('RESTART_DELAY', 10000))
                )
            
            # 创建表环境
            settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
            self.table_env = StreamTableEnvironment.create(self.env, settings)
            
            self.logger.info("Flink环境设置完成")
            
        except Exception as e:
            self.logger.error(f"设置Flink环境失败: {str(e)}")
            raise
    
    def create_kafka_source(self):
        """创建Kafka数据源
        
        Returns:
            DataStream: Kafka数据流
        """
        try:
            # Kafka消费者配置
            kafka_props = {
                'bootstrap.servers': ','.join(KAFKA_CONFIG.get('BOOTSTRAP_SERVERS', ['localhost:9092'])),
                'group.id': 'nine_turn_strategy_group',
                'auto.offset.reset': 'latest'
            }
            
            # 创建Kafka消费者
            kafka_consumer = FlinkKafkaConsumer(
                topics=KAFKA_CONFIG.get('STOCK_TOPIC', 'stock_realtime_data'),
                deserialization_schema=SimpleStringSchema(),
                properties=kafka_props
            )
            
            # 创建数据流
            data_stream = self.env.add_source(kafka_consumer)
            
            self.logger.info("Kafka数据源创建完成")
            return data_stream
            
        except Exception as e:
            self.logger.error(f"创建Kafka数据源失败: {str(e)}")
            raise
    
    def create_kafka_sink(self):
        """创建Kafka输出
        
        Returns:
            FlinkKafkaProducer: Kafka生产者
        """
        try:
            # Kafka生产者配置
            kafka_props = {
                'bootstrap.servers': ','.join(KAFKA_CONFIG.get('BOOTSTRAP_SERVERS', ['localhost:9092']))
            }
            
            # 创建Kafka生产者
            kafka_producer = FlinkKafkaProducer(
                topic=KAFKA_CONFIG.get('STRATEGY_TOPIC', 'nine_turn_strategy'),
                serialization_schema=SimpleStringSchema(),
                producer_config=kafka_props
            )
            
            self.logger.info("Kafka输出创建完成")
            return kafka_producer
            
        except Exception as e:
            self.logger.error(f"创建Kafka输出失败: {str(e)}")
            raise
    
    def run(self):
        """运行九转策略作业"""
        try:
            self.logger.info("开始运行九转策略Flink作业")
            
            # 设置环境
            self.setup_environment()
            
            # 创建数据源
            source_stream = self.create_kafka_source()
            
            # 处理数据
            processed_stream = source_stream.process(StockDataProcessor())
            
            # 创建输出
            kafka_sink = self.create_kafka_sink()
            
            # 输出到Kafka
            processed_stream.add_sink(kafka_sink)
            
            # 执行作业
            self.env.execute("Nine Turn Strategy Job")
            
        except Exception as e:
            self.logger.error(f"运行九转策略作业失败: {str(e)}")
            raise


def main():
    """主函数"""
    logger = get_logger(__name__)
    
    try:
        logger.info("启动九转策略Flink作业")
        
        # 创建并运行作业
        job = NineTurnStrategyJob()
        job.run()
        
    except KeyboardInterrupt:
        logger.info("接收到中断信号，停止作业")
    except Exception as e:
        logger.error(f"九转策略作业异常: {str(e)}")
        raise


if __name__ == "__main__":
    main()