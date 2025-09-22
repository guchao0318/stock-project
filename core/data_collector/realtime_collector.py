#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
实时行情数据采集模块

从AKShare获取沪深两市实时行情数据
"""

import akshare as ak
import pandas as pd
from datetime import datetime, date
import time
from typing import List, Dict, Optional
import sys
import os

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils.logger_config import get_logger
from core.data_storage.daily_storage import DailyDataStorage
from core.kafka.kafka_producer import KafkaProducer


class RealtimeDataCollector:
    """实时行情数据采集器"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.daily_storage = DailyDataStorage()
        self.kafka_producer = KafkaProducer()
        
    def get_sz_realtime_data(self) -> Optional[pd.DataFrame]:
        """获取深圳A股实时行情数据
        
        Returns:
            pd.DataFrame: 深圳A股实时行情数据
        """
        try:
            self.logger.info("开始获取深圳A股实时行情数据")
            data = ak.stock_sz_a_spot_em()
            
            if data is not None and not data.empty:
                self.logger.info(f"成功获取深圳A股实时行情数据，共 {len(data)} 条")
                return data
            else:
                self.logger.warning("深圳A股实时行情数据为空")
                return None
                
        except Exception as e:
            self.logger.error(f"获取深圳A股实时行情数据失败: {str(e)}")
            return None
    
    def get_sh_realtime_data(self) -> Optional[pd.DataFrame]:
        """获取上海A股实时行情数据
        
        Returns:
            pd.DataFrame: 上海A股实时行情数据
        """
        try:
            self.logger.info("开始获取上海A股实时行情数据")
            data = ak.stock_sh_a_spot_em()
            
            if data is not None and not data.empty:
                self.logger.info(f"成功获取上海A股实时行情数据，共 {len(data)} 条")
                return data
            else:
                self.logger.warning("上海A股实时行情数据为空")
                return None
                
        except Exception as e:
            self.logger.error(f"获取上海A股实时行情数据失败: {str(e)}")
            return None
    
    def process_realtime_data(self, data: pd.DataFrame, market_type: str) -> List[Dict]:
        """处理实时行情数据
        
        Args:
            data (pd.DataFrame): 原始行情数据
            market_type (str): 市场类型 ('SZ' 或 'SH')
            
        Returns:
            List[Dict]: 处理后的行情数据列表
        """
        processed_data = []
        current_date = date.today().strftime('%Y-%m-%d')
        
        try:
            for _, row in data.iterrows():
                # 获取前一个交易日收盘价作为昨收价
                prev_close = self._get_previous_close_price(row['代码'], current_date)
                
                # 数据映射和处理
                stock_data = {
                    'date': current_date,
                    'stock_code': row['代码'],
                    'market_type': market_type,
                    'open_price': int(float(row['今开']) * 100),  # 今开 -> 开盘价，乘以100
                    'close_price': int(float(row['最新价']) * 100),  # 最新价 -> 收盘价，乘以100
                    'high_price': int(float(row['最高']) * 100),  # 最高 -> 最高价，乘以100
                    'low_price': int(float(row['最低']) * 100),  # 最低 -> 最低价，乘以100
                    'prev_close_price': prev_close,  # 昨收价
                    'volume': int(float(row['成交量']) * 100),  # 成交量，乘以100
                    'amount': int(float(row['成交额']) * 100),  # 成交额，乘以100
                    'change_percent': int(float(row['涨跌幅']) * 100),  # 涨跌幅，乘以100
                    'change_amount': int(float(row['涨跌额']) * 100),  # 涨跌额，乘以100
                    'turnover_rate': int(float(row['换手率']) * 100),  # 换手率，乘以100
                    'created_at': datetime.now().isoformat()
                }
                
                processed_data.append(stock_data)
                
        except Exception as e:
            self.logger.error(f"处理实时行情数据失败: {str(e)}")
            
        return processed_data
    
    def _get_previous_close_price(self, stock_code: str, current_date: str) -> int:
        """获取前一个交易日的收盘价作为昨收价
        
        Args:
            stock_code (str): 股票代码
            current_date (str): 当前日期
            
        Returns:
            int: 昨收价（乘以100后的整数）
        """
        try:
            # 从ES中查询该股票最近的一条记录
            recent_data = self.daily_storage.search_daily_data(
                stock_code=stock_code,
                size=1,
                sort_by_date=True,
                ascending=False
            )
            
            if recent_data and len(recent_data) > 0:
                return recent_data[0].get('close_price', 0)
            else:
                # 如果没有历史数据，返回0
                return 0
                
        except Exception as e:
            self.logger.warning(f"获取股票 {stock_code} 昨收价失败: {str(e)}")
            return 0
    
    def collect_all_realtime_data(self) -> Dict:
        """采集所有实时行情数据
        
        Returns:
            Dict: 采集结果统计
        """
        result = {
            'total': 0,
            'success': 0,
            'failed': 0,
            'sz_count': 0,
            'sh_count': 0
        }
        
        all_data = []
        
        try:
            # 获取深圳A股数据
            sz_data = self.get_sz_realtime_data()
            if sz_data is not None:
                sz_processed = self.process_realtime_data(sz_data, 'SZ')
                all_data.extend(sz_processed)
                result['sz_count'] = len(sz_processed)
                self.logger.info(f"深圳A股处理完成，共 {len(sz_processed)} 条")
            
            # 延迟1秒，避免请求过于频繁
            time.sleep(1)
            
            # 获取上海A股数据
            sh_data = self.get_sh_realtime_data()
            if sh_data is not None:
                sh_processed = self.process_realtime_data(sh_data, 'SH')
                all_data.extend(sh_processed)
                result['sh_count'] = len(sh_processed)
                self.logger.info(f"上海A股处理完成，共 {len(sh_processed)} 条")
            
            result['total'] = len(all_data)
            
            if all_data:
                # 保存到ES
                save_success = self.daily_storage.save_daily_data_batch(all_data)
                if save_success:
                    result['success'] = len(all_data)
                    self.logger.info(f"实时行情数据保存成功，共 {len(all_data)} 条")
                    
                    # 发送到Kafka
                    self._send_to_kafka(all_data)
                else:
                    result['failed'] = len(all_data)
                    self.logger.error("实时行情数据保存失败")
            
        except Exception as e:
            self.logger.error(f"采集实时行情数据失败: {str(e)}")
            result['failed'] = result['total']
        
        return result
    
    def _send_to_kafka(self, data: List[Dict]):
        """发送数据到Kafka
        
        Args:
            data (List[Dict]): 行情数据列表
        """
        try:
            for stock_data in data:
                self.kafka_producer.send_stock_data(stock_data)
            
            self.logger.info(f"成功发送 {len(data)} 条行情数据到Kafka")
            
        except Exception as e:
            self.logger.error(f"发送数据到Kafka失败: {str(e)}")
    
    def close(self):
        """关闭连接"""
        try:
            if hasattr(self, 'daily_storage'):
                self.daily_storage.close()
            if hasattr(self, 'kafka_producer'):
                self.kafka_producer.close()
        except Exception as e:
            self.logger.error(f"关闭连接失败: {str(e)}")


if __name__ == "__main__":
    # 测试代码
    collector = RealtimeDataCollector()
    
    try:
        print("开始测试实时行情数据采集...")
        result = collector.collect_all_realtime_data()
        
        print(f"\n采集结果:")
        print(f"总计: {result['total']} 条")
        print(f"成功: {result['success']} 条")
        print(f"失败: {result['failed']} 条")
        print(f"深圳A股: {result['sz_count']} 条")
        print(f"上海A股: {result['sh_count']} 条")
        
    except Exception as e:
        print(f"测试失败: {str(e)}")
    
    finally:
        collector.close()
        print("测试完成")