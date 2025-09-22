#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
历史行情数据采集模块

用于获取股票的历史行情数据，包括日K线数据等
"""

import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import time
import sys
import os

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils.logger_config import get_logger
from core.data_storage.es_storage import ElasticsearchStorage


class HistoryDataCollector:
    """历史行情数据采集器"""
    
    def __init__(self):
        """初始化历史数据采集器"""
        self.logger = get_logger('history_collector')
        self.es_storage = ElasticsearchStorage()
        
    def get_stock_history_data(self, stock_code, start_date="2025-08-17", end_date="2025-09-17", 
                              period="daily", adjust="qfq", max_retries=3):
        """
        获取股票历史行情数据
        
        Args:
            stock_code (str): 股票代码，如 '000001'
            start_date (str): 开始日期，格式 'YYYY-MM-DD'
            end_date (str): 结束日期，格式 'YYYY-MM-DD'
            period (str): 周期，默认 'daily' (日线)
            adjust (str): 复权类型，默认 'qfq' (前复权)
            max_retries (int): 最大重试次数
            
        Returns:
            pd.DataFrame: 历史行情数据，如果失败返回None
        """
        for attempt in range(max_retries):
            try:
                self.logger.info(f"正在获取股票 {stock_code} 的历史数据 ({start_date} 到 {end_date})，第 {attempt + 1} 次尝试")
                
                # 调用akshare接口获取历史数据
                df = ak.stock_zh_a_hist(
                    symbol=stock_code,
                    period=period,
                    start_date=start_date.replace('-', ''),
                    end_date=end_date.replace('-', ''),
                    adjust=adjust
                )
                
                if df is not None and not df.empty:
                    self.logger.info(f"成功获取股票 {stock_code} 的历史数据，共 {len(df)} 条记录")
                    return df
                else:
                    self.logger.warning(f"股票 {stock_code} 的历史数据为空")
                    return None
                    
            except Exception as e:
                self.logger.error(f"获取股票 {stock_code} 历史数据失败 (第 {attempt + 1} 次): {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # 指数退避
                else:
                    self.logger.error(f"获取股票 {stock_code} 历史数据最终失败")
                    return None
        
        return None
    
    def process_history_data(self, df, stock_code, market_type):
        """
        处理历史行情数据，转换为标准格式
        
        Args:
            df (pd.DataFrame): 原始历史数据
            stock_code (str): 股票代码
            market_type (str): 市场类型
            
        Returns:
            list: 处理后的数据列表
        """
        if df is None or df.empty:
            return []
        
        processed_data = []
        prev_close = 0  # 第一条数据的昨收价设为0
        
        # 按日期排序，确保数据顺序正确
        df = df.sort_values('日期')
        
        for index, row in df.iterrows():
            try:
                # 数据转换和处理
                data_item = {
                    'date': row['日期'],
                    'stock_code': stock_code,
                    'market_type': market_type,
                    # 价格相关字段乘以100转为整数（以分为单位）
                    'open_price': int(float(row['开盘']) * 100) if pd.notna(row['开盘']) else 0,
                    'close_price': int(float(row['收盘']) * 100) if pd.notna(row['收盘']) else 0,
                    'high_price': int(float(row['最高']) * 100) if pd.notna(row['最高']) else 0,
                    'low_price': int(float(row['最低']) * 100) if pd.notna(row['最低']) else 0,
                    'prev_close_price': prev_close,
                    # 成交量和成交额乘以100
                    'volume': int(float(row['成交量']) * 100) if pd.notna(row['成交量']) else 0,
                    'amount': int(float(row['成交额']) * 100) if pd.notna(row['成交额']) else 0,
                    # 涨跌幅和涨跌额乘以100
                    'change_percent': int(float(row['涨跌幅']) * 100) if pd.notna(row['涨跌幅']) else 0,
                    'change_amount': int(float(row['涨跌额']) * 100) if pd.notna(row['涨跌额']) else 0,
                    # 换手率乘以100
                    'turnover_rate': int(float(row['换手率']) * 100) if pd.notna(row['换手率']) else 0,
                    'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'data_source': 'akshare_history'
                }
                
                processed_data.append(data_item)
                
                # 更新昨收价为当前收盘价，供下一条数据使用
                prev_close = data_item['close_price']
                
            except Exception as e:
                self.logger.error(f"处理股票 {stock_code} 第 {index} 行数据失败: {str(e)}")
                continue
        
        self.logger.info(f"股票 {stock_code} 历史数据处理完成，共处理 {len(processed_data)} 条记录")
        return processed_data
    
    def get_stock_codes_from_es(self):
        """
        从Elasticsearch的stock_basic_data索引中获取所有股票代码
        
        Returns:
            list: 股票代码和市场类型的列表，格式为 [{'stock_code': 'xxx', 'market_type': 'xxx'}, ...]
        """
        try:
            self.logger.info("正在从Elasticsearch获取股票代码列表")
            
            # 查询所有股票基础数据
            query = {
                "query": {
                    "match_all": {}
                },
                "_source": ["stock_code", "market_type"],
                "size": 10000  # 假设股票数量不超过10000
            }
            
            response = self.es_storage.es_client.search(
                index="stock_basic_data",
                body=query
            )
            
            stock_list = []
            for hit in response['hits']['hits']:
                source = hit['_source']
                stock_list.append({
                    'stock_code': source.get('stock_code', ''),
                    'market_type': source.get('market_type', '')
                })
            
            self.logger.info(f"成功获取 {len(stock_list)} 个股票代码")
            return stock_list
            
        except Exception as e:
            self.logger.error(f"从Elasticsearch获取股票代码失败: {str(e)}")
            return []
    
    def collect_all_history_data(self, start_date="2025-08-17", end_date="2025-09-17", 
                                batch_size=50, delay_between_requests=1):
        """
        采集所有股票的历史行情数据
        
        Args:
            start_date (str): 开始日期
            end_date (str): 结束日期
            batch_size (int): 批量处理大小
            delay_between_requests (float): 请求间隔时间（秒）
            
        Returns:
            dict: 采集结果统计
        """
        self.logger.info(f"开始采集历史行情数据 ({start_date} 到 {end_date})")
        
        # 获取股票代码列表
        stock_list = self.get_stock_codes_from_es()
        if not stock_list:
            self.logger.error("未能获取到股票代码列表，采集终止")
            return {'success': 0, 'failed': 0, 'total': 0}
        
        success_count = 0
        failed_count = 0
        total_count = len(stock_list)
        
        self.logger.info(f"共需要采集 {total_count} 个股票的历史数据")
        
        # 分批处理股票数据
        for i in range(0, total_count, batch_size):
            batch_stocks = stock_list[i:i + batch_size]
            batch_data = []
            
            self.logger.info(f"正在处理第 {i//batch_size + 1} 批数据 ({len(batch_stocks)} 个股票)")
            
            for stock_info in batch_stocks:
                stock_code = stock_info['stock_code']
                market_type = stock_info['market_type']
                
                try:
                    # 获取历史数据
                    df = self.get_stock_history_data(stock_code, start_date, end_date)
                    
                    if df is not None and not df.empty:
                        # 处理数据
                        processed_data = self.process_history_data(df, stock_code, market_type)
                        batch_data.extend(processed_data)
                        success_count += 1
                        self.logger.info(f"股票 {stock_code} 历史数据采集成功")
                    else:
                        failed_count += 1
                        self.logger.warning(f"股票 {stock_code} 历史数据采集失败或为空")
                    
                    # 请求间隔
                    time.sleep(delay_between_requests)
                    
                except Exception as e:
                    failed_count += 1
                    self.logger.error(f"股票 {stock_code} 历史数据采集异常: {str(e)}")
            
            # 批量保存数据到Elasticsearch
            if batch_data:
                try:
                    from core.data_storage.daily_storage import DailyDataStorage
                    daily_storage = DailyDataStorage()
                    save_result = daily_storage.save_daily_data(batch_data)
                    if save_result:
                        self.logger.info(f"第 {i//batch_size + 1} 批数据保存成功，共 {len(batch_data)} 条记录")
                    else:
                        self.logger.error(f"第 {i//batch_size + 1} 批数据保存失败")
                except Exception as e:
                    self.logger.error(f"第 {i//batch_size + 1} 批数据保存异常: {str(e)}")
        
        result = {
            'success': success_count,
            'failed': failed_count,
            'total': total_count
        }
        
        self.logger.info(f"历史数据采集完成: 成功 {success_count}, 失败 {failed_count}, 总计 {total_count}")
        return result
    
    def collect_history_data_by_codes(self, start_date, end_date, stock_codes, adjust_flag="qfq",
                                     batch_size=50, delay_between_requests=1):
        """
        按指定股票代码列表采集历史行情数据
        
        Args:
            start_date (str): 开始日期
            end_date (str): 结束日期
            stock_codes (list): 股票代码列表
            adjust_flag (str): 复权方式，'qfq'前复权，'hfq'后复权，''不复权
            batch_size (int): 批量处理大小
            delay_between_requests (float): 请求间隔时间（秒）
            
        Returns:
            dict: 采集结果统计
        """
        self.logger.info(f"开始按代码列表采集历史行情数据 ({start_date} 到 {end_date})，复权方式: {adjust_flag}")
        
        if not stock_codes:
            self.logger.error("股票代码列表为空，采集终止")
            return {'success': 0, 'failed': 0, 'total': 0}
        
        success_count = 0
        failed_count = 0
        total_count = len(stock_codes)
        
        self.logger.info(f"共需要采集 {total_count} 个股票的历史数据")
        
        # 分批处理股票数据
        for i in range(0, total_count, batch_size):
            batch_codes = stock_codes[i:i + batch_size]
            batch_data = []
            
            self.logger.info(f"正在处理第 {i//batch_size + 1} 批数据 ({len(batch_codes)} 个股票)")
            
            for stock_code in batch_codes:
                try:
                    # 获取历史数据，使用指定的复权方式
                    df = self.get_stock_history_data(stock_code, start_date, end_date, adjust=adjust_flag)
                    
                    if df is not None and not df.empty:
                        # 确定市场类型（简单判断）
                        market_type = "SZ" if stock_code.startswith(('000', '002', '300')) else "SH"
                        
                        # 处理数据
                        processed_data = self.process_history_data(df, stock_code, market_type)
                        batch_data.extend(processed_data)
                        success_count += 1
                        self.logger.info(f"股票 {stock_code} 历史数据采集成功")
                    else:
                        failed_count += 1
                        self.logger.warning(f"股票 {stock_code} 历史数据采集失败或为空")
                    
                    # 请求间隔
                    time.sleep(delay_between_requests)
                    
                except Exception as e:
                    failed_count += 1
                    self.logger.error(f"股票 {stock_code} 历史数据采集异常: {str(e)}")
            
            # 批量保存数据到Elasticsearch
            if batch_data:
                try:
                    from core.data_storage.daily_storage import DailyDataStorage
                    daily_storage = DailyDataStorage()
                    save_result = daily_storage.save_daily_data(batch_data)
                    if save_result:
                        self.logger.info(f"第 {i//batch_size + 1} 批数据保存成功，共 {len(batch_data)} 条记录")
                    else:
                        self.logger.error(f"第 {i//batch_size + 1} 批数据保存失败")
                except Exception as e:
                    self.logger.error(f"第 {i//batch_size + 1} 批数据保存异常: {str(e)}")
        
        result = {
            'success': success_count,
            'failed': failed_count,
            'total': total_count
        }
        
        self.logger.info(f"按代码列表历史数据采集完成: 成功 {success_count}, 失败 {failed_count}, 总计 {total_count}")
        return result


if __name__ == "__main__":
    """测试历史数据采集器"""
    collector = HistoryDataCollector()
    
    # 测试单个股票数据获取
    test_stock = "000001"
    print(f"测试获取股票 {test_stock} 的历史数据...")
    
    df = collector.get_stock_history_data(test_stock)
    if df is not None:
        print(f"获取成功，数据量: {len(df)}")
        print("数据示例:")
        print(df.head())
        
        # 测试数据处理
        processed = collector.process_history_data(df, test_stock, "深圳A股")
        print(f"\n处理后数据量: {len(processed)}")
        if processed:
            print("处理后数据示例:")
            for i, item in enumerate(processed[:3]):
                print(f"{i+1}. {item}")
    else:
        print("获取失败")