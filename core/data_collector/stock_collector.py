# 股票数据采集模块
import akshare as ak
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional
from loguru import logger
import time

from config.settings import MARKET_CONFIG, FIELD_MAPPING, DATA_COLLECTION_CONFIG


class StockDataCollector:
    """股票数据采集器"""
    
    def __init__(self):
        self.retry_times = DATA_COLLECTION_CONFIG['retry_times']
        self.retry_delay = DATA_COLLECTION_CONFIG['retry_delay']
        
    def _retry_request(self, func, *args, **kwargs) -> Optional[pd.DataFrame]:
        """重试机制"""
        for attempt in range(self.retry_times):
            try:
                result = func(*args, **kwargs)
                if result is not None and not result.empty:
                    return result
                else:
                    logger.warning(f"第{attempt + 1}次尝试获取数据为空")
            except Exception as e:
                logger.error(f"第{attempt + 1}次尝试失败: {str(e)}")
                if attempt < self.retry_times - 1:
                    time.sleep(self.retry_delay)
                else:
                    logger.error(f"重试{self.retry_times}次后仍然失败")
        return None
    
    def get_sz_a_stock_data(self) -> Optional[pd.DataFrame]:
        """获取深圳A股数据"""
        logger.info("开始获取深圳A股数据")
        try:
            data = self._retry_request(ak.stock_sz_a_spot_em)
            if data is not None:
                # 添加市场类型标识
                data['market_type'] = MARKET_CONFIG['sz_market']
                data['data_source'] = 'akshare_sz_a_spot_em'
                logger.info(f"成功获取深圳A股数据，共{len(data)}条记录")
                return data
            else:
                logger.error("获取深圳A股数据失败")
                return None
        except Exception as e:
            logger.error(f"获取深圳A股数据异常: {str(e)}")
            return None
    
    def get_sh_a_stock_data(self) -> Optional[pd.DataFrame]:
        """获取上海A股数据"""
        logger.info("开始获取上海A股数据")
        try:
            data = self._retry_request(ak.stock_sh_a_spot_em)
            if data is not None:
                # 添加市场类型标识
                data['market_type'] = MARKET_CONFIG['sh_market']
                data['data_source'] = 'akshare_sh_a_spot_em'
                logger.info(f"成功获取上海A股数据，共{len(data)}条记录")
                return data
            else:
                logger.error("获取上海A股数据失败")
                return None
        except Exception as e:
            logger.error(f"获取上海A股数据异常: {str(e)}")
            return None
    
    def _clean_and_transform_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """清洗和转换数据"""
        if data is None or data.empty:
            return pd.DataFrame()
        
        try:
            # 重命名列名
            data = data.rename(columns=FIELD_MAPPING)
            
            # 添加更新时间
            data['update_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # 数据类型转换
            numeric_columns = [
                'current_price', 'change_percent', 'change_amount', 'volume',
                'turnover', 'amplitude', 'highest_price', 'lowest_price',
                'opening_price', 'previous_close', 'volume_ratio', 'pe_ratio',
                'pb_ratio', 'total_market_value', 'circulating_market_value'
            ]
            
            for col in numeric_columns:
                if col in data.columns:
                    data[col] = pd.to_numeric(data[col], errors='coerce')
            
            # 处理缺失值
            data = data.fillna(0)
            
            # 确保股票代码为字符串类型
            if 'stock_code' in data.columns:
                data['stock_code'] = data['stock_code'].astype(str)
            
            logger.info(f"数据清洗完成，处理{len(data)}条记录")
            return data
            
        except Exception as e:
            logger.error(f"数据清洗异常: {str(e)}")
            return pd.DataFrame()
    
    def collect_all_stock_data(self) -> List[Dict]:
        """采集所有股票数据"""
        logger.info("开始采集所有股票数据")
        all_data = []
        
        # 获取深圳A股数据
        sz_data = self.get_sz_a_stock_data()
        if sz_data is not None and not sz_data.empty:
            sz_cleaned = self._clean_and_transform_data(sz_data)
            if not sz_cleaned.empty:
                all_data.extend(sz_cleaned.to_dict('records'))
        
        # 获取上海A股数据
        sh_data = self.get_sh_a_stock_data()
        if sh_data is not None and not sh_data.empty:
            sh_cleaned = self._clean_and_transform_data(sh_data)
            if not sh_cleaned.empty:
                all_data.extend(sh_cleaned.to_dict('records'))
        
        logger.info(f"数据采集完成，共获取{len(all_data)}条股票数据")
        return all_data
    
    def get_stock_basic_info(self) -> List[Dict]:
        """获取股票基础信息（代码、名称、市场类型）"""
        logger.info("开始获取股票基础信息")
        basic_info = []
        
        try:
            # 获取所有股票数据
            all_data = self.collect_all_stock_data()
            
            # 提取基础信息
            for stock in all_data:
                if 'stock_code' in stock and 'stock_name' in stock:
                    basic_info.append({
                        'stock_code': stock.get('stock_code'),
                        'stock_name': stock.get('stock_name'),
                        'market_type': stock.get('market_type'),
                        'current_price': stock.get('current_price', 0),
                        'change_percent': stock.get('change_percent', 0),
                        'change_amount': stock.get('change_amount', 0),
                        'volume': stock.get('volume', 0),
                        'turnover': stock.get('turnover', 0),
                        'amplitude': stock.get('amplitude', 0),
                        'highest_price': stock.get('highest_price', 0),
                        'lowest_price': stock.get('lowest_price', 0),
                        'opening_price': stock.get('opening_price', 0),
                        'previous_close': stock.get('previous_close', 0),
                        'volume_ratio': stock.get('volume_ratio', 0),
                        'pe_ratio': stock.get('pe_ratio', 0),
                        'pb_ratio': stock.get('pb_ratio', 0),
                        'total_market_value': stock.get('total_market_value', 0),
                        'circulating_market_value': stock.get('circulating_market_value', 0),
                        'update_time': stock.get('update_time'),
                        'data_source': stock.get('data_source')
                    })
            
            logger.info(f"获取股票基础信息完成，共{len(basic_info)}条记录")
            return basic_info
            
        except Exception as e:
            logger.error(f"获取股票基础信息异常: {str(e)}")
            return []


if __name__ == "__main__":
    # 测试代码
    collector = StockDataCollector()
    data = collector.get_stock_basic_info()
    print(f"获取到{len(data)}条股票数据")
    if data:
        print("示例数据:")
        for i, stock in enumerate(data[:3]):
            print(f"{i+1}. {stock}")