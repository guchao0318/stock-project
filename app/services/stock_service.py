#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票数据服务

实现股票数据相关的业务逻辑
"""

import sys
import os
import time
from typing import List, Optional, Dict, Any
from datetime import datetime

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from scheduler.task_scheduler import StockDataScheduler
from core.data_storage.es_storage import ElasticsearchStorage
from app.models.request_models import HistoryCollectRequest, StockCodesRequest
from utils.logger_config import setup_logger


class StockService:
    """股票数据服务类"""
    
    def __init__(self):
        """初始化服务"""
        self.logger = setup_logger("stock_service")
        self.scheduler = None
        self.es_storage = None
    
    def _get_scheduler(self) -> StockDataScheduler:
        """获取调度器实例（懒加载）"""
        if self.scheduler is None:
            self.scheduler = StockDataScheduler()
        return self.scheduler
    
    def _get_es_storage(self) -> ElasticsearchStorage:
        """获取ES存储实例（懒加载）"""
        if self.es_storage is None:
            self.es_storage = ElasticsearchStorage()
        return self.es_storage
    
    def collect_history_data(self, request: HistoryCollectRequest) -> Dict[str, Any]:
        """采集历史数据
        
        Args:
            request: 历史数据采集请求
            
        Returns:
            Dict: 采集结果
            
        Raises:
            Exception: 采集过程中的异常
        """
        try:
            start_time = time.time()
            
            self.logger.info(
                f"开始采集历史数据: {request.start_date} 到 {request.end_date}, "
                f"复权方式: {request.adjust_flag}"
            )
            
            # 处理股票代码
            stock_codes = request.stock_codes
            if stock_codes:
                self.logger.info(f"指定股票代码: {stock_codes}")
            else:
                self.logger.info("未指定股票代码，将从ES获取全部股票代码")
            
            # 获取调度器并执行采集
            scheduler = self._get_scheduler()
            result = scheduler.setup_history_schedule_once(
                start_date=request.start_date,
                end_date=request.end_date,
                stock_codes=stock_codes,
                adjust_flag=request.adjust_flag,
                execute_immediately=True
            )
            
            # 计算执行时间
            execution_time = time.time() - start_time
            
            # 添加额外信息
            result["start_date"] = request.start_date
            result["end_date"] = request.end_date
            result["adjust_flag"] = request.adjust_flag
            result["execution_time"] = round(execution_time, 2)
            
            if stock_codes:
                result["stock_codes_count"] = len(stock_codes)
            
            # 记录结果
            if result.get("total", 0) > 0:
                success_rate = (result.get("success", 0) / result["total"]) * 100
                self.logger.info(
                    f"历史数据采集完成: 成功率 {success_rate:.1f}% "
                    f"({result.get('success', 0)}/{result['total']}), "
                    f"耗时 {execution_time:.2f}秒"
                )
            else:
                self.logger.warning("历史数据采集完成，但没有处理任何数据")
            
            return result
            
        except Exception as e:
            self.logger.error(f"历史数据采集失败: {str(e)}")
            raise Exception(f"历史数据采集失败: {str(e)}")
    
    def get_stock_codes(self, request: StockCodesRequest) -> List[str]:
        """获取股票代码列表
        
        Args:
            request: 获取股票代码请求
            
        Returns:
            List[str]: 股票代码列表
            
        Raises:
            Exception: 获取过程中的异常
        """
        try:
            self.logger.info(f"开始获取股票代码列表，限制数量: {request.limit}")
            
            # 获取ES存储实例
            es_storage = self._get_es_storage()
            
            # 从ES获取股票代码
            codes = es_storage.get_all_stock_codes(limit=request.limit)
            
            if codes:
                self.logger.info(f"成功获取 {len(codes)} 个股票代码")
            else:
                self.logger.warning("未获取到任何股票代码")
                codes = []
            
            return codes
            
        except Exception as e:
            self.logger.error(f"获取股票代码失败: {str(e)}")
            raise Exception(f"获取股票代码失败: {str(e)}")
    
    def health_check(self) -> Dict[str, Any]:
        """健康检查
        
        Returns:
            Dict: 健康状态信息
        """
        try:
            # 检查ES连接
            es_storage = self._get_es_storage()
            es_healthy = es_storage.check_connection()
            
            # 检查调度器
            scheduler_healthy = True
            try:
                self._get_scheduler()
            except Exception:
                scheduler_healthy = False
            
            status = "healthy" if (es_healthy and scheduler_healthy) else "unhealthy"
            
            return {
                "status": status,
                "timestamp": datetime.now().isoformat(),
                "components": {
                    "elasticsearch": "healthy" if es_healthy else "unhealthy",
                    "scheduler": "healthy" if scheduler_healthy else "unhealthy"
                }
            }
            
        except Exception as e:
            self.logger.error(f"健康检查失败: {str(e)}")
            return {
                "status": "unhealthy",
                "timestamp": datetime.now().isoformat(),
                "error": str(e)
            }


# 全局服务实例（单例模式）
_stock_service_instance = None


def get_stock_service() -> StockService:
    """获取股票服务实例（单例模式）"""
    global _stock_service_instance
    if _stock_service_instance is None:
        _stock_service_instance = StockService()
    return _stock_service_instance