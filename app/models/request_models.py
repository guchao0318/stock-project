#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
请求数据模型

定义API请求的数据传输对象和验证逻辑
"""

from datetime import datetime
from typing import List, Optional
from dataclasses import dataclass
import re


@dataclass
class HistoryCollectRequest:
    """历史数据采集请求模型"""
    start_date: str
    end_date: str
    stock_codes: Optional[List[str]] = None
    adjust_flag: str = "qfq"
    
    def __post_init__(self):
        """初始化后验证"""
        self.validate()
    
    def validate(self):
        """验证请求参数"""
        # 验证日期格式
        if not self._is_valid_date(self.start_date):
            raise ValueError("start_date格式错误，应为YYYY-MM-DD")
        
        if not self._is_valid_date(self.end_date):
            raise ValueError("end_date格式错误，应为YYYY-MM-DD")
        
        # 验证日期逻辑
        start = datetime.strptime(self.start_date, "%Y-%m-%d")
        end = datetime.strptime(self.end_date, "%Y-%m-%d")
        
        if start > end:
            raise ValueError("start_date不能晚于end_date")
        
        # 验证复权方式
        if self.adjust_flag not in ["qfq", "hfq", "none"]:
            raise ValueError("adjust_flag必须是qfq、hfq或none之一")
        
        # 验证股票代码
        if self.stock_codes:
            for code in self.stock_codes:
                if not self._is_valid_stock_code(code):
                    raise ValueError(f"股票代码格式错误: {code}")
    
    @staticmethod
    def _is_valid_date(date_str: str) -> bool:
        """验证日期格式"""
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
            return True
        except ValueError:
            return False
    
    @staticmethod
    def _is_valid_stock_code(code: str) -> bool:
        """验证股票代码格式"""
        # 股票代码应为6位数字
        return bool(re.match(r'^\d{6}$', code))


@dataclass
class StockCodesRequest:
    """获取股票代码请求模型"""
    limit: int = 1000
    
    def __post_init__(self):
        """初始化后验证"""
        self.validate()
    
    def validate(self):
        """验证请求参数"""
        if not isinstance(self.limit, int) or self.limit <= 0:
            raise ValueError("limit必须是正整数")
        
        if self.limit > 10000:
            raise ValueError("limit不能超过10000")


class RequestValidator:
    """请求验证器"""
    
    @staticmethod
    def validate_history_collect_data(data: dict) -> HistoryCollectRequest:
        """验证历史数据采集请求"""
        if not isinstance(data, dict):
            raise ValueError("请求数据必须是JSON对象")
        
        # 检查必需字段
        required_fields = ["start_date", "end_date"]
        for field in required_fields:
            if field not in data:
                raise ValueError(f"缺少必需字段: {field}")
        
        # 创建请求对象
        return HistoryCollectRequest(
            start_date=data["start_date"],
            end_date=data["end_date"],
            stock_codes=data.get("stock_codes"),
            adjust_flag=data.get("adjust_flag", "qfq")
        )
    
    @staticmethod
    def validate_stock_codes_params(args: dict) -> StockCodesRequest:
        """验证获取股票代码请求"""
        limit = args.get("limit", 1000)
        
        # 转换为整数
        try:
            limit = int(limit)
        except (ValueError, TypeError):
            raise ValueError("limit必须是整数")
        
        return StockCodesRequest(limit=limit)