#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
响应数据模型

定义API响应的标准格式和数据传输对象
"""

from datetime import datetime
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, asdict
from flask import jsonify


@dataclass
class ApiResponse:
    """标准API响应模型"""
    success: bool
    data: Optional[Any] = None
    message: str = ""
    error: Optional[str] = None
    details: Optional[str] = None
    timestamp: Optional[str] = None
    
    def __post_init__(self):
        """初始化后处理"""
        if self.timestamp is None:
            self.timestamp = datetime.now().isoformat()
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        result = asdict(self)
        # 移除None值
        return {k: v for k, v in result.items() if v is not None}
    
    def to_json_response(self, status_code: int = 200):
        """转换为Flask JSON响应"""
        response = jsonify(self.to_dict())
        response.status_code = status_code
        return response


@dataclass
class HealthResponse:
    """健康检查响应模型"""
    status: str
    timestamp: str
    version: str = "1.0.0"
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return asdict(self)


@dataclass
class HistoryCollectResult:
    """历史数据采集结果模型"""
    total: int
    success: int
    failed: int
    start_date: str
    end_date: str
    adjust_flag: str
    stock_codes_count: Optional[int] = None
    execution_time: Optional[float] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        result = asdict(self)
        # 移除None值
        return {k: v for k, v in result.items() if v is not None}


@dataclass
class StockCodesResult:
    """股票代码列表结果模型"""
    codes: List[str]
    total: int
    limit: int
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "codes": self.codes,
            "total": self.total,
            "limit": self.limit
        }


class ResponseBuilder:
    """响应构建器"""
    
    @staticmethod
    def success(data: Any = None, message: str = "操作成功") -> ApiResponse:
        """构建成功响应"""
        return ApiResponse(
            success=True,
            data=data,
            message=message
        )
    
    @staticmethod
    def error(error: str, details: str = None, status_code: int = 400) -> ApiResponse:
        """构建错误响应"""
        return ApiResponse(
            success=False,
            error=error,
            details=details
        )
    
    @staticmethod
    def validation_error(message: str) -> ApiResponse:
        """构建验证错误响应"""
        return ApiResponse(
            success=False,
            error="参数验证失败",
            details=message
        )
    
    @staticmethod
    def server_error(message: str = "服务器内部错误") -> ApiResponse:
        """构建服务器错误响应"""
        return ApiResponse(
            success=False,
            error="服务器错误",
            details=message
        )
    
    @staticmethod
    def health_response() -> HealthResponse:
        """构建健康检查响应"""
        return HealthResponse(
            status="healthy",
            timestamp=datetime.now().isoformat()
        )
    
    @staticmethod
    def history_collect_response(result: Dict[str, Any]) -> ApiResponse:
        """构建历史数据采集响应"""
        collect_result = HistoryCollectResult(
            total=result.get("total", 0),
            success=result.get("success", 0),
            failed=result.get("failed", 0),
            start_date=result.get("start_date", ""),
            end_date=result.get("end_date", ""),
            adjust_flag=result.get("adjust_flag", "qfq"),
            stock_codes_count=result.get("stock_codes_count"),
            execution_time=result.get("execution_time")
        )
        
        return ResponseBuilder.success(
            data=collect_result.to_dict(),
            message="历史数据采集完成"
        )
    
    @staticmethod
    def stock_codes_response(codes: List[str], limit: int) -> ApiResponse:
        """构建股票代码列表响应"""
        codes_result = StockCodesResult(
            codes=codes,
            total=len(codes),
            limit=limit
        )
        
        return ResponseBuilder.success(
            data=codes_result.to_dict(),
            message="获取股票代码成功"
        )