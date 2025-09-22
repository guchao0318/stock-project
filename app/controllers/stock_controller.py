#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票数据控制器

处理股票数据相关的HTTP请求和响应
"""

from flask import Blueprint, request, jsonify
from functools import wraps
import traceback

from app.models.request_models import RequestValidator
from app.models.response_models import ResponseBuilder
from app.services.stock_service import get_stock_service
from utils.logger_config import setup_logger


# 创建蓝图
stock_bp = Blueprint('stock', __name__, url_prefix='/api/v1')
logger = setup_logger("stock_controller")


def handle_exceptions(f):
    """异常处理装饰器"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except ValueError as e:
            # 参数验证错误
            logger.warning(f"参数验证错误: {str(e)}")
            response = ResponseBuilder.validation_error(str(e))
            return response.to_json_response(400)
        except Exception as e:
            # 服务器内部错误
            logger.error(f"服务器错误: {str(e)}")
            logger.error(f"错误堆栈: {traceback.format_exc()}")
            response = ResponseBuilder.server_error(str(e))
            return response.to_json_response(500)
    return decorated_function


@stock_bp.route('/health', methods=['GET'])
@handle_exceptions
def health_check():
    """健康检查接口"""
    logger.info("收到健康检查请求")
    
    # 获取服务实例
    stock_service = get_stock_service()
    
    # 执行健康检查
    health_data = stock_service.health_check()
    
    # 构建响应
    response = ResponseBuilder.health_response()
    response_data = response.to_dict()
    response_data.update(health_data)
    
    status_code = 200 if health_data.get("status") == "healthy" else 503
    
    return jsonify(response_data), status_code


@stock_bp.route('/stocks/codes', methods=['GET'])
@handle_exceptions
def get_stock_codes():
    """获取股票代码列表接口"""
    logger.info("收到获取股票代码请求")
    
    # 验证请求参数
    request_obj = RequestValidator.validate_stock_codes_params(request.args)
    
    # 获取服务实例
    stock_service = get_stock_service()
    
    # 执行业务逻辑
    codes = stock_service.get_stock_codes(request_obj)
    
    # 构建响应
    response = ResponseBuilder.stock_codes_response(codes, request_obj.limit)
    
    logger.info(f"成功返回 {len(codes)} 个股票代码")
    return response.to_json_response(200)


@stock_bp.route('/history/collect', methods=['POST'])
@handle_exceptions
def collect_history_data():
    """采集历史数据接口"""
    logger.info("收到历史数据采集请求")
    
    # 验证请求数据
    if not request.is_json:
        raise ValueError("请求必须是JSON格式")
    
    request_data = request.get_json()
    if not request_data:
        raise ValueError("请求数据不能为空")
    
    # 验证请求参数
    request_obj = RequestValidator.validate_history_collect_data(request_data)
    
    logger.info(
        f"历史数据采集参数: {request_obj.start_date} 到 {request_obj.end_date}, "
        f"复权方式: {request_obj.adjust_flag}, "
        f"股票数量: {len(request_obj.stock_codes) if request_obj.stock_codes else '全部'}"
    )
    
    # 获取服务实例
    stock_service = get_stock_service()
    
    # 执行业务逻辑
    result = stock_service.collect_history_data(request_obj)
    
    # 构建响应
    response = ResponseBuilder.history_collect_response(result)
    
    logger.info(
        f"历史数据采集完成: 总数 {result.get('total', 0)}, "
        f"成功 {result.get('success', 0)}, 失败 {result.get('failed', 0)}"
    )
    
    return response.to_json_response(200)


@stock_bp.route('/history/collect/status/<task_id>', methods=['GET'])
@handle_exceptions
def get_collect_status(task_id):
    """获取采集任务状态接口"""
    logger.info(f"收到获取任务状态请求: {task_id}")
    
    # 这里可以扩展为真正的任务状态查询
    # 目前返回简单的响应
    response = ResponseBuilder.success(
        data={"task_id": task_id, "status": "completed"},
        message="任务状态查询成功"
    )
    
    return response.to_json_response(200)


# 错误处理器
@stock_bp.errorhandler(404)
def not_found(error):
    """404错误处理"""
    response = ResponseBuilder.error("接口不存在", "请检查请求路径")
    return response.to_json_response(404)


@stock_bp.errorhandler(405)
def method_not_allowed(error):
    """405错误处理"""
    response = ResponseBuilder.error("请求方法不允许", "请检查HTTP方法")
    return response.to_json_response(405)


@stock_bp.errorhandler(400)
def bad_request(error):
    """400错误处理"""
    response = ResponseBuilder.error("请求格式错误", str(error))
    return response.to_json_response(400)