#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
定时任务控制器

处理定时任务相关的HTTP请求和响应
"""

from flask import Blueprint, request, jsonify
from functools import wraps
import traceback

from app.models.response_models import ResponseBuilder
from utils.logger_config import setup_logger
from flask import current_app


# 创建蓝图
scheduler_bp = Blueprint('scheduler', __name__, url_prefix='/api/v1/scheduler')
logger = setup_logger("scheduler_controller")


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


@scheduler_bp.route('/status', methods=['GET'])
@handle_exceptions
def get_scheduler_status():
    """获取调度器状态"""
    logger.info("收到获取调度器状态请求")
    
    scheduler_service = current_app.scheduler_service
    status_data = scheduler_service.get_scheduler_status()
    
    response = ResponseBuilder.success(
        data=status_data,
        message="获取调度器状态成功"
    )
    
    return response.to_json_response(200)


@scheduler_bp.route('/start', methods=['POST'], endpoint="api_start_scheduler")
@handle_exceptions
def start_scheduler():
    """启动调度器"""
    logger.info("收到启动调度器请求")
    
    scheduler_service = current_app.scheduler_service
    result = scheduler_service.start_scheduler()
    
    if result['status'] == 'started':
        response = ResponseBuilder.success(message=result['message'])
        return response.to_json_response(200)
    elif result['status'] == 'already_running':
        response = ResponseBuilder.success(message=result['message'])
        return response.to_json_response(200)
    else:
        response = ResponseBuilder.error("启动调度器失败", result['message'])
        return response.to_json_response(500)


@scheduler_bp.route('/stop', methods=['POST'], endpoint="api_stop_scheduler")
@handle_exceptions
def stop_scheduler():
    """停止调度器"""
    logger.info("收到停止调度器请求")
    
    scheduler_service = current_app.scheduler_service
    result = scheduler_service.stop_scheduler()
    
    if result['status'] == 'stopped':
        response = ResponseBuilder.success(message=result['message'])
        return response.to_json_response(200)
    elif result['status'] == 'not_running':
        response = ResponseBuilder.success(message=result['message'])
        return response.to_json_response(200)
    else:
        response = ResponseBuilder.error("停止调度器失败", result['message'])
        return response.to_json_response(500)


@scheduler_bp.route('/tasks', methods=['GET'])
@handle_exceptions
def list_tasks():
    """列出所有任务"""
    logger.info("收到列出任务请求")
    
    scheduler_service = current_app.scheduler_service
    tasks = scheduler_service.list_tasks()
    
    response = ResponseBuilder.success(
        data={"tasks": tasks, "count": len(tasks)},
        message="获取任务列表成功"
    )
    
    return response.to_json_response(200)


@scheduler_bp.route('/tasks/<task_id>', methods=['GET'])
@handle_exceptions
def get_task_status(task_id):
    """获取任务状态"""
    logger.info(f"收到获取任务状态请求: {task_id}")
    
    scheduler_service = current_app.scheduler_service
    task_status = scheduler_service.get_task_status(task_id)
    
    if task_status:
        response = ResponseBuilder.success(
            data=task_status,
            message="获取任务状态成功"
        )
        return response.to_json_response(200)
    else:
        response = ResponseBuilder.error(
            "任务不存在",
            f"未找到任务ID: {task_id}"
        )
        return response.to_json_response(404)


@scheduler_bp.route('/tasks/<task_id>', methods=['DELETE'])
@handle_exceptions
def remove_task(task_id):
    """删除任务"""
    logger.info(f"收到删除任务请求: {task_id}")
    
    scheduler_service = current_app.scheduler_service
    result = scheduler_service.remove_job(task_id)
    
    if result:
        response = ResponseBuilder.success(
            message="任务删除成功"
        )
        return response.to_json_response(200)
    else:
        response = ResponseBuilder.error(
            "任务删除失败",
            f"未找到任务ID: {task_id}"
        )
        return response.to_json_response(404)


@scheduler_bp.route('/tasks/history', methods=['POST'])
@handle_exceptions
def add_history_task():
    """添加历史数据采集任务"""
    logger.info("收到添加历史数据采集任务请求")
    
    if not request.is_json:
        raise ValueError("请求必须是JSON格式")
    
    data = request.get_json()
    if not data:
        raise ValueError("请求数据不能为空")
    
    # 验证必需字段
    required_fields = ["task_id", "schedule_time", "start_date", "end_date"]
    for field in required_fields:
        if field not in data:
            raise ValueError(f"缺少必需字段: {field}")
    
    scheduler_service = current_app.scheduler_service
    success = scheduler_service.add_history_job(
        job_id=data["task_id"],
        schedule_time=data["schedule_time"],
        start_date=data["start_date"],
        end_date=data["end_date"],
        stock_codes=data.get("stock_codes"),
        adjust_flag=data.get("adjust_flag", "qfq")
    )
    
    if success:
        response = ResponseBuilder.success(
            message="历史数据采集任务添加成功"
        )
        return response.to_json_response(201)
    else:
        response = ResponseBuilder.error(
            "任务添加失败",
            "可能是任务ID已存在或参数错误"
        )
        return response.to_json_response(400)


@scheduler_bp.route('/tasks/realtime', methods=['POST'])
@handle_exceptions
def add_realtime_task():
    """添加实时数据采集任务"""
    logger.info("收到添加实时数据采集任务请求")
    
    if not request.is_json:
        raise ValueError("请求必须是JSON格式")
    
    data = request.get_json()
    if not data:
        raise ValueError("请求数据不能为空")
    
    # 验证必需字段
    if "task_id" not in data:
        raise ValueError("缺少必需字段: task_id")
    
    scheduler_service = current_app.scheduler_service
    success = scheduler_service.add_realtime_job(
        job_id=data["task_id"],
        interval_seconds=data.get("interval_seconds", 300)
    )
    
    if success:
        response = ResponseBuilder.success(
            message="实时数据采集任务添加成功"
        )
        return response.to_json_response(201)
    else:
        response = ResponseBuilder.error(
            "任务添加失败",
            "可能是任务ID已存在或参数错误"
        )
        return response.to_json_response(400)


# 错误处理器
@scheduler_bp.errorhandler(404)
def not_found(error):
    """404错误处理"""
    response = ResponseBuilder.error("接口不存在", "请检查请求路径")
    return response.to_json_response(404)


@scheduler_bp.errorhandler(405)
def method_not_allowed(error):
    """405错误处理"""
    response = ResponseBuilder.error("请求方法不允许", "请检查HTTP方法")
    return response.to_json_response(405)