#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flask应用主文件

集成MVC架构的股票数据API服务
"""

from flask import Flask, jsonify
from flask_cors import CORS
import os
import sys

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.controllers.stock_controller import stock_bp
from app.controllers.scheduler_controller import scheduler_bp
from app.models.response_models import ResponseBuilder
from app.services.scheduler_service import SchedulerService
from app.services.scheduler_service import get_scheduler_service
from app.config import get_config
from utils.logger_config import setup_logger


def create_app(config_name=None):
    """创建Flask应用实例"""
    app = Flask(__name__)
    
    # 加载配置
    config_class = get_config(config_name)
    app.config.from_object(config_class)
    config_class.init_app(app)
    
    # 启用CORS
    CORS(app, resources={
        r"/api/*": {
            "origins": "*",
            "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
            "allow_headers": ["Content-Type", "Authorization"]
        }
    })
    
    # 初始化调度器服务
    scheduler_service = SchedulerService()
    scheduler_service.init_app(app)
    
    # 注册蓝图
    app.register_blueprint(stock_bp)
    app.register_blueprint(scheduler_bp)
    
    # 设置日志
    logger = setup_logger("flask_app")
    
    # 全局错误处理器
    @app.errorhandler(404)
    def not_found(error):
        """404错误处理"""
        response = ResponseBuilder.error("接口不存在", "请检查请求路径")
        return response.to_json_response(404)
    
    @app.errorhandler(405)
    def method_not_allowed(error):
        """405错误处理"""
        response = ResponseBuilder.error("请求方法不允许", "请检查HTTP方法")
        return response.to_json_response(405)
    
    @app.errorhandler(500)
    def internal_error(error):
        """500错误处理"""
        logger.error(f"服务器内部错误: {str(error)}")
        response = ResponseBuilder.server_error("服务器内部错误")
        return response.to_json_response(500)
    
    # 根路径
    @app.route('/')
    def index():
        """API根路径"""
        response = ResponseBuilder.success(
            data={
                "name": "股票数据API服务",
                "version": "2.0.0",
                "description": "基于Flask框架的股票数据采集和查询API",
                "endpoints": {
                    "stock": "/api/v1/stock",
                    "scheduler": "/api/v1/scheduler"
                }
            },
            message="欢迎使用股票数据API服务"
        )
        return response.to_json_response(200)
    
    # 健康检查
    @app.route('/health')
    def health_check():
        """健康检查接口"""
        response = ResponseBuilder.success(
            data={
                "status": "healthy",
                "service": "stock-api",
                "version": "2.0.0"
            },
            message="服务运行正常"
        )
        return response.to_json_response(200)
    
    # 应用启动时的初始化
    @app.before_request
    def initialize_app():
        """应用首次请求前的初始化"""
        logger.info("初始化Flask应用...")
        
        # 初始化调度器服务（但不自动启动）
        scheduler_service = get_scheduler_service()
        logger.info("调度器服务已初始化")
        
        # 检查是否需要自动启动调度器
        auto_start_scheduler = app.config.get('AUTO_START_SCHEDULER', False)
        if auto_start_scheduler:
            scheduler_service.start()
            logger.info("调度器已自动启动")
    
    # 应用关闭时的清理
    @app.teardown_appcontext
    def cleanup(error):
        """应用上下文清理"""
        if error:
            logger.error(f"应用上下文错误: {str(error)}")
    
    logger.info("Flask应用创建完成")
    return app


def start_api_server(host='0.0.0.0', port=5000, debug=False, config_name=None):
    """启动API服务器"""
    logger = setup_logger("api_server")
    
    try:
        logger.info(f"启动股票数据API服务器...")
        logger.info(f"服务地址: http://{host}:{port}")
        logger.info(f"调试模式: {debug}")
        logger.info(f"配置环境: {config_name or 'default'}")
        
        # 创建应用实例
        app = create_app(config_name)
        
        # 启动服务器
        app.run(
            host=host,
            port=port,
            debug=debug,
            threaded=True,  # 启用多线程
            use_reloader=False  # 禁用自动重载（避免调度器重复启动）
        )
        
        return True
        
    except Exception as e:
        logger.error(f"启动API服务器失败: {str(e)}")
        return False


if __name__ == '__main__':
    # 直接运行时启动开发服务器
    import argparse
    
    parser = argparse.ArgumentParser(description='股票数据API服务器')
    parser.add_argument('--host', default='0.0.0.0', help='服务器地址')
    parser.add_argument('--port', type=int, default=5001, help='服务器端口')
    parser.add_argument('--debug', action='store_true', help='启用调试模式')
    
    args = parser.parse_args()
    
    start_api_server(
        host=args.host,
        port=args.port,
        debug=args.debug
    )