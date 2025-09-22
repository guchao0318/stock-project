#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flask API服务器启动脚本

专门用于启动股票数据API服务器
"""

import sys
import os
import argparse
import signal

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.app import start_api_server
from app.services.scheduler_service import get_scheduler_service
from utils.logger_config import setup_logger


def signal_handler(signum, frame):
    """信号处理器"""
    logger = setup_logger("start_api")
    logger.info(f"接收到信号 {signum}，正在停止API服务器...")
    
    # 停止调度器
    try:
        scheduler_service = get_scheduler_service()
        scheduler_service.stop()
        logger.info("调度器已停止")
    except Exception as e:
        logger.error(f"停止调度器失败: {str(e)}")
    
    logger.info("API服务器已停止")
    sys.exit(0)


def main():
    """主函数"""
    # 设置命令行参数解析
    parser = argparse.ArgumentParser(
        description='股票数据API服务器启动脚本',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""使用示例:
  python start_api.py                    # 使用默认配置启动
  python start_api.py --host 127.0.0.1   # 指定监听地址
  python start_api.py --port 8080        # 指定端口
  python start_api.py --debug            # 启用调试模式
  python start_api.py --env production   # 使用生产环境配置
  python start_api.py --auto-scheduler   # 自动启动调度器"""
    )
    
    parser.add_argument('--host', default='0.0.0.0', help='服务器监听地址 (默认: 0.0.0.0)')
    parser.add_argument('--port', type=int, default=5000, help='服务器端口 (默认: 5000)')
    parser.add_argument('--debug', action='store_true', help='启用调试模式')
    parser.add_argument('--env', choices=['development', 'production', 'testing'], 
                       default='development', help='运行环境 (默认: development)')
    parser.add_argument('--auto-scheduler', action='store_true', 
                       help='自动启动定时任务调度器')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], 
                       default='INFO', help='日志级别 (默认: INFO)')
    
    args = parser.parse_args()
    
    try:
        # 初始化日志系统
        setup_logger()
        logger = setup_logger("start_api")
        
        logger.info("=" * 50)
        logger.info("股票数据API服务器启动")
        logger.info("=" * 50)
        logger.info(f"Python版本: {sys.version}")
        logger.info(f"工作目录: {os.getcwd()}")
        logger.info(f"运行环境: {args.env}")
        logger.info(f"服务地址: http://{args.host}:{args.port}")
        logger.info(f"调试模式: {args.debug}")
        logger.info(f"自动启动调度器: {args.auto_scheduler}")
        
        # 注册信号处理器
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # 设置环境变量
        os.environ['FLASK_ENV'] = args.env
        if args.auto_scheduler:
            os.environ['AUTO_START_SCHEDULER'] = 'true'
        os.environ['LOG_LEVEL'] = args.log_level
        
        # 启动API服务器
        logger.info("正在启动API服务器...")
        success = start_api_server(
            host=args.host,
            port=args.port,
            debug=args.debug,
            config_name=args.env
        )
        
        if success:
            logger.info("API服务器启动成功")
        else:
            logger.error("API服务器启动失败")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("程序被用户中断")
        sys.exit(0)
    except Exception as e:
        logger = setup_logger("start_api")
        logger.error(f"程序运行异常: {str(e)}")
        import traceback
        logger.error(f"错误堆栈: {traceback.format_exc()}")
        sys.exit(1)


if __name__ == "__main__":
    main()