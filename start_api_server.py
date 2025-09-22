#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票数据采集API服务启动脚本

用于启动HTTP API服务器
"""

import sys
import os
import argparse
from datetime import datetime

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.logger_config import setup_logger, get_logger


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='股票数据采集API服务器',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  python start_api_server.py                    # 启动开发服务器 (localhost:5000)
  python start_api_server.py --host 0.0.0.0    # 监听所有网络接口
  python start_api_server.py --port 8080       # 使用8080端口
  python start_api_server.py --prod            # 生产模式运行

API接口:
  GET  /health                     # 健康检查
  POST /api/v1/history/collect     # 历史数据采集
  GET  /api/v1/stocks/codes        # 获取股票代码列表

示例API调用:
  # 健康检查
  curl http://localhost:5000/health
  
  # 采集历史数据
  curl -X POST http://localhost:5000/api/v1/history/collect \
    -H "Content-Type: application/json" \
    -d '{
      "start_date": "2025-01-01",
      "end_date": "2025-01-05",
      "stock_codes": ["000001", "000002"],
      "adjust_flag": "qfq"
    }'
  
  # 获取股票代码
  curl "http://localhost:5000/api/v1/stocks/codes?limit=10"
"""
    )
    
    parser.add_argument('--host', type=str, default='127.0.0.1', 
                       help='服务器监听地址 (默认: 127.0.0.1)')
    parser.add_argument('--port', type=int, default=5000, 
                       help='服务器监听端口 (默认: 5000)')
    parser.add_argument('--prod', action='store_true', 
                       help='生产模式运行 (关闭调试模式)')
    parser.add_argument('--workers', type=int, default=1,
                       help='工作进程数量 (仅生产模式有效，默认: 1)')
    
    args = parser.parse_args()
    
    try:
        # 初始化日志系统
        setup_logger()
        logger = get_logger('api_server')
        
        logger.info("股票数据采集API服务器启动")
        logger.info(f"Python版本: {sys.version}")
        logger.info(f"工作目录: {os.getcwd()}")
        logger.info(f"服务地址: http://{args.host}:{args.port}")
        
        if args.prod:
            logger.info("生产模式运行")
            # 生产模式使用gunicorn或其他WSGI服务器
            try:
                import gunicorn.app.wsgiapp as wsgi
                
                # 构建gunicorn参数
                sys.argv = [
                    'gunicorn',
                    '--bind', f'{args.host}:{args.port}',
                    '--workers', str(args.workers),
                    '--worker-class', 'sync',
                    '--timeout', '300',
                    '--keep-alive', '2',
                    '--max-requests', '1000',
                    '--max-requests-jitter', '100',
                    '--access-logfile', '-',
                    '--error-logfile', '-',
                    'src.api.stock_api:app'
                ]
                
                wsgi.run()
                
            except ImportError:
                logger.warning("gunicorn未安装，使用Flask开发服务器")
                app.run(
                    host=args.host,
                    port=args.port,
                    debug=False,
                    threaded=True
                )
        else:
            logger.info("开发模式运行")
            app.run(
                host=args.host,
                port=args.port,
                debug=True,
                threaded=True
            )
            
    except KeyboardInterrupt:
        logger.info("服务器被用户中断")
        sys.exit(0)
    except Exception as e:
        logger.error(f"服务器启动失败: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()