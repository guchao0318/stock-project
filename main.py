#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票数据采集系统主程序

功能:
1. 每天9:10分自动采集深圳和上海A股数据
2. 将数据存储到Elasticsearch中
3. 支持手动执行数据采集
4. 提供系统状态查询

使用方法:
    python main.py [选项]
    
选项:
    --run-once      立即执行一次数据采集
    --daemon        以守护进程模式运行
    --status        查看系统状态
    --stop          停止运行中的调度器
    --schedule-time 设置调度时间 (格式: HH:MM)
    --help          显示帮助信息
"""

import sys
import os
import argparse
import signal
import time
from datetime import datetime

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.logger_config import setup_logger, get_logger
from scheduler.task_scheduler import StockDataScheduler
from core.data_collector.stock_collector import StockDataCollector
from core.data_collector.history_collector import HistoryDataCollector
from core.data_collector.realtime_collector import RealtimeDataCollector
from core.data_storage.es_storage import ElasticsearchStorage
from core.data_storage.daily_storage import DailyDataStorage
from storage.nine_turn_storage import NineTurnStorage
from config.settings import DATA_COLLECTION_CONFIG

# 全局变量
scheduler = None
logger = None


def signal_handler(signum, frame):
    """信号处理器"""
    global scheduler, logger
    
    if logger:
        logger.info(f"接收到信号 {signum}，正在停止程序...")
    
    if scheduler:
        scheduler.stop()
    
    if logger:
        logger.info("程序已停止")
    
    sys.exit(0)


def run_once():
    """立即执行一次数据采集"""
    global scheduler, logger
    
    logger.info("开始手动执行数据采集")
    
    try:
        scheduler = StockDataScheduler()
        result = scheduler.run_once()
        
        if result:
            logger.info("数据采集执行成功")
            
            # 显示结果统计
            status = scheduler.get_status()
            logger.info(f"当前数据总量: {status.get('data_count', 0)}条")
            
            return True
        else:
            logger.error("数据采集执行失败")
            return False
            
    except Exception as e:
        logger.error(f"手动执行数据采集异常: {str(e)}")
        return False
    finally:
        if scheduler:
            scheduler.stop()


def run_history_collection(start_date, end_date):
    """执行历史数据采集
    
    Args:
        start_date (str): 开始日期，格式 'YYYY-MM-DD'
        end_date (str): 结束日期，格式 'YYYY-MM-DD'
    """
    global logger
    
    logger.info(f"开始执行历史数据采集 ({start_date} 到 {end_date})")
    
    try:
        history_collector = HistoryDataCollector()
        
        print(f"🚀 开始采集历史行情数据 ({start_date} 到 {end_date})")
        print("⏳ 这可能需要较长时间，请耐心等待...")
        
        # 执行历史数据采集
        result = history_collector.collect_all_history_data(
            start_date=start_date,
            end_date=end_date,
            batch_size=50,
            delay_between_requests=1
        )
        
        # 显示结果
        if result['total'] > 0:
            success_rate = (result['success'] / result['total']) * 100
            print(f"\n📊 历史数据采集完成:")
            print(f"   总计: {result['total']} 个股票")
            print(f"   成功: {result['success']} 个 ({success_rate:.1f}%)")
            print(f"   失败: {result['failed']} 个")
            
            if result['success'] > 0:
                print(f"✅ 历史数据采集成功")
                return True
            else:
                print(f"❌ 所有股票的历史数据采集都失败了")
                return False
        else:
            print("❌ 未找到需要采集的股票")
            return False
        
    except Exception as e:
        logger.error(f"历史数据采集失败: {str(e)}")
        print(f"❌ 历史数据采集失败: {str(e)}")
        return False


def run_history_collection_by_api(start_date, end_date, stock_codes_str=None, adjust_flag="qfq"):
    """通过接口运行历史数据采集
    
    Args:
        start_date (str): 开始日期
        end_date (str): 结束日期
        stock_codes_str (str): 股票代码字符串，用逗号分隔
        adjust_flag (str): 复权方式
        
    Returns:
        bool: 采集是否成功
    """
    try:
        logger.info(f"开始通过接口采集历史数据 ({start_date} 到 {end_date})，复权方式: {adjust_flag}")
        
        # 处理股票代码列表
        stock_codes = None
        if stock_codes_str:
            stock_codes = [code.strip() for code in stock_codes_str.split(',') if code.strip()]
            logger.info(f"指定股票代码: {stock_codes}")
        else:
            logger.info("未指定股票代码，将从ES获取全部股票代码")
        
        # 创建调度器实例
        scheduler = StockDataScheduler()
        
        # 执行历史数据采集
        result = scheduler.setup_history_schedule_once(
            start_date=start_date,
            end_date=end_date,
            stock_codes=stock_codes,
            adjust_flag=adjust_flag,
            execute_now=True
        )
        
        if result['total'] > 0:
            success_rate = (result['success'] / result['total']) * 100
            logger.info(f"接口历史数据采集完成: 成功率 {success_rate:.1f}% ({result['success']}/{result['total']})")
            return True
        else:
            logger.error("接口历史数据采集失败: 没有处理任何数据")
            return False
            
    except Exception as e:
        logger.error(f"接口历史数据采集异常: {str(e)}")
        return False


def start_api_server():
    """启动HTTP API服务器"""
    try:
        logger.info("启动HTTP API服务器")
        
        from app.app import start_api_server as start_flask_server
        
        # 启动Flask应用
        return start_flask_server(
            host='0.0.0.0',
            port=5000,
            debug=False
        )
        
    except Exception as e:
        logger.error(f"API服务器启动失败: {str(e)}")
        return False


def run_realtime_collection():
    """执行实时行情数据采集"""
    global logger
    
    logger.info("开始执行实时行情数据采集")
    
    try:
        realtime_collector = RealtimeDataCollector()
        
        print("🚀 开始采集实时行情数据")
        print("⏳ 正在获取实时行情数据...")
        
        # 执行实时数据采集
        result = realtime_collector.collect_all_realtime_data()
        
        # 显示结果
        if result['total'] > 0:
            success_rate = (result['success'] / result['total']) * 100
            print(f"\n📊 实时数据采集完成:")
            print(f"   总计: {result['total']} 个股票")
            print(f"   成功: {result['success']} 个 ({success_rate:.1f}%)")
            print(f"   失败: {result['failed']} 个")
            
            if result['success'] > 0:
                print(f"✅ 实时数据采集成功")
                return True
            else:
                print(f"❌ 所有股票的实时数据采集都失败了")
                return False
        else:
            print("❌ 未找到需要采集的股票")
            return False
        
    except Exception as e:
        logger.error(f"实时数据采集失败: {str(e)}")
        print(f"❌ 实时数据采集失败: {str(e)}")
        return False


def query_nine_turn_signals(date_from=None, date_to=None, stock_code=None, market_type=None):
    """查询九转策略信号
    
    Args:
        date_from (str): 开始日期
        date_to (str): 结束日期
        stock_code (str): 股票代码
        market_type (str): 市场类型
    """
    global logger
    
    logger.info("查询九转策略信号")
    
    try:
        storage = NineTurnStorage()
        signals = storage.query_strategy_signals(
            date_from=date_from,
            date_to=date_to,
            stock_code=stock_code,
            market_type=market_type,
            limit=50
        )
        
        if signals:
            print(f"\n📊 找到 {len(signals)} 条九转策略信号:")
            print("-" * 80)
            for signal in signals:
                print(f"日期: {signal['date']}, 股票: {signal['stock_code']}, "
                      f"市场: {signal['market_type']}, 当前价: {signal['current_price']/100:.2f}, "
                      f"对比价: {signal['compare_price']/100:.2f}, "
                      f"涨幅: {signal['price_change_pct']/100:.2f}%")
        else:
            print("未找到符合条件的九转策略信号")
            
        storage.close()
        
    except Exception as e:
        logger.error(f"查询九转策略信号失败: {str(e)}")
        print(f"❌ 查询九转策略信号失败: {str(e)}")


def query_data(stock_code=None, limit=10, data_type='basic'):
    """查询数据
    
    Args:
        stock_code (str): 股票代码，可选
        limit (int): 返回记录数限制
        data_type (str): 数据类型，'basic' 或 'daily'
    """
    global logger
    
    logger.info(f"开始查询{data_type}数据，股票代码: {stock_code}, 限制: {limit}")
    
    try:
        if data_type == 'daily':
            # 查询日线数据
            storage = DailyDataStorage()
            
            if stock_code:
                results = storage.search_daily_data(stock_code=stock_code, size=limit)
                print(f"\n📊 股票 {stock_code} 的日线数据:")
            else:
                results = storage.search_daily_data(size=limit)
                print(f"\n📊 最新 {limit} 条日线数据:")
            
            if results:
                print(f"找到 {len(results)} 条记录:\n")
                for i, data in enumerate(results, 1):
                    print(f"{i:2d}. {data.get('date', 'N/A'):10s} | "
                          f"{data.get('stock_code', 'N/A'):8s} | "
                          f"开盘: {data.get('open_price', 0)/100:6.2f} | "
                          f"收盘: {data.get('close_price', 0)/100:6.2f} | "
                          f"涨跌: {data.get('change_percent', 0)/100:5.2f}%")
            else:
                print("未找到匹配的日线数据")
            
            # 显示总数据量
            total_count = storage.get_daily_data_count()
            print(f"\n📈 数据库中共有 {total_count} 条日线记录")
            
        else:
            # 查询基础数据
            storage = ElasticsearchStorage()
            
            if stock_code:
                results = storage.search_stock(stock_code=stock_code, size=limit)
                print(f"\n📊 股票 {stock_code} 的基础数据:")
            else:
                results = storage.search_stock(size=limit)
                print(f"\n📊 最新 {limit} 条基础数据:")
            
            if results:
                print(f"找到 {len(results)} 条记录:\n")
                for i, stock in enumerate(results, 1):
                    print(f"{i:2d}. {stock.get('stock_code', 'N/A'):8s} | "
                          f"{stock.get('stock_name', 'N/A'):12s} | "
                          f"{stock.get('market_type', 'N/A'):8s} | "
                          f"价格: {stock.get('current_price', 'N/A'):8s} | "
                          f"涨跌: {stock.get('change_percent', 'N/A'):6s}%")
            else:
                print("未找到匹配的基础数据")
            
            # 显示总数据量
            total_count = storage.get_stock_count()
            print(f"\n📈 数据库中共有 {total_count} 条基础记录")
        
        storage.close()
        
    except Exception as e:
        logger.error(f"查询数据失败: {str(e)}")
        print(f"❌ 查询数据失败: {str(e)}")


def run_daemon():
    """以守护进程模式运行"""
    global scheduler, logger
    
    logger.info("启动股票数据采集守护进程")
    
    try:
        scheduler = StockDataScheduler()
        scheduler.start()
        
        logger.info("守护进程启动成功，按Ctrl+C停止")
        
        # 主循环
        while True:
            try:
                time.sleep(60)  # 每分钟检查一次
                
                # 定期输出状态信息
                if datetime.now().minute % 10 == 0:  # 每10分钟输出一次
                    status = scheduler.get_status()
                    logger.info(f"系统运行中 - 下次执行: {status.get('next_run', 'N/A')}, 数据量: {status.get('data_count', 0)}条")
                    
            except KeyboardInterrupt:
                logger.info("接收到停止信号")
                break
            except Exception as e:
                logger.error(f"守护进程运行异常: {str(e)}")
                time.sleep(60)
                
    except Exception as e:
        logger.error(f"启动守护进程异常: {str(e)}")
        return False
    finally:
        if scheduler:
            scheduler.stop()
    
    return True


def show_status():
    """显示系统状态"""
    global logger
    
    logger.info("查询系统状态")
    
    try:
        scheduler = StockDataScheduler()
        status = scheduler.get_status()
        
        print("\n=== 股票数据采集系统状态 ===")
        print(f"运行状态: {'运行中' if status.get('is_running') else '已停止'}")
        print(f"调度时间: {status.get('schedule_time')}")
        print(f"下次执行: {status.get('next_run', '未设置')}")
        print(f"任务数量: {status.get('jobs_count', 0)}")
        print(f"数据总量: {status.get('data_count', 0)}条")
        print(f"当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("==============================\n")
        
        scheduler.stop()
        return True
        
    except Exception as e:
        logger.error(f"查询系统状态异常: {str(e)}")
        return False


def update_schedule_time(new_time: str):
    """更新调度时间"""
    global logger
    
    logger.info(f"更新调度时间为: {new_time}")
    
    try:
        # 验证时间格式
        datetime.strptime(new_time, '%H:%M')
        
        # 更新配置文件中的时间
        DATA_COLLECTION_CONFIG['schedule_time'] = new_time
        
        logger.info(f"调度时间已更新为: {new_time}")
        logger.info("请重启程序以使新的调度时间生效")
        
        return True
        
    except ValueError:
        logger.error(f"无效的时间格式: {new_time}，应为HH:MM格式")
        return False
    except Exception as e:
        logger.error(f"更新调度时间异常: {str(e)}")
        return False


def main():
    """主函数"""
    global logger
    
    # 设置命令行参数解析
    parser = argparse.ArgumentParser(
        description='股票数据采集系统',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""使用示例:
  python main.py --run-once          # 立即执行一次数据采集
  python main.py --daemon            # 以守护进程模式运行
  python main.py --status            # 查看系统状态
  python main.py --query             # 查询最新基础数据
  python main.py --query --stock 000001  # 查询指定股票基础数据
  python main.py --query-daily       # 查询最新日线数据
  python main.py --query-daily --stock 000001  # 查询指定股票日线数据
  python main.py --history           # 采集历史数据(默认2025-08-17到2025-09-17)
  python main.py --history --start-date 2025-08-01 --end-date 2025-08-31  # 指定日期范围
  python main.py --history-api       # 通过接口采集历史数据(从ES获取全部股票)
  python main.py --history-api --stock-codes 000001,000002 --adjust-flag hfq  # 指定股票和复权方式
  python main.py --start-api         # 启动HTTP API服务器(端口5000)
  python main.py --schedule-time 09:30    # 设置调度时间为09:30"""
    )
    
    parser.add_argument('--run-once', action='store_true', help='立即执行一次数据采集')
    parser.add_argument('--daemon', action='store_true', help='以守护进程模式运行')
    parser.add_argument('--status', action='store_true', help='查看系统状态')
    parser.add_argument('--query', action='store_true', help='查询基础数据')
    parser.add_argument('--query-daily', action='store_true', help='查询日线数据')
    parser.add_argument('--query-signals', action='store_true', help='查询九转策略信号')
    parser.add_argument('--history', action='store_true', help='采集历史行情数据')
    parser.add_argument('--history-api', action='store_true', help='通过接口采集历史行情数据')
    parser.add_argument('--start-api', action='store_true', help='启动HTTP API服务器')
    parser.add_argument('--realtime', action='store_true', help='采集实时行情数据')
    parser.add_argument('--stock', type=str, help='指定股票代码进行查询')
    parser.add_argument('--stock-codes', type=str, help='指定股票代码列表，用逗号分隔 (如: 000001,000002,600000)')
    parser.add_argument('--adjust-flag', type=str, default='qfq', choices=['qfq', 'hfq', ''], help='复权方式: qfq前复权, hfq后复权, 空字符串不复权 (默认: qfq)')
    parser.add_argument('--limit', type=int, default=10, help='查询结果数量限制 (默认: 10)')
    parser.add_argument('--start-date', type=str, default='2025-08-17', help='历史数据开始日期 (默认: 2025-08-17)')
    parser.add_argument('--end-date', type=str, default='2025-09-17', help='历史数据结束日期 (默认: 2025-09-17)')
    parser.add_argument('--date-from', type=str, help='查询开始日期 (YYYY-MM-DD)')
    parser.add_argument('--date-to', type=str, help='查询结束日期 (YYYY-MM-DD)')
    parser.add_argument('--schedule-time', type=str, help='设置调度时间 (格式: HH:MM)')
    
    args = parser.parse_args()
    
    try:
        # 初始化日志系统
        setup_logger()
        logger = get_logger('main')
        
        logger.info("股票数据采集系统启动")
        logger.info(f"Python版本: {sys.version}")
        logger.info(f"工作目录: {os.getcwd()}")
        
        # 注册信号处理器
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # 根据参数执行相应操作
        if args.run_once:
            success = run_once()
            sys.exit(0 if success else 1)
            
        elif args.status:
            success = show_status()
            sys.exit(0 if success else 1)
            
        elif args.query:
            query_data(stock_code=args.stock, limit=args.limit, data_type='basic')
            
        elif args.query_daily:
            query_data(stock_code=args.stock, limit=args.limit, data_type='daily')
            
        elif args.history:
            success = run_history_collection(args.start_date, args.end_date)
            sys.exit(0 if success else 1)
            
        elif args.history_api:
            success = run_history_collection_by_api(
                args.start_date, 
                args.end_date, 
                args.stock_codes, 
                args.adjust_flag
            )
            sys.exit(0 if success else 1)
            
        elif args.start_api:
            success = start_api_server()
            sys.exit(0 if success else 1)
            
        elif args.realtime:
            success = run_realtime_collection()
            sys.exit(0 if success else 1)
            
        elif args.query_signals:
            query_nine_turn_signals(
                date_from=args.date_from,
                date_to=args.date_to,
                stock_code=args.stock,
                market_type=None
            )
            
        elif args.schedule_time:
            success = update_schedule_time(args.schedule_time)
            sys.exit(0 if success else 1)
            
        elif args.daemon:
            success = run_daemon()
            sys.exit(0 if success else 1)
            
        else:
            # 默认显示帮助信息
            parser.print_help()
            print("\n示例用法:")
            print("  python main.py --run-once          # 立即执行一次数据采集")
            print("  python main.py --daemon            # 启动守护进程")
            print("  python main.py --status            # 查看系统状态")
            print("  python main.py --query             # 查询基础数据")
            print("  python main.py --query-daily       # 查询日线数据")
            print("  python main.py --query-signals     # 查询九转策略信号")
            print("  python main.py --history           # 采集历史数据")
            print("  python main.py --history-api       # 通过接口采集历史数据")
        print("  python main.py --history-api --stock-codes 000001,000002 --adjust-flag hfq  # 指定股票和复权方式")
        print("  python main.py --start-api         # 启动HTTP API服务器")
        print("  python main.py --realtime          # 采集实时行情数据")
        print("  python main.py --schedule-time 09:30  # 设置调度时间为9:30")
            
    except KeyboardInterrupt:
        if logger:
            logger.info("程序被用户中断")
        sys.exit(0)
    except Exception as e:
        if logger:
            logger.error(f"程序运行异常: {str(e)}")
        else:
            print(f"程序运行异常: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()