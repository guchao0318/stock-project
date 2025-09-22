# 定时任务调度器
import schedule
import time
from datetime import datetime
from loguru import logger
from typing import Callable
import threading

from config.settings import DATA_COLLECTION_CONFIG
from core.data_collector.stock_collector import StockDataCollector
from core.data_collector.history_collector import HistoryDataCollector
from core.data_collector.realtime_collector import RealtimeDataCollector
from core.data_storage.es_storage import ElasticsearchStorage


class StockDataScheduler:
    """股票数据采集调度器"""
    
    def __init__(self):
        # self.schedule_time = DATA_COLLECTION_CONFIG['schedule_time']
        self.collector = StockDataCollector()
        self.history_collector = HistoryDataCollector()
        self.realtime_collector = RealtimeDataCollector()
        self.storage = ElasticsearchStorage()
        self.is_running = False
        self.scheduler_thread = None
    
    def collect_and_save_data(self):
        """采集并保存股票数据"""
        try:
            logger.info("开始执行定时数据采集任务")
            start_time = datetime.now()
            
            # 采集股票数据
            logger.info("正在采集股票数据...")
            stock_data = self.collector.get_stock_basic_info()
            
            if not stock_data:
                logger.error("未获取到股票数据")
                return False
            
            logger.info(f"成功采集到{len(stock_data)}条股票数据")
            
            # 保存到Elasticsearch
            logger.info("正在保存数据到Elasticsearch...")
            save_result = self.storage.save_stock_data(stock_data, clear_before_save=True)
            
            if save_result:
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                
                # 验证保存结果
                saved_count = self.storage.get_stock_count()
                
                logger.info(f"数据采集任务完成")
                logger.info(f"采集数据量: {len(stock_data)}条")
                logger.info(f"保存数据量: {saved_count}条")
                logger.info(f"执行时间: {duration:.2f}秒")
                
                return True
            else:
                logger.error("数据保存失败")
                return False
                
        except Exception as e:
            logger.error(f"定时任务执行异常: {str(e)}")
            return False
    
    def collect_history_data_once(self, start_date="2025-08-17", end_date="2025-09-17"):
        """一次性采集历史行情数据
        
        Args:
            start_date (str): 开始日期，格式 'YYYY-MM-DD'
            end_date (str): 结束日期，格式 'YYYY-MM-DD'
            
        Returns:
            dict: 采集结果统计
        """
        try:
            logger.info(f"开始执行历史数据采集任务 ({start_date} 到 {end_date})")
            
            # 执行历史数据采集
            result = self.history_collector.collect_all_history_data(
                start_date=start_date,
                end_date=end_date,
                batch_size=50,  # 批量处理50个股票
                delay_between_requests=1  # 请求间隔1秒
            )
            
            logger.info(f"历史数据采集任务完成: 成功 {result['success']}, 失败 {result['failed']}, 总计 {result['total']}")
            return result
            
        except Exception as e:
            logger.error(f"历史数据采集任务执行失败: {str(e)}")
            return {'success': 0, 'failed': 0, 'total': 0, 'error': str(e)}
    
    def collect_history_data_by_api(self, start_date, end_date, stock_codes, adjust_flag="qfq"):
        """通过接口方式采集历史行情数据
        
        Args:
            start_date (str): 开始日期，格式 'YYYY-MM-DD'
            end_date (str): 结束日期，格式 'YYYY-MM-DD'
            stock_codes (list): 股票代码列表
            adjust_flag (str): 前复权标识，'qfq'前复权，'hfq'后复权，''不复权
            
        Returns:
            dict: 采集结果统计
        """
        try:
            logger.info(f"开始通过接口采集历史数据: {start_date} 到 {end_date}, 股票数量: {len(stock_codes)}, 复权方式: {adjust_flag}")
            
            # 执行历史数据采集
            result = self.history_collector.collect_history_data_by_codes(
                start_date=start_date,
                end_date=end_date,
                stock_codes=stock_codes,
                adjust_flag=adjust_flag,
                batch_size=50,  # 批量处理50个股票
                delay_between_requests=1  # 请求间隔1秒
            )
            
            logger.info(f"接口历史数据采集任务完成: 成功 {result['success']}, 失败 {result['failed']}, 总计 {result['total']}")
            return result
            
        except Exception as e:
            logger.error(f"接口历史数据采集任务执行失败: {str(e)}")
            return {'success': 0, 'failed': 0, 'total': 0, 'error': str(e)}
    
    def setup_schedule(self):
        """设置定时任务"""
        try:
            # 清除现有的任务
            schedule.clear()
            
            # 设置每天指定时间执行任务
            schedule.every().day.at(self.schedule_time).do(self.collect_and_save_data)
            
            # 设置实时行情数据采集定时任务
            self.setup_realtime_schedule()
            
            logger.info(f"定时任务已设置: 每天{self.schedule_time}执行数据采集")
            
            # 显示下次执行时间
            next_run = schedule.next_run()
            if next_run:
                logger.info(f"下次执行时间: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
            
        except Exception as e:
            logger.error(f"设置定时任务异常: {str(e)}")
    
    def setup_history_schedule_once(self, start_date="2025-08-17", end_date="2025-09-17", 
                                   stock_codes=None, adjust_flag="qfq", execute_immediately=True):
        """设置历史数据采集的一次性任务（通过接口方式）
        
        Args:
            start_date (str): 开始日期，格式 'YYYY-MM-DD'
            end_date (str): 结束日期，格式 'YYYY-MM-DD'
            stock_codes (list, optional): 股票代码列表，如果为空则从ES的stock_basic_data索引获取全部股票代码
            adjust_flag (str): 前复权标识，'qfq'前复权，'hfq'后复权，''不复权
            execute_immediately (bool): 是否立即执行
            
        Returns:
            dict: 执行结果
        """
        try:
            logger.info(f"设置历史数据采集一次性任务 ({start_date} 到 {end_date})")
            
            # 如果没有提供股票代码，从ES获取全部股票代码
            if not stock_codes:
                logger.info("未提供股票代码，从ES的stock_basic_data索引获取全部股票代码")
                stock_codes = self.storage.get_all_stock_codes()
                
                if not stock_codes:
                    error_msg = "无法从ES获取股票代码列表"
                    logger.error(error_msg)
                    return {'error': error_msg, 'success': 0, 'failed': 0, 'total': 0}
                
                logger.info(f"从ES获取到 {len(stock_codes)} 个股票代码")
            
            if execute_immediately:
                # 立即执行历史数据采集
                return self.collect_history_data_by_api(start_date, end_date, stock_codes, adjust_flag)
            else:
                # 添加到调度队列，下次调度循环时执行
                schedule.every().minute.do(
                    lambda: self.collect_history_data_by_api(start_date, end_date, stock_codes, adjust_flag)
                ).tag('history_once')
                
                logger.info("历史数据采集任务已添加到调度队列")
                return {'status': 'scheduled', 'stock_count': len(stock_codes)}
                
        except Exception as e:
            logger.error(f"设置历史数据采集任务失败: {str(e)}")
            return {'error': str(e)}
    
    def collect_realtime_data_once(self):
        """执行一次实时行情数据采集"""
        try:
            logger.info("开始执行实时行情数据采集")
            
            # 采集沪深A股实时行情数据
            success = self.realtime_collector.collect_and_process_data()
            
            if success:
                logger.info("实时行情数据采集完成")
            else:
                logger.error("实时行情数据采集失败")
                
        except Exception as e:
            logger.error(f"实时行情数据采集过程中发生错误: {e}")
    
    def setup_realtime_schedule(self):
        """设置实时行情数据采集的定时任务（每天下午4点执行）"""
        try:
            logger.info("设置实时行情数据采集定时任务")
            
            # 每天下午4点执行实时行情数据采集
            schedule.every().day.at("16:00").do(self.collect_realtime_data_once)
            
            logger.info("实时行情数据采集定时任务设置完成：每天16:00执行")
            
        except Exception as e:
            logger.error(f"设置实时行情数据采集定时任务失败: {e}")
    
    def setup_realtime_schedule_once(self):
        """设置实时行情数据采集的一次性任务"""
        try:
            logger.info("设置实时行情数据采集一次性任务")
            
            # 立即执行一次性任务
            self.collect_realtime_data_once()
            
        except Exception as e:
            logger.error(f"设置实时行情数据采集一次性任务失败: {e}")
    
    def run_once(self) -> bool:
        """立即执行一次数据采集"""
        logger.info("手动执行数据采集任务")
        return self.collect_and_save_data()
    
    def _scheduler_worker(self):
        """调度器工作线程"""
        logger.info("调度器工作线程启动")
        
        while self.is_running:
            try:
                # 检查并执行待执行的任务
                schedule.run_pending()
                
                # 每分钟检查一次
                time.sleep(60)
                
            except Exception as e:
                logger.error(f"调度器工作线程异常: {str(e)}")
                time.sleep(60)
        
        logger.info("调度器工作线程停止")
    
    def start(self):
        """启动调度器"""
        if self.is_running:
            logger.warning("调度器已经在运行中")
            return
        
        try:
            # 设置定时任务
            self.setup_schedule()
            
            # 启动调度器
            self.is_running = True
            self.scheduler_thread = threading.Thread(target=self._scheduler_worker, daemon=True)
            self.scheduler_thread.start()
            
            logger.info("股票数据采集调度器启动成功")
            
        except Exception as e:
            logger.error(f"启动调度器异常: {str(e)}")
            self.is_running = False
    
    def stop(self):
        """停止调度器"""
        if not self.is_running:
            logger.warning("调度器未在运行")
            return
        
        try:
            self.is_running = False
            
            # 等待工作线程结束
            if self.scheduler_thread and self.scheduler_thread.is_alive():
                self.scheduler_thread.join(timeout=5)
            
            # 清除所有任务
            schedule.clear()
            
            # 关闭存储连接
            if self.storage:
                self.storage.close()
            
            logger.info("股票数据采集调度器已停止")
            
        except Exception as e:
            logger.error(f"停止调度器异常: {str(e)}")
    
    def get_status(self) -> dict:
        """获取调度器状态"""
        try:
            status = {
                'is_running': self.is_running,
                'schedule_time': self.schedule_time,
                'next_run': None,
                'jobs_count': len(schedule.jobs),
                'data_count': 0
            }
            
            # 获取下次执行时间
            next_run = schedule.next_run()
            if next_run:
                status['next_run'] = next_run.strftime('%Y-%m-%d %H:%M:%S')
            
            # 获取当前数据量
            if self.storage:
                status['data_count'] = self.storage.get_stock_count()
            
            return status
            
        except Exception as e:
            logger.error(f"获取调度器状态异常: {str(e)}")
            return {'error': str(e)}
    
    def update_schedule_time(self, new_time: str):
        """更新调度时间"""
        try:
            # 验证时间格式
            datetime.strptime(new_time, '%H:%M')
            
            self.schedule_time = new_time
            
            # 重新设置定时任务
            if self.is_running:
                self.setup_schedule()
            
            logger.info(f"调度时间已更新为: {new_time}")
            
        except ValueError:
            logger.error(f"无效的时间格式: {new_time}，应为HH:MM格式")
        except Exception as e:
            logger.error(f"更新调度时间异常: {str(e)}")


if __name__ == "__main__":
    # 测试代码
    scheduler = StockDataScheduler()
    
    try:
        # 显示状态
        status = scheduler.get_status()
        print(f"调度器状态: {status}")
        
        # 手动执行一次
        print("\n手动执行数据采集...")
        result = scheduler.run_once()
        print(f"执行结果: {result}")
        
        # 启动调度器
        print("\n启动调度器...")
        scheduler.start()
        
        # 显示状态
        status = scheduler.get_status()
        print(f"调度器状态: {status}")
        
        # 运行一段时间
        print("\n调度器运行中，按Ctrl+C停止...")
        while True:
            time.sleep(10)
            status = scheduler.get_status()
            print(f"状态检查: {datetime.now().strftime('%H:%M:%S')} - 下次执行: {status.get('next_run', 'N/A')}")
            
    except KeyboardInterrupt:
        print("\n正在停止调度器...")
        scheduler.stop()
        print("调度器已停止")
    except Exception as e:
        print(f"运行异常: {e}")
        scheduler.stop()