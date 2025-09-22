# Flask定时任务调度器
from flask import Flask
from flask_apscheduler import APScheduler
from datetime import datetime
from loguru import logger
from typing import Optional, Dict, Any
import threading
import os
import sys

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import DATA_COLLECTION_CONFIG
from core.data_collector.stock_collector import StockDataCollector
from core.data_collector.history_collector import HistoryDataCollector
from core.data_collector.realtime_collector import RealtimeDataCollector
from core.data_storage.es_storage import ElasticsearchStorage


class FlaskStockDataScheduler:
    """Flask股票数据采集调度器"""
    
    def __init__(self, app: Optional[Flask] = None):
        self.app = app
        self.scheduler = APScheduler()
        self.schedule_time = DATA_COLLECTION_CONFIG['schedule_time']
        self.collector = StockDataCollector()
        self.history_collector = HistoryDataCollector()
        self.realtime_collector = RealtimeDataCollector()
        self.storage = ElasticsearchStorage()
        self.is_running = False
        self._lock = threading.Lock()
        
        if app is not None:
            self.init_app(app)
    
    def init_app(self, app: Flask):
        """初始化Flask应用"""
        self.app = app
        
        # 配置APScheduler
        app.config.setdefault('SCHEDULER_API_ENABLED', True)
        app.config.setdefault('SCHEDULER_TIMEZONE', 'Asia/Shanghai')
        app.config.setdefault('SCHEDULER_EXECUTORS', {
            'default': {'type': 'threadpool', 'max_workers': 20}
        })
        app.config.setdefault('SCHEDULER_JOB_DEFAULTS', {
            'coalesce': False,
            'max_instances': 3
        })
        
        # 初始化调度器
        self.scheduler.init_app(app)
        
        # 注册应用关闭时的清理函数
        app.teardown_appcontext(self._cleanup)
    
    def _cleanup(self, exception):
        """清理资源"""
        if exception:
            logger.error(f"应用上下文异常: {exception}")
    
    def collect_and_save_data(self):
        """采集并保存股票数据"""
        with self._lock:
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
    
    def setup_schedule(self):
        """设置定时任务"""
        try:
            # 清除现有的任务
            self.scheduler.remove_all_jobs()
            
            # 设置每天指定时间执行任务
            hour, minute = map(int, self.schedule_time.split(':'))
            self.scheduler.add_job(
                id='daily_stock_collection',
                func=self.collect_and_save_data,
                trigger='cron',
                hour=hour,
                minute=minute,
                replace_existing=True
            )
            
            # 设置实时行情数据采集定时任务（每天下午4点执行）
            self.scheduler.add_job(
                id='daily_realtime_collection',
                func=self.collect_realtime_data_once,
                trigger='cron',
                hour=16,
                minute=0,
                replace_existing=True
            )
            
            logger.info(f"定时任务已设置: 每天{self.schedule_time}执行数据采集")
            logger.info("实时行情数据采集定时任务设置完成：每天16:00执行")
            
        except Exception as e:
            logger.error(f"设置定时任务异常: {str(e)}")
    
    def add_history_job(self, start_date="2025-08-17", end_date="2025-09-17", 
                       stock_codes=None, adjust_flag="qfq", execute_immediately=True):
        """添加历史数据采集任务
        
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
            logger.info(f"添加历史数据采集任务 ({start_date} 到 {end_date})")
            
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
                # 添加到调度队列
                job_id = f'history_collection_{datetime.now().strftime("%Y%m%d_%H%M%S")}'
                self.scheduler.add_job(
                    id=job_id,
                    func=self.collect_history_data_by_api,
                    args=[start_date, end_date, stock_codes, adjust_flag],
                    trigger='date',
                    run_date=datetime.now(),
                    replace_existing=True
                )
                
                logger.info(f"历史数据采集任务已添加到调度队列，任务ID: {job_id}")
                return {'status': 'scheduled', 'job_id': job_id, 'stock_count': len(stock_codes)}
                
        except Exception as e:
            logger.error(f"添加历史数据采集任务失败: {str(e)}")
            return {'error': str(e)}
    
    def add_realtime_job(self):
        """添加实时行情数据采集的一次性任务"""
        try:
            logger.info("添加实时行情数据采集一次性任务")
            
            job_id = f'realtime_collection_{datetime.now().strftime("%Y%m%d_%H%M%S")}'
            self.scheduler.add_job(
                id=job_id,
                func=self.collect_realtime_data_once,
                trigger='date',
                run_date=datetime.now(),
                replace_existing=True
            )
            
            logger.info(f"实时行情数据采集任务已添加，任务ID: {job_id}")
            return {'status': 'scheduled', 'job_id': job_id}
            
        except Exception as e:
            logger.error(f"添加实时行情数据采集任务失败: {str(e)}")
            return {'error': str(e)}
    
    def run_once(self) -> bool:
        """立即执行一次数据采集"""
        logger.info("手动执行数据采集任务")
        return self.collect_and_save_data()
    
    def start(self):
        """启动调度器"""
        if self.is_running:
            logger.warning("调度器已经在运行中")
            return
        
        try:
            # 设置定时任务
            self.setup_schedule()
            
            # 启动调度器
            if not self.scheduler.running:
                self.scheduler.start()
            
            self.is_running = True
            logger.info("Flask股票数据采集调度器启动成功")
            
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
            
            # 停止调度器
            if self.scheduler.running:
                self.scheduler.shutdown(wait=False)
            
            # 关闭存储连接
            if self.storage:
                self.storage.close()
            
            logger.info("Flask股票数据采集调度器已停止")
            
        except Exception as e:
            logger.error(f"停止调度器异常: {str(e)}")
    
    def get_status(self) -> Dict[str, Any]:
        """获取调度器状态"""
        try:
            jobs = self.scheduler.get_jobs()
            
            status = {
                'is_running': self.is_running,
                'scheduler_running': self.scheduler.running,
                'schedule_time': self.schedule_time,
                'jobs_count': len(jobs),
                'jobs': [],
                'data_count': 0
            }
            
            # 获取任务信息
            for job in jobs:
                job_info = {
                    'id': job.id,
                    'name': job.name,
                    'next_run_time': job.next_run_time.strftime('%Y-%m-%d %H:%M:%S') if job.next_run_time else None,
                    'trigger': str(job.trigger)
                }
                status['jobs'].append(job_info)
            
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
    
    def remove_job(self, job_id: str) -> bool:
        """移除指定任务"""
        try:
            self.scheduler.remove_job(job_id)
            logger.info(f"任务 {job_id} 已移除")
            return True
        except Exception as e:
            logger.error(f"移除任务 {job_id} 失败: {str(e)}")
            return False
    
    def pause_job(self, job_id: str) -> bool:
        """暂停指定任务"""
        try:
            self.scheduler.pause_job(job_id)
            logger.info(f"任务 {job_id} 已暂停")
            return True
        except Exception as e:
            logger.error(f"暂停任务 {job_id} 失败: {str(e)}")
            return False
    
    def resume_job(self, job_id: str) -> bool:
        """恢复指定任务"""
        try:
            self.scheduler.resume_job(job_id)
            logger.info(f"任务 {job_id} 已恢复")
            return True
        except Exception as e:
            logger.error(f"恢复任务 {job_id} 失败: {str(e)}")
            return False