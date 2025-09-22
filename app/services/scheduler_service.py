#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
定时任务服务

基于Flask-APScheduler的定时任务管理
"""

import sys
import os
from datetime import datetime
from typing import Dict, Any, List, Optional
from flask_apscheduler import APScheduler

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from scheduler.task_scheduler import StockDataScheduler
from utils.logger_config import get_logger

logger = get_logger(__name__)

class SchedulerService:
    """Flask-APScheduler调度器服务"""
    
    def __init__(self, app=None):
        self.scheduler = APScheduler()
        self.stock_scheduler = None
        if app is not None:
            self.init_app(app)
    
    def init_app(self, app):
        """初始化Flask应用"""
        # 配置APScheduler
        app.config.setdefault('SCHEDULER_API_ENABLED', True)
        app.config.setdefault('SCHEDULER_TIMEZONE', 'Asia/Shanghai')
        
        self.scheduler.init_app(app)
        
        # 将服务实例存储到应用上下文中
        app.scheduler_service = self
    
    def _get_stock_scheduler(self) -> StockDataScheduler:
        """获取股票调度器实例（懒加载）"""
        if self.stock_scheduler is None:
            self.stock_scheduler = StockDataScheduler()
        return self.stock_scheduler
    
    def start_scheduler(self):
        """启动调度器"""
        if self.scheduler.running:
            logger.warning("调度器已经在运行中")
            return {'status': 'already_running', 'message': '调度器已经在运行中'}
        
        try:
            self.scheduler.start()
            logger.info("Flask-APScheduler调度器启动成功")
            return {'status': 'started', 'message': 'Flask-APScheduler调度器启动成功'}
                
        except Exception as e:
            logger.error(f"启动调度器异常: {str(e)}")
            return {'status': 'error', 'message': str(e)}
    
    def stop_scheduler(self):
        """停止调度器"""
        if not self.scheduler.running:
            logger.warning("调度器未在运行")
            return {'status': 'not_running', 'message': '调度器未在运行'}
        
        try:
            self.scheduler.shutdown()
            logger.info("Flask-APScheduler调度器停止成功")
            return {'status': 'stopped', 'message': 'Flask-APScheduler调度器停止成功'}
            
        except Exception as e:
            logger.error(f"停止调度器异常: {str(e)}")
            return {'status': 'error', 'message': str(e)}
    
    def get_scheduler_status(self):
        """获取调度器状态"""
        try:
            jobs = self.scheduler.get_jobs()
            job_list = []
            
            for job in jobs:
                job_info = {
                    'id': job.id,
                    'name': job.name,
                    'next_run_time': job.next_run_time.isoformat() if job.next_run_time else None,
                    'trigger': str(job.trigger)
                }
                job_list.append(job_info)
            
            return {
                'running': self.scheduler.running,
                'jobs_count': len(jobs),
                'jobs': job_list
            }
        except Exception as e:
            logger.error(f"获取调度器状态异常: {str(e)}")
            return {'error': str(e)}
    
    def run_once(self):
        """立即执行一次数据采集"""
        try:
            scheduler = self._get_stock_scheduler()
            result = scheduler.setup_realtime_schedule(execute_now=True)
            if result:
                return {'status': 'success', 'message': '数据采集执行成功'}
            else:
                return {'status': 'failed', 'message': '数据采集执行失败'}
        except Exception as e:
            logger.error(f"执行数据采集异常: {str(e)}")
            return {'status': 'error', 'message': str(e)}
    
    def add_history_job(self, start_date, end_date, stock_codes=None, adjust_flag="qfq", execute_immediately=True):
        """添加历史数据采集任务"""
        try:
            def collect_history():
                scheduler = self._get_stock_scheduler()
                return scheduler.setup_history_schedule_once(
                    start_date=start_date,
                    end_date=end_date,
                    stock_codes=stock_codes,
                    adjust_flag=adjust_flag,
                    execute_now=True
                )
            
            job_id = f"history_{start_date}_{end_date}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            if execute_immediately:
                # 立即执行一次
                job = self.scheduler.add_job(
                    id=job_id,
                    func=collect_history,
                    trigger='date',
                    run_date=datetime.now(),
                    name=f"股票历史数据采集({start_date}到{end_date})"
                )
            else:
                # 只添加任务，不立即执行
                job = self.scheduler.add_job(
                    id=job_id,
                    func=collect_history,
                    trigger='cron',
                    hour=9,
                    minute=0,
                    name=f"股票历史数据采集({start_date}到{end_date})"
                )
            
            return {
                'job_id': job.id,
                'name': job.name,
                'next_run_time': job.next_run_time.isoformat() if job.next_run_time else None
            }
        except Exception as e:
            logger.error(f"添加历史数据采集任务异常: {str(e)}")
            raise e
    
    def add_realtime_job(self):
        """添加实时行情数据采集任务"""
        try:
            def collect_realtime():
                scheduler = self._get_stock_scheduler()
                return scheduler.setup_realtime_schedule(execute_now=True)
            
            job_id = "realtime_stock_data"
            
            # 移除已存在的同名任务
            if self.scheduler.get_job(job_id):
                self.scheduler.remove_job(job_id)
            
            # 添加新的实时数据采集任务（每5分钟执行一次）
            job = self.scheduler.add_job(
                id=job_id,
                func=collect_realtime,
                trigger='interval',
                minutes=5,
                name="股票实时数据采集"
            )
            
            return {
                'job_id': job.id,
                'name': job.name,
                'next_run_time': job.next_run_time.isoformat() if job.next_run_time else None
            }
        except Exception as e:
            logger.error(f"添加实时行情数据采集任务异常: {str(e)}")
            raise e
    
    def remove_job(self, job_id):
        """移除指定任务"""
        try:
            self.scheduler.remove_job(job_id)
            logger.info(f"任务 {job_id} 已移除")
            return True
        except Exception as e:
            logger.error(f"移除任务异常: {str(e)}")
            return False
    
    def pause_job(self, job_id):
        """暂停指定任务"""
        try:
            self.scheduler.pause_job(job_id)
            logger.info(f"任务 {job_id} 已暂停")
            return True
        except Exception as e:
            logger.error(f"暂停任务异常: {str(e)}")
            return False
    
    def resume_job(self, job_id):
        """恢复指定任务"""
        try:
            self.scheduler.resume_job(job_id)
            logger.info(f"任务 {job_id} 已恢复")
            return True
        except Exception as e:
            logger.error(f"恢复任务异常: {str(e)}")
            return False
    
    def update_schedule_time(self, new_time):
        """更新调度时间（暂不支持）"""
        return {'status': 'error', 'message': '请使用具体的任务管理方法'}
    
    def add_task(self, task_id: str, name: str, func, 
                 schedule_time: str = None, interval_seconds: int = None,
                 *args, **kwargs) -> bool:
        """添加定时任务
        
        Args:
            task_id: 任务ID
            name: 任务名称
            func: 任务函数
            schedule_time: 调度时间（HH:MM格式）
            interval_seconds: 间隔秒数
            *args: 任务函数参数
            **kwargs: 任务函数关键字参数
            
        Returns:
            bool: 是否添加成功
        """
        try:
            # 移除已存在的同名任务
            if self.scheduler.get_job(task_id):
                self.scheduler.remove_job(task_id)
            
            if schedule_time:
                # 按时间调度
                hour, minute = map(int, schedule_time.split(':'))
                job = self.scheduler.add_job(
                    id=task_id,
                    func=func,
                    args=args,
                    kwargs=kwargs,
                    trigger='cron',
                    hour=hour,
                    minute=minute,
                    name=name
                )
            elif interval_seconds:
                # 按间隔调度
                job = self.scheduler.add_job(
                    id=task_id,
                    func=func,
                    args=args,
                    kwargs=kwargs,
                    trigger='interval',
                    seconds=interval_seconds,
                    name=name
                )
            else:
                logger.error("必须指定schedule_time或interval_seconds之一")
                return False
            
            logger.info(f"添加定时任务: {name} (ID: {task_id})")
            return True
            
        except Exception as e:
            logger.error(f"添加任务异常: {str(e)}")
            return False
    
    def remove_task(self, task_id: str) -> bool:
        """移除定时任务"""
        return self.remove_job(task_id)
    
    def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """获取任务状态"""
        try:
            job = self.scheduler.get_job(task_id)
            if job:
                return {
                    "task_id": job.id,
                    "name": job.name,
                    "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
                    "trigger": str(job.trigger)
                }
            return None
        except Exception as e:
            logger.error(f"获取任务状态异常: {str(e)}")
            return None
    
    def list_tasks(self) -> List[Dict[str, Any]]:
        """列出所有任务"""
        try:
            jobs = self.scheduler.get_jobs()
            return [{
                "task_id": job.id,
                "name": job.name,
                "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
                "trigger": str(job.trigger)
            } for job in jobs]
        except Exception as e:
            logger.error(f"列出任务异常: {str(e)}")
            return []
    
    def add_stock_history_task(self, task_id: str, schedule_time: str, 
                              start_date: str, end_date: str,
                              stock_codes: List[str] = None,
                              adjust_flag: str = "qfq") -> bool:
        """添加股票历史数据采集任务"""
        def collect_history():
            scheduler = self._get_stock_scheduler()
            return scheduler.setup_history_schedule_once(
                start_date=start_date,
                end_date=end_date,
                stock_codes=stock_codes,
                adjust_flag=adjust_flag,
                execute_now=True
            )
        
        return self.add_task(
            task_id=task_id,
            name=f"股票历史数据采集({start_date}到{end_date})",
            func=collect_history,
            schedule_time=schedule_time
        )
    
    def add_stock_realtime_task(self, task_id: str, interval_seconds: int = 300) -> bool:
        """添加股票实时数据采集任务"""
        def collect_realtime():
            scheduler = self._get_stock_scheduler()
            return scheduler.setup_realtime_schedule(execute_now=True)
        
        return self.add_task(
            task_id=task_id,
            name="股票实时数据采集",
            func=collect_realtime,
            interval_seconds=interval_seconds
        )


# 全局调度服务实例（单例模式）
_scheduler_service_instance = None


def get_scheduler_service() -> SchedulerService:
    """获取调度服务实例（单例模式）"""
    global _scheduler_service_instance
    if _scheduler_service_instance is None:
        _scheduler_service_instance = SchedulerService()
    return _scheduler_service_instance