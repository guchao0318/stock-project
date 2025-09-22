# 日志配置模块
import os
import sys
from loguru import logger
from config.settings import LOG_CONFIG


def setup_logger(name="default_logger"):
    """配置日志系统"""
    try:
        # 移除默认的日志处理器
        logger.remove()
        
        # 确保日志目录存在
        log_dir = os.path.dirname(LOG_CONFIG['file_path'])
        if not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)
        
        # 添加控制台输出
        logger.add(
            sys.stdout,
            format=LOG_CONFIG['format'],
            level=LOG_CONFIG['level'],
            colorize=True
        )
        
        # 添加文件输出
        logger.add(
            LOG_CONFIG['file_path'],
            format=LOG_CONFIG['format'],
            level=LOG_CONFIG['level'],
            rotation=LOG_CONFIG['max_size'],
            retention=LOG_CONFIG['backup_count'],
            compression="zip",
            encoding="utf-8"
        )
        
        logger.info("日志系统初始化完成")
        logger.info(f"日志级别: {LOG_CONFIG['level']}")
        logger.info(f"日志文件: {LOG_CONFIG['file_path']}")

        # **返回带 name 的日志实例**
        return logger.bind(name=name)

    except Exception as e:
        print(f"日志系统初始化失败: {str(e)}")
        raise


def get_logger(name: str = None):
    """获取日志记录器"""
    if name:
        return logger.bind(name=name)
    return logger