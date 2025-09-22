#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flask应用配置文件

定义不同环境下的配置选项
"""

import os
from datetime import timedelta


class Config:
    """基础配置类"""
    
    # Flask基础配置
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'stock-api-secret-key-2024'
    JSON_AS_ASCII = False
    JSONIFY_PRETTYPRINT_REGULAR = True
    
    # 调度器配置
    AUTO_START_SCHEDULER = os.environ.get('AUTO_START_SCHEDULER', 'false').lower() == 'true'
    SCHEDULER_DEFAULT_TIME = os.environ.get('SCHEDULER_DEFAULT_TIME', '09:10')
    
    # API配置
    API_TITLE = 'Stock Data API'
    API_VERSION = '2.0.0'
    API_DESCRIPTION = '基于Flask框架的股票数据采集和查询API'
    
    # CORS配置
    CORS_ORIGINS = os.environ.get('CORS_ORIGINS', '*').split(',')
    CORS_METHODS = ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS']
    CORS_HEADERS = ['Content-Type', 'Authorization']
    
    # 日志配置
    LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
    LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # 数据库配置（如果需要）
    ELASTICSEARCH_HOST = os.environ.get('ELASTICSEARCH_HOST', 'localhost')
    ELASTICSEARCH_PORT = int(os.environ.get('ELASTICSEARCH_PORT', 9200))
    
    # 股票数据配置
    STOCK_DATA_SOURCE = os.environ.get('STOCK_DATA_SOURCE', 'tushare')
    STOCK_BATCH_SIZE = int(os.environ.get('STOCK_BATCH_SIZE', 100))
    
    # 请求限制配置
    RATELIMIT_STORAGE_URL = os.environ.get('RATELIMIT_STORAGE_URL', 'memory://')
    RATELIMIT_DEFAULT = os.environ.get('RATELIMIT_DEFAULT', '100 per hour')
    
    @staticmethod
    def init_app(app):
        """初始化应用配置"""
        pass


class DevelopmentConfig(Config):
    """开发环境配置"""
    
    DEBUG = True
    TESTING = False
    
    # 开发环境下自动启动调度器
    AUTO_START_SCHEDULER = os.environ.get('AUTO_START_SCHEDULER', 'false').lower() == 'true'
    
    # 开发环境日志级别
    LOG_LEVEL = 'DEBUG'
    
    @staticmethod
    def init_app(app):
        Config.init_app(app)
        
        # 开发环境特定初始化
        import logging
        logging.basicConfig(level=logging.DEBUG)


class ProductionConfig(Config):
    """生产环境配置"""
    
    DEBUG = False
    TESTING = False
    
    # 生产环境下默认启动调度器
    AUTO_START_SCHEDULER = os.environ.get('AUTO_START_SCHEDULER', 'true').lower() == 'true'
    
    # 生产环境日志级别
    LOG_LEVEL = 'INFO'
    
    # 生产环境安全配置
    SESSION_COOKIE_SECURE = True
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = 'Lax'
    
    @staticmethod
    def init_app(app):
        Config.init_app(app)
        
        # 生产环境特定初始化
        import logging
        from logging.handlers import RotatingFileHandler
        
        if not app.debug and not app.testing:
            # 设置文件日志
            file_handler = RotatingFileHandler(
                'logs/stock-api.log',
                maxBytes=10240000,  # 10MB
                backupCount=10
            )
            file_handler.setFormatter(logging.Formatter(
                '%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'
            ))
            file_handler.setLevel(logging.INFO)
            app.logger.addHandler(file_handler)
            
            app.logger.setLevel(logging.INFO)
            app.logger.info('Stock API startup')


class TestingConfig(Config):
    """测试环境配置"""
    
    DEBUG = True
    TESTING = True
    
    # 测试环境下不启动调度器
    AUTO_START_SCHEDULER = False
    
    # 测试环境使用内存数据库
    WTF_CSRF_ENABLED = False
    
    @staticmethod
    def init_app(app):
        Config.init_app(app)


# 配置字典
config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'testing': TestingConfig,
    'default': DevelopmentConfig
}


def get_config(config_name=None):
    """获取配置类"""
    if config_name is None:
        config_name = os.environ.get('FLASK_ENV', 'default')
    
    return config.get(config_name, config['default'])