#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
系统功能测试脚本

用于测试股票数据采集系统的各个组件是否正常工作
"""

import sys
import os

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.logger_config import setup_logger, get_logger
from core.data_collector.stock_collector import StockDataCollector
from core.data_storage.es_storage import ElasticsearchStorage
from scheduler.task_scheduler import StockDataScheduler


def test_logger():
    """测试日志系统"""
    print("\n=== 测试日志系统 ===")
    try:
        setup_logger()
        logger = get_logger('test')
        logger.info("日志系统测试成功")
        logger.warning("这是一个警告消息")
        logger.error("这是一个错误消息")
        print("✅ 日志系统测试通过")
        return True
    except Exception as e:
        print(f"❌ 日志系统测试失败: {e}")
        return False


def test_data_collector():
    """测试数据采集器"""
    print("\n=== 测试数据采集器 ===")
    try:
        collector = StockDataCollector()
        
        # 测试获取少量数据
        print("正在测试深圳A股数据获取...")
        sz_data = collector.get_sz_a_stock_data()
        if sz_data is not None and not sz_data.empty:
            print(f"✅ 深圳A股数据获取成功，共{len(sz_data)}条记录")
        else:
            print("⚠️ 深圳A股数据获取失败或为空")
        
        print("正在测试上海A股数据获取...")
        sh_data = collector.get_sh_a_stock_data()
        if sh_data is not None and not sh_data.empty:
            print(f"✅ 上海A股数据获取成功，共{len(sh_data)}条记录")
        else:
            print("⚠️ 上海A股数据获取失败或为空")
        
        # 测试基础信息获取
        print("正在测试股票基础信息获取...")
        basic_info = collector.get_stock_basic_info()
        if basic_info:
            print(f"✅ 股票基础信息获取成功，共{len(basic_info)}条记录")
            # 显示前3条数据示例
            print("数据示例:")
            for i, stock in enumerate(basic_info[:3]):
                print(f"  {i+1}. {stock.get('stock_code')} - {stock.get('stock_name')} ({stock.get('market_type')})")
            return True
        else:
            print("❌ 股票基础信息获取失败")
            return False
            
    except Exception as e:
        print(f"❌ 数据采集器测试失败: {e}")
        return False


def test_elasticsearch_storage():
    """测试Elasticsearch存储"""
    print("\n=== 测试Elasticsearch存储 ===")
    try:
        storage = ElasticsearchStorage()
        
        # 测试连接
        if not storage.es_client:
            print("❌ Elasticsearch连接失败")
            return False
        
        print("✅ Elasticsearch连接成功")
        
        # 测试保存数据
        test_data = [
            {
                'stock_code': 'TEST001',
                'stock_name': '测试股票1',
                'market_type': 'TEST',
                'current_price': 10.50,
                'change_percent': 2.5,
                'update_time': '2024-01-01 09:10:00',
                'data_source': 'test'
            },
            {
                'stock_code': 'TEST002',
                'stock_name': '测试股票2',
                'market_type': 'TEST',
                'current_price': 20.30,
                'change_percent': -1.2,
                'update_time': '2024-01-01 09:10:00',
                'data_source': 'test'
            }
        ]
        
        print("正在测试数据保存...")
        save_result = storage.save_stock_data(test_data, clear_before_save=False)
        if save_result:
            print("✅ 测试数据保存成功")
        else:
            print("❌ 测试数据保存失败")
            return False
        
        # 测试数据查询
        print("正在测试数据查询...")
        search_result = storage.search_stock(stock_code='TEST001')
        if search_result:
            print(f"✅ 数据查询成功，找到{len(search_result)}条记录")
        else:
            print("⚠️ 数据查询结果为空")
        
        # 测试数据统计
        count = storage.get_stock_count()
        print(f"✅ 当前数据总量: {count}条")
        
        # 清理测试数据
        print("正在清理测试数据...")
        storage.es_client.delete_by_query(
            index='stock_basic_data',
            body={"query": {"term": {"market_type": "TEST"}}}
        )
        print("✅ 测试数据清理完成")
        
        storage.close()
        return True
        
    except Exception as e:
        print(f"❌ Elasticsearch存储测试失败: {e}")
        return False


def test_scheduler():
    """测试调度器"""
    print("\n=== 测试调度器 ===")
    try:
        scheduler = StockDataScheduler()
        
        # 测试状态获取
        status = scheduler.get_status()
        print(f"✅ 调度器状态获取成功: {status}")
        
        # 测试调度设置
        scheduler.setup_schedule()
        print("✅ 调度任务设置成功")
        
        # 不执行实际的数据采集，避免耗时过长
        print("✅ 调度器基本功能测试通过")
        
        scheduler.stop()
        return True
        
    except Exception as e:
        print(f"❌ 调度器测试失败: {e}")
        return False


def main():
    """主测试函数"""
    print("股票数据采集系统功能测试")
    print("=" * 50)
    
    test_results = []
    
    # 执行各项测试
    test_results.append(test_logger())
    test_results.append(test_data_collector())
    test_results.append(test_elasticsearch_storage())
    test_results.append(test_scheduler())
    
    # 汇总测试结果
    print("\n" + "=" * 50)
    print("测试结果汇总:")
    
    test_names = ["日志系统", "数据采集器", "Elasticsearch存储", "调度器"]
    passed = 0
    
    for i, (name, result) in enumerate(zip(test_names, test_results)):
        status = "✅ 通过" if result else "❌ 失败"
        print(f"{i+1}. {name}: {status}")
        if result:
            passed += 1
    
    print(f"\n总计: {passed}/{len(test_results)} 项测试通过")
    
    if passed == len(test_results):
        print("🎉 所有测试通过！系统可以正常使用。")
        return True
    else:
        print("⚠️ 部分测试失败，请检查相关配置和依赖。")
        return False


if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n测试被用户中断")
        sys.exit(0)
    except Exception as e:
        print(f"\n测试执行异常: {e}")
        sys.exit(1)