#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票数据HTTP API测试脚本

功能:
1. 测试API服务器的健康检查
2. 测试获取股票代码列表
3. 测试历史数据采集接口

使用方法:
1. 先启动API服务器: python main.py --start-api
2. 运行测试脚本: python test_api.py
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.api.api_client import StockAPIClient
import time

def test_api():
    """测试API功能"""
    # 创建API客户端
    client = StockAPIClient(base_url="http://localhost:5000")
    
    print("=== 股票数据HTTP API测试 ===")
    print()
    
    # 1. 测试健康检查
    print("1. 测试健康检查...")
    try:
        health = client.health_check()
        if health:
            print("   ✓ 健康检查通过")
            print(f"   服务状态: {health.get('status')}")
            print(f"   服务时间: {health.get('timestamp')}")
        else:
            print("   ✗ 健康检查失败")
            return False
    except Exception as e:
        print(f"   ✗ 健康检查异常: {e}")
        return False
    
    print()
    
    # 2. 测试获取股票代码
    print("2. 测试获取股票代码列表...")
    try:
        codes = client.get_stock_codes(limit=5)
        if codes:
            print(f"   ✓ 获取到 {len(codes)} 个股票代码")
            print(f"   示例代码: {codes[:3]}")
        else:
            print("   ✗ 未获取到股票代码")
    except Exception as e:
        print(f"   ✗ 获取股票代码异常: {e}")
    
    print()
    
    # 3. 测试历史数据采集
    print("3. 测试历史数据采集...")
    try:
        # 使用少量股票进行测试
        test_codes = ["000001", "000002"]
        print(f"   测试股票: {test_codes}")
        
        result = client.collect_history_data(
            start_date="2024-01-01",
            end_date="2024-01-05",
            stock_codes=test_codes,
            adjust_flag="qfq"
        )
        
        if result:
            print("   ✓ 历史数据采集请求成功")
            print(f"   处理总数: {result.get('total', 0)}")
            print(f"   成功数量: {result.get('success', 0)}")
            print(f"   失败数量: {result.get('failed', 0)}")
            if result.get('total', 0) > 0:
                success_rate = (result.get('success', 0) / result.get('total', 1)) * 100
                print(f"   成功率: {success_rate:.1f}%")
        else:
            print("   ✗ 历史数据采集失败")
    except Exception as e:
        print(f"   ✗ 历史数据采集异常: {e}")
    
    print()
    print("=== 测试完成 ===")
    return True

def main():
    """主函数"""
    print("股票数据HTTP API测试")
    print("请确保API服务器已启动: python main.py --start-api")
    print()
    
    # 等待用户确认
    input("按回车键开始测试...")
    
    # 执行测试
    test_api()

if __name__ == "__main__":
    main()