"""
揽宝量化 - 回测执行器
运行 TradeMasterAgent 在历史数据上的完整回测
"""

import os
import sys
import json
import argparse
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, '/root/lanbao')

import pandas as pd

from lb_agents.trade_master_agent import TradeMasterAgent, create_llm_client, RuleBasedClient
from backtests.engine import BacktestEngine


def generate_lsi_for_backtest(dates: list) -> pd.DataFrame:
    """
    为回测生成模拟 LSI 数据（简化版）。
    实际生产环境应从 lsi_history 加载真实数据。
    """
    import random
    random.seed(42)
    scores = []
    base = 50
    for d in dates:
        # 随机游走模拟市场情绪
        change = random.gauss(0, 12)
        base = max(20, min(85, base + change))
        scores.append(base)

    return pd.DataFrame({
        "date": dates,
        "lsi_score": scores,
    })


def run_backtest(
    start_date: str,
    end_date: str,
    llm_mode: str = "rule",
    agent_version: str = "haoyunge_2008_v1.0",
    initial_cash: float = 500000.0,
    output_dir: str = "/root/lanbao/backtests/results",
):
    """
    运行完整回测流程
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    print(f"\n{'='*60}")
    print(f"好运哥2008 回测启动")
    print(f"区间: {start_date} ~ {end_date}")
    print(f"LLM模式: {llm_mode}")
    print(f"初始资金: {initial_cash:,.0f}")
    print(f"{'='*60}\n")

    # 1. 创建 Agent
    client = create_llm_client(llm_mode)
    agent = TradeMasterAgent(llm_client=client, agent_version=agent_version)

    # 2. 创建回测引擎
    engine = BacktestEngine(initial_cash=initial_cash)

    # 3. 加载历史数据并获取日期列表
    engine.load_data(start_date=start_date, end_date=end_date)

    # 4. 生成/加载 LSI 数据
    lsi_df = generate_lsi_for_backtest(engine.all_dates)

    # 5. 设置决策回调
    def decision_callback(date: str, lsi_score: float, holdings: list, leaders: dict, quotes: dict) -> dict:
        # Agent 生成决策（回测模式，使用预计算上下文）
        decision = agent.generate_decision_from_context(
            lsi_score=lsi_score, leaders=leaders, quotes=quotes,
            holdings=holdings, date_str=date
        )
        return decision.to_dict()

    engine.set_decision_callback(decision_callback)

    # 6. 运行回测
    engine.run(lsi_data=lsi_df)

    # 7. 输出报告
    report = engine.report()
    print(f"\n{'='*60}")
    print("回测报告")
    print(f"{'='*60}")
    for k, v in report.items():
        print(f"  {k}: {v}")
    print(f"{'='*60}")

    # 8. 保存结果
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    result_file = output_path / f"backtest_{agent_version}_{start_date}_{end_date}_{timestamp}.json"
    engine.save_report(str(result_file))

    # 同时保存交易流水
    trades_file = output_path / f"trades_{agent_version}_{start_date}_{end_date}_{timestamp}.json"
    trades_data = [
        {
            "date": t.date, "code": t.code, "name": t.name,
            "action": t.action, "price": t.price, "volume": t.volume,
            "amount": t.amount, "commission": t.commission,
            "stamp_tax": t.stamp_tax, "pnl": t.pnl,
        }
        for t in engine.account.trades
    ]
    trades_file.write_text(json.dumps(trades_data, ensure_ascii=False, indent=2), encoding="utf-8")

    # 保存净值曲线
    nav_file = output_path / f"nav_{agent_version}_{start_date}_{end_date}_{timestamp}.json"
    nav_file.write_text(json.dumps(engine.account.nav_history, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"\n结果文件:")
    print(f"  报告: {result_file}")
    print(f"  流水: {trades_file}")
    print(f"  净值: {nav_file}")

    return report, engine


def main():
    parser = argparse.ArgumentParser(description="好运哥2008 回测执行器")
    parser.add_argument("--start", default="2024-01-01", help="开始日期")
    parser.add_argument("--end", default="2024-12-31", help="结束日期")
    parser.add_argument("--llm", default="rule", choices=["rule", "openrouter", "ollama", "auto"], help="LLM模式")
    parser.add_argument("--version", default="haoyunge_2008_v1.0", help="Agent版本")
    parser.add_argument("--cash", type=float, default=500000.0, help="初始资金")
    parser.add_argument("--output", default="/root/lanbao/backtests/results", help="输出目录")

    args = parser.parse_args()
    run_backtest(
        start_date=args.start,
        end_date=args.end,
        llm_mode=args.llm,
        agent_version=args.version,
        initial_cash=args.cash,
        output_dir=args.output,
    )


if __name__ == "__main__":
    main()
