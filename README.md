# lanbao-backtest

揽宝量化回测引擎与模拟盘基础设施。

## 核心组件

- `engine.py` — 回测引擎：T日收盘决策 → T+1开盘执行、盘中止损、净值跟踪
- `paper_account.py` — 虚拟账户：T+1、-7%硬止损、佣金万3+印花税千1、SQLite持久化
- `runner.py` — 回测执行器：支持历史LSI模拟、Sharpe-first报告输出

## 使用方式

```python
from backtests.runner import run_backtest

report, engine = run_backtest(
    start_date="2025-04-01",
    end_date="2026-03-31",
    llm_mode="rule",
    initial_cash=500000.0
)
```

## 安装

```bash
pip install -e .
```
