"""
回测引擎 + 虚拟账户 单元测试
"""

import sys
import tempfile
import os
sys.path.insert(0, '/root/lanbao')

from backtests.paper_account import PaperAccount, Position
from backtests.engine import BacktestEngine


def test_paper_account_buy_sell():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    acc = PaperAccount(initial_cash=100000, db_path=db_path)
    acc.reset()

    # 买入
    trade = acc.buy("2025-04-01", "000001", "平安银行", 10.0, 0.5)
    assert trade is not None
    assert trade.volume > 0
    assert "000001" in acc.positions
    assert acc.cash < 100000

    # 卖出
    trade = acc.sell("2025-04-02", "000001", 11.0)
    assert trade is not None
    assert trade.pnl > 0
    assert "000001" not in acc.positions
    os.unlink(db_path)
    print("test_paper_account_buy_sell PASSED")


def test_stop_loss():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    acc = PaperAccount(initial_cash=100000, db_path=db_path)
    acc.reset()
    acc.buy("2025-04-01", "000001", "平安银行", 10.0, 0.5, hard_stop_loss_pct=0.05)
    pos = acc.positions["000001"]
    assert abs(pos.stop_loss - 10.0 * 0.95) < 0.001

    # 盘中最低价触发止损
    trades = acc.check_stop_loss("2025-04-02", {"000001": 9.0})
    assert len(trades) == 1
    assert trades[0].action == "STOP_LOSS"
    assert "000001" not in acc.positions
    os.unlink(db_path)
    print("test_stop_loss PASSED")


def test_report_metrics():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    acc = PaperAccount(initial_cash=100000, db_path=db_path)
    acc.reset()
    acc.buy("2025-04-01", "000001", "平安银行", 10.0, 0.5)
    acc.sell("2025-04-02", "000001", 11.0)

    stats = acc.stats()
    assert stats["total_trades"] == 1
    assert stats["win_count"] == 1
    assert stats["win_rate"] == 100.0
    os.unlink(db_path)
    print("test_report_metrics PASSED")


if __name__ == "__main__":
    test_paper_account_buy_sell()
    test_stop_loss()
    test_report_metrics()
    print("\nAll tests passed!")
