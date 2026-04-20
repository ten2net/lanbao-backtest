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


# ==================== 补充测试 ====================

def test_buy_insufficient_cash():
    """资金不足时应返回 None"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    acc = PaperAccount(initial_cash=500, db_path=db_path)
    acc.reset()
    trade = acc.buy("2025-04-01", "000001", "平安银行", 10.0, 0.5)
    assert trade is None
    os.unlink(db_path)
    print("test_buy_insufficient_cash PASSED")


def test_buy_less_than_one_lot():
    """不够买1手（100股）时返回 None"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    acc = PaperAccount(initial_cash=5000, db_path=db_path)
    acc.reset()
    # 50万资金 * 0.5 = 25万目标，但可用现金只有5000，不够买1手（1000元）
    # 等等，这个测试有问题。让我重新设计
    # 5000元现金，目标仓位50%，总资产=5000，目标=2500
    # 价格10元，1手=1000元，2500元可以买200股
    # 这个场景不会失败
    # 我需要让 price * 100 > available_cash
    acc = PaperAccount(initial_cash=500, db_path=db_path)
    acc.reset()
    trade = acc.buy("2025-04-01", "000001", "平安银行", 10.0, 0.5)
    assert trade is None
    os.unlink(db_path)
    print("test_buy_less_than_one_lot PASSED")


def test_buy_add_position_avg_cost():
    """加仓时验证加权平均成本"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    acc = PaperAccount(initial_cash=200000, db_path=db_path)
    acc.reset()

    # 第一次买入：总资产20万 * 10% = 2万 / 10元 = 2000股
    acc.buy("2025-04-01", "000001", "平安银行", 10.0, 0.1)
    pos1 = acc.positions["000001"]
    assert pos1.volume == 2000
    assert abs(pos1.cost_price - 10.0) < 0.01

    # 第二次加仓：可用现金减少后实际买入约1600股 @ 12元
    acc.buy("2025-04-02", "000001", "平安银行", 12.0, 0.1)
    pos2 = acc.positions["000001"]
    assert pos2.volume == 3600  # 2000 + 1600
    expected_cost = (2000 * 10.0 + 1600 * 12.0) / 3600
    assert abs(pos2.cost_price - expected_cost) < 0.01
    os.unlink(db_path)
    print("test_buy_add_position_avg_cost PASSED")


def test_sell_partial():
    """部分卖出"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    acc = PaperAccount(initial_cash=100000, db_path=db_path)
    acc.reset()
    acc.buy("2025-04-01", "000001", "平安银行", 10.0, 0.5)

    # 总资产10万 * 50% = 5万 / 10元 = 5000股；卖出500股后剩余4500股
    trade = acc.sell("2025-04-02", "000001", 11.0, volume=500)
    assert trade is not None
    assert trade.volume == 500
    assert "000001" in acc.positions
    assert acc.positions["000001"].volume == 4500
    os.unlink(db_path)
    print("test_sell_partial PASSED")


def test_sell_nonexistent_position():
    """卖出不存在的持仓返回 None"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    acc = PaperAccount(initial_cash=100000, db_path=db_path)
    acc.reset()
    trade = acc.sell("2025-04-02", "000001", 11.0)
    assert trade is None
    os.unlink(db_path)
    print("test_sell_nonexistent_position PASSED")


def test_clear_all():
    """清仓所有持仓"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    acc = PaperAccount(initial_cash=200000, db_path=db_path)
    acc.reset()
    acc.buy("2025-04-01", "000001", "平安银行", 10.0, 0.2)
    acc.buy("2025-04-01", "000002", "万科A", 20.0, 0.2)

    trades = acc.clear_all("2025-04-02", {"000001": 11.0, "000002": 21.0})
    assert len(trades) == 2
    assert len(acc.positions) == 0
    os.unlink(db_path)
    print("test_clear_all PASSED")


def test_check_stop_loss_not_triggered():
    """价格高于止损价时不触发"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    acc = PaperAccount(initial_cash=100000, db_path=db_path)
    acc.reset()
    acc.buy("2025-04-01", "000001", "平安银行", 10.0, 0.5, hard_stop_loss_pct=0.05)

    trades = acc.check_stop_loss("2025-04-02", {"000001": 9.6})
    assert len(trades) == 0
    assert "000001" in acc.positions
    os.unlink(db_path)
    print("test_check_stop_loss_not_triggered PASSED")


def test_daily_snapshot():
    """每日收盘快照"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    acc = PaperAccount(initial_cash=100000, db_path=db_path)
    acc.reset()
    acc.buy("2025-04-01", "000001", "平安银行", 10.0, 0.5)

    snapshot = acc.daily_snapshot("2025-04-01", {"000001": 11.0})
    assert snapshot["date"] == "2025-04-01"
    assert snapshot["positions_count"] == 1
    assert snapshot["total_value"] > 100000  # 盈利
    os.unlink(db_path)
    print("test_daily_snapshot PASSED")


def test_stats_loss():
    """亏损交易统计"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    acc = PaperAccount(initial_cash=100000, db_path=db_path)
    acc.reset()
    acc.buy("2025-04-01", "000001", "平安银行", 10.0, 0.5)
    acc.sell("2025-04-02", "000001", 9.0)

    stats = acc.stats()
    assert stats["total_trades"] == 1
    assert stats["win_count"] == 0
    assert stats["loss_count"] == 1
    assert stats["win_rate"] == 0.0
    assert stats["total_pnl"] < 0
    os.unlink(db_path)
    print("test_stats_loss PASSED")


def test_reset():
    """重置账户"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    acc = PaperAccount(initial_cash=100000, db_path=db_path)
    acc.reset()
    acc.buy("2025-04-01", "000001", "平安银行", 10.0, 0.5)
    assert len(acc.positions) == 1

    acc.reset()
    assert len(acc.positions) == 0
    assert acc.cash == 100000
    os.unlink(db_path)
    print("test_reset PASSED")


def test_total_value():
    """计算总资产"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    acc = PaperAccount(initial_cash=100000, db_path=db_path)
    acc.reset()
    acc.buy("2025-04-01", "000001", "平安银行", 10.0, 0.5)

    value = acc.total_value({"000001": 12.0})
    pos = acc.positions["000001"]
    expected = acc.cash + pos.volume * 12.0
    assert abs(value - expected) < 1.0
    os.unlink(db_path)
    print("test_total_value PASSED")


if __name__ == "__main__":
    test_paper_account_buy_sell()
    test_stop_loss()
    test_report_metrics()
    test_buy_insufficient_cash()
    test_buy_less_than_one_lot()
    test_buy_add_position_avg_cost()
    test_sell_partial()
    test_sell_nonexistent_position()
    test_clear_all()
    test_check_stop_loss_not_triggered()
    test_daily_snapshot()
    test_stats_loss()
    test_reset()
    test_total_value()
    print("\nAll tests passed!")
