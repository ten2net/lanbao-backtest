"""
揽宝量化 - 虚拟账户系统 (Paper Trading Account)
模拟A股账户的资金、持仓、T+1结算、费用计算
"""

import sqlite3
from typing import List, Dict, Optional
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path


@dataclass
class Position:
    """持仓记录"""
    code: str
    name: str
    volume: int          # 股数
    cost_price: float    # 成本价
    entry_date: str      # 买入日期
    stop_loss: float = 0.0
    take_profit: float = 0.0

    def market_value(self, price: float) -> float:
        return self.volume * price

    def unrealized_pnl(self, price: float) -> float:
        return (price - self.cost_price) * self.volume

    def pnl_pct(self, price: float) -> float:
        if self.cost_price == 0:
            return 0.0
        return (price - self.cost_price) / self.cost_price


@dataclass
class TradeRecord:
    """成交记录"""
    date: str
    code: str
    name: str
    action: str          # BUY | SELL | STOP_LOSS
    price: float
    volume: int
    amount: float        # 成交金额
    commission: float
    stamp_tax: float
    net_amount: float    # 实际收支
    pnl: float = 0.0     # 卖出时盈亏


class PaperAccount:
    """
    虚拟账户
    - 支持T+1卖出（A股规则：当日买入次日才能卖出）
    - 卖出资金当日可用（可再买入）
    - 费用：佣金万3双向，最低5元；印花税千1卖出收取
    """

    def __init__(
        self,
        initial_cash: float = 500000.0,
        commission_rate: float = 0.0003,
        min_commission: float = 5.0,
        stamp_tax_rate: float = 0.001,
        db_path: str = "/root/lanbao/data/lanbao.db",
        account_name: str = "paper_default",
    ):
        self.initial_cash = initial_cash
        self.cash = initial_cash
        self.commission_rate = commission_rate
        self.min_commission = min_commission
        self.stamp_tax_rate = stamp_tax_rate
        self.db_path = Path(db_path)
        self.account_name = account_name

        self.positions: Dict[str, Position] = {}
        self.trades: List[TradeRecord] = []
        self.nav_history: List[Dict] = []

        self._init_db()
        self._load_state()

    def _init_db(self):
        """初始化数据库表，启用 WAL 模式提升并发性能"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS paper_positions (
                    account TEXT,
                    code TEXT,
                    name TEXT,
                    volume INTEGER,
                    cost_price REAL,
                    entry_date TEXT,
                    stop_loss REAL,
                    take_profit REAL,
                    updated_at TEXT,
                    PRIMARY KEY (account, code)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS paper_trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account TEXT,
                    date TEXT,
                    code TEXT,
                    name TEXT,
                    action TEXT,
                    price REAL,
                    volume INTEGER,
                    amount REAL,
                    commission REAL,
                    stamp_tax REAL,
                    net_amount REAL,
                    pnl REAL,
                    created_at TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS paper_nav (
                    account TEXT,
                    date TEXT,
                    cash REAL,
                    market_value REAL,
                    total_value REAL,
                    positions_count INTEGER,
                    updated_at TEXT,
                    PRIMARY KEY (account, date)
                )
            """)
            conn.commit()

    def _load_state(self):
        """从数据库恢复最新状态"""
        with sqlite3.connect(self.db_path) as conn:
            # 恢复持仓
            cursor = conn.execute(
                "SELECT code, name, volume, cost_price, entry_date, stop_loss, take_profit FROM paper_positions WHERE account = ?",
                (self.account_name,)
            )
            for row in cursor:
                self.positions[row[0]] = Position(
                    code=row[0], name=row[1], volume=row[2],
                    cost_price=row[3], entry_date=row[4],
                    stop_loss=row[5], take_profit=row[6]
                )

            # 恢复最新现金（从最新的NAV记录）
            cursor = conn.execute(
                "SELECT cash FROM paper_nav WHERE account = ? ORDER BY date DESC LIMIT 1",
                (self.account_name,)
            )
            row = cursor.fetchone()
            if row:
                self.cash = row[0]

    def _save_positions(self, conn=None):
        """持久化持仓；支持传入连接以参与外部事务"""
        now = datetime.now().isoformat()
        if conn is None:
            with sqlite3.connect(self.db_path) as c:
                self._save_positions(c)
            return
        conn.execute("DELETE FROM paper_positions WHERE account = ?", (self.account_name,))
        for pos in self.positions.values():
            conn.execute("""
                INSERT INTO paper_positions
                (account, code, name, volume, cost_price, entry_date, stop_loss, take_profit, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                self.account_name, pos.code, pos.name, pos.volume,
                pos.cost_price, pos.entry_date, pos.stop_loss, pos.take_profit, now
            ))

    def _save_trade(self, trade: TradeRecord, conn=None):
        """持久化成交记录；支持传入连接以参与外部事务"""
        now = datetime.now().isoformat()
        if conn is None:
            with sqlite3.connect(self.db_path) as c:
                self._save_trade(trade, c)
            return
        conn.execute("""
            INSERT INTO paper_trades
            (account, date, code, name, action, price, volume, amount, commission, stamp_tax, net_amount, pnl, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            self.account_name, trade.date, trade.code, trade.name, trade.action,
            trade.price, trade.volume, trade.amount, trade.commission,
            trade.stamp_tax, trade.net_amount, trade.pnl, now
        ))

    def _save_nav(self, date: str, market_value: float):
        """持久化净值记录"""
        total = self.cash + market_value
        now = datetime.now().isoformat()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO paper_nav
                (account, date, cash, market_value, total_value, positions_count, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                self.account_name, date, self.cash, market_value, total,
                len(self.positions), now
            ))
            conn.commit()

    def calc_fees(self, price: float, volume: int, is_sell: bool) -> tuple:
        """计算交易费用：佣金 + 印花税"""
        amount = price * volume
        commission = max(amount * self.commission_rate, self.min_commission)
        stamp_tax = amount * self.stamp_tax_rate if is_sell else 0.0
        return commission, stamp_tax

    def can_buy(self, code: str, date: str) -> bool:
        """检查是否能买入（简单版：已有持仓不再重复买入，除非清仓后）"""
        # A股可以买入已有持仓（加仓），这里不做限制，由外部决策控制
        return True

    def buy(self, date: str, code: str, name: str, price: float, target_position_pct: float, hard_stop_loss_pct: float = 0.07) -> Optional[TradeRecord]:
        """
        买入股票
        - target_position_pct: 目标仓位占比（基于当前总资产）
        - 实际按可用现金和100股整数倍计算
        """
        total_value = self.total_value({code: price for code in self.positions})
        target_amount = total_value * target_position_pct
        available_cash = self.cash

        if target_amount > available_cash:
            target_amount = available_cash

        if target_amount < price * 100:
            return None  # 不够买1手

        volume = int(target_amount / price / 100) * 100
        if volume == 0:
            return None

        amount = price * volume
        commission, stamp_tax = self.calc_fees(price, volume, is_sell=False)
        total_cost = amount + commission + stamp_tax

        if total_cost > self.cash:
            return None

        self.cash -= total_cost

        # 更新持仓（加仓则加权平均成本）
        if code in self.positions:
            old = self.positions[code]
            total_vol = old.volume + volume
            avg_cost = (old.volume * old.cost_price + volume * price) / total_vol
            self.positions[code] = Position(
                code=code, name=name or old.name, volume=total_vol,
                cost_price=avg_cost, entry_date=old.entry_date,
                stop_loss=avg_cost * (1 - hard_stop_loss_pct),
                take_profit=self.positions[code].take_profit,
            )
        else:
            self.positions[code] = Position(
                code=code, name=name, volume=volume,
                cost_price=price, entry_date=date,
                stop_loss=price * (1 - hard_stop_loss_pct),
                take_profit=price * 1.10,
            )

        trade = TradeRecord(
            date=date, code=code, name=name or self.positions[code].name,
            action="BUY", price=price, volume=volume,
            amount=amount, commission=commission, stamp_tax=stamp_tax,
            net_amount=-total_cost,
        )
        self.trades.append(trade)
        with sqlite3.connect(self.db_path) as conn:
            self._save_trade(trade, conn)
            self._save_positions(conn)
            conn.commit()
        return trade

    def sell(self, date: str, code: str, price: float, volume: Optional[int] = None, action: str = "SELL") -> Optional[TradeRecord]:
        """卖出股票"""
        if code not in self.positions:
            return None

        pos = self.positions[code]
        sell_vol = volume if volume else pos.volume
        if sell_vol <= 0:
            return None

        if sell_vol > pos.volume:
            sell_vol = pos.volume

        amount = price * sell_vol
        commission, stamp_tax = self.calc_fees(price, sell_vol, is_sell=True)
        net_income = amount - commission - stamp_tax

        # 计算盈亏
        pnl = (price - pos.cost_price) * sell_vol - commission - stamp_tax

        self.cash += net_income
        pos.volume -= sell_vol

        if pos.volume <= 0:
            del self.positions[code]

        trade = TradeRecord(
            date=date, code=code, name=pos.name,
            action=action, price=price, volume=sell_vol,
            amount=amount, commission=commission, stamp_tax=stamp_tax,
            net_amount=net_income, pnl=pnl,
        )
        self.trades.append(trade)
        with sqlite3.connect(self.db_path) as conn:
            self._save_trade(trade, conn)
            self._save_positions(conn)
            conn.commit()
        return trade

    def clear_all(self, date: str, prices: Dict[str, float]) -> List[TradeRecord]:
        """清仓所有持仓"""
        trades = []
        for code in list(self.positions.keys()):
            price = prices.get(code, 0)
            if price > 0:
                trade = self.sell(date, code, price, action="CLEAR")
                if trade:
                    trades.append(trade)
        return trades

    def check_stop_loss(self, date: str, prices: Dict[str, float]) -> List[TradeRecord]:
        """检查并触发硬止损"""
        trades = []
        for code, pos in list(self.positions.items()):
            low_price = prices.get(code, 0)  # 引擎传入当日最低价
            if low_price > 0 and low_price <= pos.stop_loss:
                # 触发止损，按止损价卖出
                trade = self.sell(date, code, pos.stop_loss, action="STOP_LOSS")
                if trade:
                    trades.append(trade)
        return trades

    def total_value(self, prices: Dict[str, float]) -> float:
        """计算总资产"""
        market_value = sum(
            pos.market_value(prices.get(code, 0))
            for code, pos in self.positions.items()
        )
        return self.cash + market_value

    def daily_snapshot(self, date: str, prices: Dict[str, float]):
        """每日收盘快照"""
        market_value = sum(
            pos.market_value(prices.get(code, 0))
            for code, pos in self.positions.items()
        )
        self.nav_history.append({
            "date": date,
            "cash": self.cash,
            "market_value": market_value,
            "total_value": self.cash + market_value,
            "positions_count": len(self.positions),
        })
        self._save_nav(date, market_value)
        return self.nav_history[-1]

    def get_holdings(self) -> List[Dict]:
        """获取当前持仓列表（用于传给 Agent）"""
        return [
            {
                "code": p.code,
                "name": p.name,
                "volume": p.volume,
                "cost_price": p.cost_price,
                "stop_loss": p.stop_loss,
                "take_profit": p.take_profit,
            }
            for p in self.positions.values()
        ]

    def reset(self):
        """重置账户（用于回测重新开始）"""
        self.cash = self.initial_cash
        self.positions.clear()
        self.trades.clear()
        self.nav_history.clear()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM paper_positions WHERE account = ?", (self.account_name,))
            conn.execute("DELETE FROM paper_trades WHERE account = ?", (self.account_name,))
            conn.execute("DELETE FROM paper_nav WHERE account = ?", (self.account_name,))
            conn.commit()

    def stats(self) -> Dict:
        """账户统计"""
        sells = [t for t in self.trades if t.action in ("SELL", "STOP_LOSS", "CLEAR")]
        wins = [t for t in sells if t.pnl > 0]
        losses = [t for t in sells if t.pnl <= 0]

        total_pnl = sum(t.pnl for t in sells)
        total_commission = sum(t.commission for t in self.trades)
        total_tax = sum(t.stamp_tax for t in self.trades)

        return {
            "total_trades": len(sells),
            "win_count": len(wins),
            "loss_count": len(losses),
            "win_rate": len(wins) / len(sells) * 100 if sells else 0,
            "avg_win": sum(t.pnl for t in wins) / len(wins) if wins else 0,
            "avg_loss": sum(t.pnl for t in losses) / len(losses) if losses else 0,
            "total_pnl": total_pnl,
            "total_commission": total_commission,
            "total_tax": total_tax,
            "max_drawdown": self._calc_max_drawdown(),
        }

    def _calc_max_drawdown(self) -> float:
        """计算最大回撤"""
        if not self.nav_history:
            return 0.0
        peak = self.nav_history[0]["total_value"]
        max_dd = 0.0
        for h in self.nav_history:
            if h["total_value"] > peak:
                peak = h["total_value"]
            dd = (peak - h["total_value"]) / peak
            if dd > max_dd:
                max_dd = dd
        return max_dd
