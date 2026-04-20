"""
揽宝量化 - 回测引擎
基于日K数据的历史回测，支持T+1、硬止损、费用计算
"""

import sqlite3
import pandas as pd
from typing import List, Dict, Optional, Callable
from datetime import datetime, timedelta
from pathlib import Path

from .paper_account import PaperAccount, TradeRecord


class BacktestEngine:
    """
    回测引擎
    - 决策日(T)收盘后生成决策
    - 执行日(T+1)开盘价成交
    - 盘中触发-7%硬止损（按当日最低价判断，成交价为止损价）
    - 收盘更新净值
    """

    def __init__(
        self,
        db_path: str = "/root/lanbao/data/lanbao.db",
        initial_cash: float = 500000.0,
        hard_stop_loss: float = 0.05,
    ):
        self.db_path = Path(db_path)
        self.initial_cash = initial_cash
        self.hard_stop_loss = hard_stop_loss
        self.account = PaperAccount(
            initial_cash=initial_cash,
            db_path=db_path,
            account_name="backtest_default",
        )
        self.daily_prices: Dict[str, pd.DataFrame] = {}
        self.all_dates: List[str] = []
        self.decision_callback: Optional[Callable] = None
        self.logs: List[Dict] = []

    def load_data(self, start_date: Optional[str] = None, end_date: Optional[str] = None):
        """从数据库加载所有股票日线数据"""
        print(f"[BacktestEngine] 正在从 {self.db_path} 加载数据...")
        with sqlite3.connect(self.db_path) as conn:
            query = "SELECT * FROM daily_prices"
            conditions = []
            params = []
            if start_date:
                conditions.append("date >= ?")
                params.append(start_date)
            if end_date:
                conditions.append("date <= ?")
                params.append(end_date)
            if conditions:
                query += " WHERE " + " AND ".join(conditions)

            df = pd.read_sql_query(query, conn, params=params)

        if df.empty:
            raise ValueError("数据库中没有 daily_prices 数据，请先运行 data_agent 拉取历史数据")

        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values(['code', 'date'])

        # 按股票代码分组缓存
        self.daily_prices = {
            code: group.reset_index(drop=True)
            for code, group in df.groupby('code')
        }

        # 获取所有交易日列表
        self.all_dates = sorted(df['date'].dt.strftime('%Y-%m-%d').unique().tolist())
        print(f"  加载完成：{len(self.daily_prices)} 只股票，{len(self.all_dates)} 个交易日")

    def get_price(self, code: str, date: str, field: str = 'open') -> float:
        """获取某股票某日的价格"""
        df = self.daily_prices.get(code)
        if df is None or df.empty:
            return 0.0
        row = df[df['date'] == pd.Timestamp(date)]
        if row.empty:
            return 0.0
        val = row.iloc[0].get(field, 0)
        return float(val) if pd.notna(val) else 0.0

    def get_prices_on_date(self, date: str, field: str = 'open') -> Dict[str, float]:
        """获取某交易日所有股票的价格"""
        result = {}
        for code, df in self.daily_prices.items():
            row = df[df['date'] == pd.Timestamp(date)]
            if not row.empty:
                val = row.iloc[0].get(field, 0)
                if pd.notna(val) and val > 0:
                    result[code] = float(val)
        return result

    def set_decision_callback(self, callback: Callable):
        """设置决策回调函数：func(date, lsi_score, holdings) -> TradeDecision"""
        self.decision_callback = callback

    def _detect_leaders(self, date: str) -> Dict[str, List[Dict]]:
        """
        基于历史日线数据检测当日龙头候选
        简化规则：
        - 涨停: 涨跌幅 >= 9.0%
        - 连板: 当日涨停 + 前一日涨停
        - 涨幅大于5: 涨跌幅 >= 5%
        """
        leaders = {"连板": [], "涨停": [], "涨幅大于5": [], "跌幅大于5": []}
        prev_date_idx = self.all_dates.index(date) - 1 if date in self.all_dates else -1
        prev_date = self.all_dates[prev_date_idx] if prev_date_idx >= 0 else None

        for code, df in self.daily_prices.items():
            row = df[df['date'] == pd.Timestamp(date)]
            if row.empty:
                continue
            change_pct = float(row.iloc[0].get('pct_change', 0)) if pd.notna(row.iloc[0].get('pct_change')) else 0
            price = float(row.iloc[0].get('close', 0)) if pd.notna(row.iloc[0].get('close')) else 0
            name = code  # 简化，用code代替name

            # 获取前一日是否涨停
            prev_limit_up = False
            if prev_date:
                prev_row = df[df['date'] == pd.Timestamp(prev_date)]
                if not prev_row.empty:
                    prev_change = float(prev_row.iloc[0].get('pct_change', 0)) if pd.notna(prev_row.iloc[0].get('pct_change')) else 0
                    prev_limit_up = prev_change >= 9.0

            is_limit_up = change_pct >= 9.0
            stock_info = {"code": code, "name": name, "price": price, "change_pct": change_pct}

            if is_limit_up and prev_limit_up:
                leaders["连板"].append(stock_info)
            elif is_limit_up:
                leaders["涨停"].append(stock_info)
            elif change_pct >= 5.0:
                leaders["涨幅大于5"].append(stock_info)
            elif change_pct <= -5.0:
                leaders["跌幅大于5"].append(stock_info)

        # 按涨幅排序
        for key in leaders:
            leaders[key].sort(key=lambda x: x["change_pct"], reverse=True)
        return leaders

    def _make_decision(self, date: str, lsi_score: float, leaders: Dict, quotes: Dict) -> Optional[Dict]:
        """调用回调生成决策"""
        if self.decision_callback is None:
            return None
        holdings = self.account.get_holdings()
        try:
            return self.decision_callback(date, lsi_score, holdings, leaders, quotes)
        except Exception as e:
            print(f"  [{date}] 决策生成失败: {e}")
            return None

    def _execute_decision(self, date: str, decision: Dict):
        """执行交易决策"""
        action = decision.get("action", "HOLD")
        target_code = decision.get("target_code")
        position_pct = decision.get("position_pct", 0)

        # 获取当日开盘价、最低价
        prices_open = self.get_prices_on_date(date, 'open')
        prices_low = self.get_prices_on_date(date, 'low')

        # 第一步：先处理止损（盘中）
        stop_trades = self.account.check_stop_loss(date, prices_low)
        if stop_trades:
            for t in stop_trades:
                self.logs.append({
                    "date": date,
                    "event": "STOP_LOSS",
                    "code": t.code,
                    "volume": t.volume,
                    "price": t.price,
                    "pnl": t.pnl,
                })

        # 第二步：执行 Agent 决策
        if action == "CLEAR":
            # 清仓所有非目标持仓
            for code in list(self.account.positions.keys()):
                if code != target_code:
                    price = prices_open.get(code, 0)
                    if price > 0:
                        trade = self.account.sell(date, code, price, action="CLEAR")
                        if trade:
                            self.logs.append({
                                "date": date, "event": "CLEAR", "code": code,
                                "volume": trade.volume, "price": trade.price, "pnl": trade.pnl,
                            })

        elif action == "SELL" and target_code:
            price = prices_open.get(target_code, 0)
            if price > 0 and target_code in self.account.positions:
                trade = self.account.sell(date, target_code, price, action="SELL")
                if trade:
                    self.logs.append({
                        "date": date, "event": "SELL", "code": target_code,
                        "volume": trade.volume, "price": trade.price, "pnl": trade.pnl,
                    })

        elif action == "BUY" and target_code:
            # 先清仓非目标持仓（好运哥风格：只留龙头）
            for code in list(self.account.positions.keys()):
                if code != target_code:
                    price = prices_open.get(code, 0)
                    if price > 0:
                        trade = self.account.sell(date, code, price, action="CLEAR")
                        if trade:
                            self.logs.append({
                                "date": date, "event": "CLEAR", "code": code,
                                "volume": trade.volume, "price": trade.price, "pnl": trade.pnl,
                            })

            # 买入目标
            price = prices_open.get(target_code, 0)
            if price > 0:
                target_name = decision.get("target_name", target_code)
                trade = self.account.buy(
                    date=date,
                    code=target_code,
                    name=target_name,
                    price=price,
                    target_position_pct=position_pct,
                    hard_stop_loss_pct=self.hard_stop_loss,
                )
                if trade:
                    self.logs.append({
                        "date": date, "event": "BUY", "code": target_code,
                        "volume": trade.volume, "price": trade.price, "amount": trade.amount,
                    })

        elif action == "HOLD":
            # 检查是否需要清掉非龙头持仓（如果决策附带 clear_non_target 标志）
            pass

    def run(self, start_date: Optional[str] = None, end_date: Optional[str] = None, lsi_data: Optional[pd.DataFrame] = None):
        """
        运行回测
        lsi_data: DataFrame with columns ['date', 'lsi_score']，如果为None则默认LSI=50
        """
        if not self.all_dates:
            self.load_data(start_date, end_date)

        # 重置账户
        self.account.reset()
        self.logs.clear()

        # LSI 数据映射
        lsi_map = {}
        if lsi_data is not None and not lsi_data.empty:
            for _, row in lsi_data.iterrows():
                d = pd.to_datetime(row['date']).strftime('%Y-%m-%d')
                lsi_map[d] = float(row['lsi_score'])

        # 遍历每个交易日
        for i, date in enumerate(self.all_dates):
            if i == 0:
                continue  # 第一天没有前一日数据，无法生成决策

            prev_date = self.all_dates[i - 1]
            lsi_score = lsi_map.get(prev_date, 50.0)

            # 基于前一日数据检测龙头候选和行情
            leaders = self._detect_leaders(prev_date)
            quotes = self.get_prices_on_date(prev_date, 'close')
            # 补充涨跌幅到 quotes
            for code in quotes:
                for key in leaders:
                    for s in leaders[key]:
                        if s["code"] == code:
                            quotes[code] = {
                                "code": code, "name": code,
                                "price": s["price"], "change_pct": s["change_pct"],
                                "turnover": 0, "amount": 0,
                            }
                            break

            # 生成决策（基于前一日收盘数据）
            decision = self._make_decision(prev_date, lsi_score, leaders, quotes)
            if decision is None:
                decision = {"action": "HOLD"}

            # 执行决策（当日开盘）
            self._execute_decision(date, decision)

            # 收盘计算净值
            prices_close = self.get_prices_on_date(date, 'close')
            snapshot = self.account.daily_snapshot(date, prices_close)

            if i % 20 == 0 or i == len(self.all_dates) - 1:
                print(f"  [{date}] NAV={snapshot['total_value']:,.0f} 持仓={snapshot['positions_count']} 现金={snapshot['cash']:,.0f}")

        print(f"\n[BacktestEngine] 回测完成")

    def report(self) -> Dict:
        """生成回测报告"""
        if not self.account.nav_history:
            return {}

        navs = pd.DataFrame(self.account.nav_history)
        navs['daily_return'] = navs['total_value'].pct_change()

        total_return = (navs['total_value'].iloc[-1] - self.initial_cash) / self.initial_cash
        days = len(navs)
        annual_return = (1 + total_return) ** (252 / days) - 1 if days > 0 else 0

        daily_returns = navs['daily_return'].dropna()
        sharpe_ratio = (daily_returns.mean() / daily_returns.std()) * (252 ** 0.5) if daily_returns.std() > 0 else 0
        max_drawdown = self.account._calc_max_drawdown()

        stats = self.account.stats()

        return {
            "total_return": round(total_return * 100, 2),
            "annual_return": round(annual_return * 100, 2),
            "sharpe_ratio": round(sharpe_ratio, 3),
            "max_drawdown": round(max_drawdown * 100, 2),
            "total_trades": stats["total_trades"],
            "win_rate": round(stats["win_rate"], 2),
            "avg_win": round(stats["avg_win"], 2),
            "avg_loss": round(stats["avg_loss"], 2),
            "total_pnl": round(stats["total_pnl"], 2),
            "total_commission": round(stats["total_commission"], 2),
            "total_tax": round(stats["total_tax"], 2),
            "final_value": round(navs['total_value'].iloc[-1], 2),
            "initial_value": self.initial_cash,
        }

    def save_report(self, path: str):
        """保存回测报告到文件"""
        import json
        report = self.report()
        Path(path).write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[BacktestEngine] 报告已保存: {path}")
