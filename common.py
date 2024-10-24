from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta, TH

import numpy as np
import pandas as pd

from py_vollib.black_scholes_merton.greeks.numerical import delta as bsm_num_delta
from py_vollib.black_scholes_merton.greeks.numerical import gamma as bsm_num_gamma
from py_vollib.black_scholes_merton.greeks.numerical import theta as bsm_num_theta
from py_vollib.black_scholes_merton.greeks.numerical import vega as bsm_num_vega
from py_vollib.black_scholes_merton.greeks.numerical import rho as bsm_num_rho
from py_vollib.black_scholes_merton.implied_volatility import implied_volatility as bsm_implied_volatility
from py_lets_be_rational.exceptions import BelowIntrinsicException


class Contract:
    def __init__(self, symbol, sec_type, currency='INR', exchange='NSE'):
        self.con_id = None
        self.symbol = symbol
        self.sec_type = sec_type
        self.currency = currency
        self.exchange = exchange
        self.expiry = None
        self.right = None
        self.strike = None
        # print(f'SYMBOL : {self.symbol} , SEC TYPE : {self.sec_type} \n')


class Order:
    # def __init__(self, action, order_type, quantity):  # Alter properties after object creation if needed...
    def __init__(self, action, order_type, quantity, lmt_price = 0.0):  # Alter properties after object creation if needed...
        self.action = action
        self.order_type = order_type
        self.quantity = quantity
        self.validity = 'DAY'
        # self.lmt_price = 0.0
        self.lmt_price = lmt_price
        self.product_type = 'DELIVERY'
        self.trigger_price = None
        self.stop_loss = None
        self.take_profit = None
        self.trailing_ticks = None


class MarketData:
    def __init__(self, symbol):
        self.timestamp = None
        self.exchange = None
        self.symbol = symbol
        self.symbol_id = None
        self.ltp = None
        self.best_bid_price = None
        self.best_bid_qty = None
        self.best_ask_price = None
        self.best_ask_qty = None
        self.volume = None
        self.atp = None
        self.oi = None
        self.turnover = None
        self.bids = []
        self.asks = []


class OrderInformation:
    def __init__(self, order_id, contract, order, order_state):
        self.order_id = order_id
        self.contract = contract
        self.order = order
        self.order_state = order_state
        self.status = np.nan
        self.filled = np.nan
        self.remaining = np.nan
        self.perm_id = np.nan
        self.parent_id = np.nan
        self.avg_total_price = np.nan
        self.avg_fill_price = np.nan
        self.last_fill_price = np.nan
        self.client_id = np.nan
        self.why_held = np.nan
        self.mkt_cap_price = np.nan
        self.commissions = None
        self.realized_pnl = None
        self.cumulative_quantity = None
        self.side = None
        self.open_order_end = False


class Bar:
    def __init__(self):
        self.o = None
        self.h = None
        self.l = None
        self.c = None
        self.v = None


class OptionComputation:
    def __init__(self, req_id, tick_type, iv: float, delta: float, opt_price: float, pv_dividend: float,
                 gamma: float, vega: float, theta: float, und_price: float, timestamp, option_contract: Contract, risk_free_rate=0.07):
        self.risk_free_rate = risk_free_rate
        # self.update(req_id=req_id, tick_type=tick_type, iv=iv, delta=delta, opt_price=opt_price, pv_dividend=pv_dividend,
        #             gamma=gamma, vega=vega, theta=theta, und_price=und_price, timestamp=timestamp, option_contract=option_contract)

    def update(self, req_id, tick_type, iv: float, delta: float, opt_price: float, pv_dividend: float,
               gamma: float, vega: float, theta: float, und_price: float, timestamp, option_contract: Contract):
        setattr(self, 'req_id', req_id)
        setattr(self, 'tick_type', tick_type)
        setattr(self, 'iv', iv)
        setattr(self, 'delta', delta)
        setattr(self, 'opt_price', opt_price)
        setattr(self, 'pv_dividend', pv_dividend)
        setattr(self, 'gamma', gamma)
        setattr(self, 'vega', vega)
        setattr(self, 'theta', theta)
        setattr(self, 'und_price', und_price)
        setattr(self, 'timestamp', timestamp)
        flag = option_contract.right.lower()
        
        
        print(f"contract : {option_contract.__dict__}")
        
        
        
        k = float(option_contract.strike)
        expiry_str = option_contract.lastTradeDateOrContractMonth
        if len(expiry_str) == 6:
            expiry_limbo = datetime.strptime(expiry_str, "%Y%m")
            expiry_date = get_next_expiry(expiry_limbo, True)
            expiry_str = expiry_date
        expiry_str = expiry_str + "-15:30"
        try:
            expiry = datetime.strptime(expiry_str, "%Y%m%d-%H:%M")
        except ValueError:
            return
        t = (expiry - datetime.now()).total_seconds() / (365 * 24 * 60 * 60)
        bsm_delta = bsm_num_delta(flag=flag, K=k, S=und_price, q=pv_dividend, t=t, sigma=iv / 100, r=self.risk_free_rate)
        bsm_gamma = bsm_num_gamma(flag=flag, K=k, S=und_price, q=pv_dividend, t=t, sigma=iv / 100, r=self.risk_free_rate)
        bsm_theta = bsm_num_theta(flag=flag, K=k, S=und_price, q=pv_dividend, t=t, sigma=iv / 100, r=self.risk_free_rate)
        bsm_vega = bsm_num_vega(flag=flag, K=k, S=und_price, q=pv_dividend, t=t, sigma=iv / 100, r=self.risk_free_rate)
        bsm_rho = bsm_num_rho(flag=flag, K=k, S=und_price, q=pv_dividend, t=t, sigma=iv / 100, r=self.risk_free_rate)
        try:
            bsm_iv = 100 * bsm_implied_volatility(price=opt_price, flag=flag, K=k, S=und_price, q=pv_dividend, t=t, r=self.risk_free_rate)
        except BelowIntrinsicException:
            bsm_iv = np.nan
        setattr(self, 'bsm_delta', bsm_delta)
        setattr(self, 'bsm_gamma', bsm_gamma)
        setattr(self, 'bsm_theta', bsm_theta)
        setattr(self, 'bsm_vega', bsm_vega)
        setattr(self, 'bsm_rho', bsm_rho)
        setattr(self, 'bsm_iv', bsm_iv)


class PositionsHandler:
    def __init__(self):
        self.positions = pd.DataFrame()
        self.temp_pos = pd.DataFrame()
        self.caller = None
        self.callback = None

    def clear(self):
        print('Clearing position handler...')
        self.positions = pd.DataFrame()
        self.temp_pos = pd.DataFrame()

    def add_position(self, position_details):
        self.temp_pos = self.temp_pos.append(position_details, ignore_index=True)

    def overwrite(self):
        self.positions = self.temp_pos
        self.temp_pos = pd.DataFrame()

    def get_futures(self, symbol=None, return_first=True, expiry=None):  # Try overloading to reduce computation
        if expiry is None:
            if symbol is None:
                return self.positions[self.positions['sec_type'] == 'FUT']
            else:
                if return_first:
                    return self.positions[
                        (self.positions['sec_type'] == 'FUT') &
                        (self.positions['symbol'] == symbol)
                        ].to_dict(orient='records')[0]
                else:
                    return self.positions[
                        (self.positions['sec_type'] == 'FUT') &
                        (self.positions['symbol'] == symbol)
                        ].to_dict(orient='records')
        else:
            if symbol is None:
                return self.positions[self.positions['sec_type'] == 'FUT']
            else:
                if return_first:
                    return self.positions[
                        (self.positions['sec_type'] == 'FUT') &
                        (self.positions['symbol'] == symbol) &
                        (self.positions['expiry'] == expiry)
                        ].to_dict(orient='records')[0]
                else:
                    return self.positions[
                        (self.positions['sec_type'] == 'FUT') &
                        (self.positions['symbol'] == symbol) &
                        (self.positions['expiry'] == expiry)
                        ].to_dict(orient='records')


def get_next_expiry(from_date=None, trade_on_expiry=False, following_month=False):
    if from_date is None:
        from_date = datetime.now()
    if trade_on_expiry:
        return get_next_expiry(from_date + relativedelta(days=-1), trade_on_expiry=False, following_month=following_month)
    expiry_month = from_date.month
    weeks_skipped = 1
    while True:
        t = from_date + relativedelta(weekday=TH(weeks_skipped))
        if t.month > expiry_month or (expiry_month == 12 and t.month == 1):
            t = t + relativedelta(weekday=TH(-2))
            if t <= from_date:
                expiry_month = expiry_month + 1
                if expiry_month == 13:
                    expiry_month = 1
                continue
            if following_month:
                following_month = False
                expiry_month = expiry_month + 1
                if expiry_month == 13:
                    expiry_month = 1
                continue
            return t.strftime('%Y%m%d')
        weeks_skipped = weeks_skipped + 1


def get_weekly_expiry(from_date=None, weeks_to_skip=0, trade_on_expiry=False):
    if from_date is None:
        from_date = datetime.now()
    current_date = from_date
    while True:
        if current_date.strftime("%Y%m%d") == datetime.now().strftime("%Y%m%d") and not trade_on_expiry:
            current_date = current_date + timedelta(days=1)
        if current_date.weekday() == 3:                                     
            if weeks_to_skip == 0:
                return current_date.strftime("%Y%m%d")
            else:
                weeks_to_skip = weeks_to_skip - 1
        current_date = current_date + timedelta(days=1)


def generate_bar_start_points(exchange_start_time, exchange_end_time, time_step_in_mins, for_date=None, calculation_style='ib'):
    if for_date is None:
        for_date = datetime.now()
    output_list = []
    if calculation_style.lower() == 'reg':
        dt = for_date.replace(hour=int(exchange_start_time.split(':')[0]), minute=int(exchange_start_time.split(':')[1]), second=0, microsecond=0)
        end_dt = for_date.replace(hour=int(exchange_end_time.split(':')[0]), minute=int(exchange_end_time.split(':')[1]), second=0, microsecond=0)
        while dt < end_dt:
            output_list.append(dt)
            dt = dt + relativedelta(minutes=time_step_in_mins)
    elif calculation_style.lower() == 'ib':
        dt = for_date.replace(hour=int(exchange_end_time.split(':')[0]), minute=int(exchange_end_time.split(':')[1]), second=0, microsecond=0)
        start_dt = for_date.replace(hour=int(exchange_start_time.split(':')[0]), minute=int(exchange_start_time.split(':')[1]), second=0, microsecond=0)
        while dt > start_dt:
            dt = dt - relativedelta(minutes=time_step_in_mins)
            output_list.append(dt)
        output_list.append(start_dt)
        output_list.reverse()
    return output_list


class Exchanges:
    NSE = {
        'name': 'NSE',
        'start_time': '9:15',
        'end_time': '15:30',
        'time_zone_offset': '+0:0'
    }
    MCX = {
        'name': 'MCX',
        'start_time': '9:00',
        'end_time': '23:55',
        'time_zone_offset': '+0:0'
    }

    @classmethod
    def get_exchange(cls, exchange_name):
        if exchange_name.upper() == 'NSE':
            return Exchanges.NSE
        elif exchange_name.upper() == 'MCX':
            return Exchanges.MCX
