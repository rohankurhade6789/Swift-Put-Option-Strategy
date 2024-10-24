from ibapi.wrapper import EWrapper
from ibapi.client import EClient
from common import Contract, Order, OrderInformation, PositionsHandler, OptionComputation, Exchanges
from common import get_next_expiry, get_weekly_expiry, generate_bar_start_points


from threading import Thread
import queue
from datetime import datetime
import time

from ibapi.common import TickerId, OrderId, TickAttrib, BarData
from ibapi.contract import Contract as IBContract
from ibapi.contract import ContractDetails
from ibapi.ticktype import TickType, TickTypeEnum
from ibapi.order import Order as IBOrder
from ibapi.order_state import OrderState
from ibapi.execution import Execution, ExecutionFilter
from ibapi.commission_report import CommissionReport

import numpy as np
import pandas as pd

STARTED = object()
FINISHED = object()
TIME_OUT = object()

DEFAULT_HISTORIC_DATA_ID = 1000
DEFAULT_CONTRACT_ID = 2000
DEFAULT_MARKET_DATA_ID = 3000
DEFAULT_EXEC_TICKER = 4000
DEFAULT_HIST_STREAM_DATA_ID = 5000

RISK_FREE_RATE = 0.08


class FinishableQueue(object):
    def __init__(self, queue_to_finish):
        self._queue = queue_to_finish
        self.status = STARTED

    def get(self, timeout=30):
        contents_of_queue = []
        finished = False
        while not finished:
            try:
                current_element = self._queue.get(timeout=timeout)
                if current_element is FINISHED:
                    finished = True
                    self.status = FINISHED
                else:
                    contents_of_queue.append(current_element)
            except queue.Empty:
                finished = True
                self.status = TIME_OUT
        return contents_of_queue

    def timed_out(self):
        return self.status is TIME_OUT


class IBTick:
    def __init__(self, timestamp, tick_id=np.nan, value=np.nan):
        for tick_type_iter in list(TickTypeEnum.idx2name.values()):
            setattr(self, tick_type_iter, np.nan)
        if not np.isnan(tick_id):
            self.timestamp = timestamp
            tick_type = TickTypeEnum.idx2name[tick_id]
            setattr(self, tick_type, value)

    def as_pd_row(self):
        return pd.DataFrame(self.__dict__, index=[self.timestamp])


class OldTick:
    def __init__(self, timestamp, tick_id=np.nan, value=np.nan):
        for tick_type_iter in list(TickTypeEnum.idx2name.values()):
            setattr(self, tick_type_iter, np.nan)
        if not np.isnan(tick_id):
            self.timestamp = timestamp
            tick_type = TickTypeEnum.idx2name[tick_id]
            setattr(self, tick_type, value)

    def as_pd_row(self):
        return pd.DataFrame(self.__dict__, index=[self.timestamp])

    def __repr__(self):
        return self.as_pd_row().__repr__()


class StreamOfTicks(list):

    def __init__(self, list_of_ticks):
        super().__init__(list_of_ticks)

    def as_pd_data_frame(self):
        if len(self) == 0:
            return OldTick(datetime.now()).as_pd_row()

        pd_row_list = [tick.as_pd_row() for tick in self]
        pd_data_frame = pd.concat(pd_row_list)
        return pd_data_frame


class IBError:
    def __init__(self, error_id, error_code, error_string):
        self.error_id = error_id
        self.error_code = error_code
        self.error_string = error_string

    def __str__(self):
        return f"IB error id - {self.error_id} || Error Code - {self.error_code} || String - {self.error_string}"


class ExecutionInformation:
    def __init__(self, req_id: int, contract: IBContract, execution_info: Execution):
        self.req_id = req_id
        self.contract = contract
        self.order_id = execution_info.orderId
        self.client_id = execution_info.clientId
        self.execution_id = execution_info.execId
        self.execution_time = execution_info.time
        self.account_number = execution_info.acctNumber
        self.exchange = execution_info.exchange
        self.side = execution_info.side
        self.shares = execution_info.shares
        self.price = execution_info.price
        self.perm_id = execution_info.permId
        self.liquidation = execution_info.liquidation
        self.cumulative_quantity = execution_info.cumQty
        self.average_price = execution_info.avgPrice
        self.order_reference = execution_info.orderRef
        self.ev_rule = execution_info.evRule
        self.ev_multiplier = execution_info.evMultiplier
        self.model_code = execution_info.modelCode
        self.last_liquidity = execution_info.lastLiquidity
        self.commission_report = np.nan


class CommissionInformation:
    def __init__(self, commission_report: CommissionReport):
        self.exec_id = commission_report.execId
        self.commission = commission_report.commission
        self.currency = commission_report.currency
        self.realized_pnl = commission_report.realizedPNL
        self.income_yield = commission_report.yield_
        self.yield_redemption_date = commission_report.yieldRedemptionDate


class Wrapper(EWrapper):

    def __init__(self, client_id):
        EWrapper.__init__(self)
        self.contract_details = {}
        self.historic_data_dict = {}
        self.keep_up_to_date_last_update = {}
        self.historic_data_queue_dict = {}
        self.historic_data_updates_dict = {}
        self.last_historical_update_at = datetime
        self.market_data_dict = {}
        self.market_data = {}
        self.requested_executions = {}
        self.requested_executions_req_ids = set()
        self.executions = {}
        self.exec_order_map = {}
        self.rogue_commissions = []
        self.orders = {}
        self.errors = queue.Queue()
        self.time_queue = queue.Queue()
        self.order_id_data = queue.Queue()
        self.store_market_data_queue = False
        self.mkt_keep_up_to_date_ids = set()
        self.option_computations = {}
        self.market_data_contracts = {}
        self.keep_up_to_date_granularity = {}
        self.keep_up_to_date_closes = {}
        self.mkt_keep_up_to_date_last_update = {}
        self.opens = {}
        self.highs = {}
        self.lows = {}
        self.closes = {}
        self.volumes = {}
        self.times = {}
        self.strike_chain = {}
        self.positions_handler = PositionsHandler()
        self.client_id = client_id
        self.executions_completed = {}
        self.exec_overwrite_status = {}
        self.mkt_data_update_candle_start_points = {}

    @staticmethod
    def get_time_stamp():
        return datetime.now()

    def is_error(self):
        return not self.errors.empty()

    def get_error(self, timeout=15):
        if self.is_error():
            try:
                return self.errors.get(timeout=timeout)
            except queue.Empty:
                return None

    def error(self, error_id, error_code, error_string):
        self.errors.put(IBError(error_id=error_id, error_code=error_code, error_string=error_string))

    def currentTime(self, curr_ib_time: int):
        self.time_queue.put(curr_ib_time)

    def contractDetails(self, req_id: int, contract_details: ContractDetails):
        if req_id not in self.contract_details.keys():
            self.contract_details[req_id] = queue.Queue()
        self.contract_details[req_id].put(contract_details)

    def contractDetailsEnd(self, req_id: int):
        if req_id not in self.contract_details.keys():
            self.contract_details[req_id] = queue.Queue()
        self.contract_details[req_id].put(FINISHED)

    def securityDefinitionOptionParameter(self, reqId: int, exchange: str,
                                        underlyingConId: int, tradingClass: str, multiplier: str,
                                        expirations, strikes):
        super().securityDefinitionOptionParameter(reqId, exchange,
                                                underlyingConId, tradingClass, multiplier, expirations, strikes)
        self.strike_chain[reqId] = tuple(strikes)
        # print(f'reqId {reqId}')
        # print(f'tuple strike : {tuple(strikes)}')
        # print(self.strike_chain[reqId])
        # time.sleep(1)

    def historicalData(self, req_id: int, bar: BarData):
        bar_data = (bar.date, bar.open, bar.high, bar.low, bar.close, bar.volume, bar.barCount, bar.average)
        if req_id not in self.historic_data_queue_dict.keys():
            self.historic_data_queue_dict[req_id] = queue.Queue()
        self.historic_data_queue_dict[req_id].put(bar_data)

    def historicalDataEnd(self, req_id: int, start: str, end: str):
        if req_id not in self.historic_data_queue_dict.keys():
            self.historic_data_queue_dict[req_id] = queue.Queue()
        self.historic_data_queue_dict[req_id].put(FINISHED)

    def historicalDataUpdate(self, req_id: int, bar: BarData):
        try:
            update_at = datetime.strptime(bar.date, '%Y%m%d %H:%M:%S')
            bar_data = (bar.date, bar.open, bar.high, bar.low, bar.close, bar.volume, bar.barCount, bar.average)
            if req_id not in self.keep_up_to_date_last_update.keys():
                self.keep_up_to_date_last_update[req_id] = update_at
            update_dict = {
                'time': bar.date,
                'o': bar.open,
                'h': bar.high,
                'l': bar.low,
                'c': bar.close,
                'v': bar.volume,
            }
            new_close = bar.close
            new_volume = bar.volume
            # if not update_at.minute > self.keep_up_to_date_last_update[req_id].minute:  # TODO: Improve and use the upstox logic
            #     del self.historic_data_dict[req_id][-1]
            #     del self.closes[req_id][-1]
            #     del self.volumes[req_id][-1]
            # self.historic_data_dict[req_id].append(update_dict)
            # self.closes[req_id].append(new_close)
            # self.volumes[req_id].append(new_volume)
            if req_id not in self.historic_data_queue_dict.keys():
                self.historic_data_queue_dict[req_id] = queue.Queue()
            self.keep_up_to_date_last_update[req_id] = update_at
            self.historic_data_queue_dict[req_id].put(bar_data)
        except KeyError:
            print(f"Waiting for HD download for {req_id}")

    def init_market_data(self, req_id):
        instant_mkt_data_tick = self.market_data[req_id] = IBTick(req_id)
        market_data_queue = self.market_data_dict[req_id] = queue.Queue()
        return market_data_queue, instant_mkt_data_tick

    def tickPrice(self, req_id: TickerId, tick_type: TickType, price: float, attrib: TickAttrib):
        tick_timestamp = self.get_time_stamp()
        if req_id not in self.market_data.keys():
            self.market_data[req_id] = IBTick(tick_timestamp, tick_type, price)
        setattr(self.market_data[req_id], TickTypeEnum.idx2name[tick_type], price)
        if req_id in self.mkt_keep_up_to_date_ids and TickTypeEnum.idx2name[tick_type] == 'LAST':
            try:
                if tick_timestamp >= self.mkt_data_update_candle_start_points[req_id][1]:  # TODO: Account for illiquid products here
                    # self.hist_data[ticker_id].append({
                    #     'time': self.market_data[ticker_id].timestamp,
                    #     'o': self.market_data[ticker_id].ltp,
                    #     'h': self.market_data[ticker_id].ltp,
                    #     'l': self.market_data[req_id].ltp
                    # })
                    self.times[req_id].append(self.mkt_data_update_candle_start_points[req_id][1])
                    self.opens[req_id].append(self.market_data[req_id].LAST)
                    self.highs[req_id].append(self.market_data[req_id].LAST)
                    self.lows[req_id].append(self.market_data[req_id].LAST)
                    self.closes[req_id].append(self.market_data[req_id].LAST)
                    self.mkt_data_update_candle_start_points[req_id] = self.mkt_data_update_candle_start_points[req_id][1:]
                self.highs[req_id][-1] = max(price, self.highs[req_id][-1])
                self.lows[req_id][-1] = min(price, self.lows[req_id][-1])
                self.closes[req_id][-1] = price
            except IndexError:
                self.highs[req_id][-1] = max(price, self.highs[req_id][-1])
                self.lows[req_id][-1] = min(price, self.lows[req_id][-1])
                self.closes[req_id][-1] = price
            self.market_data[req_id].timestamp = tick_timestamp
        if self.store_market_data_queue:
            _tick_data = OldTick(tick_timestamp, tick_type, price)
            self.market_data_dict[req_id].put(_tick_data)

    def tickSize(self, req_id: TickerId, tick_type: TickType, size: int):
        tick_timestamp = self.get_time_stamp()
        if req_id not in self.market_data.keys():
            self.market_data[req_id] = IBTick(tick_timestamp)
        setattr(self.market_data[req_id], TickTypeEnum.idx2name[tick_type], size)
        setattr(self.market_data[req_id], "timestamp", tick_timestamp)
        if self.store_market_data_queue:
            _tick_data = OldTick(tick_timestamp, tick_type, size)
            self.market_data_dict[req_id].put(_tick_data)

    def tickGeneric(self, req_id: TickerId, tick_type: TickType, value: float):
        tick_timestamp = self.get_time_stamp()
        if req_id not in self.market_data.keys():
            self.market_data[req_id] = IBTick(tick_timestamp)
        setattr(self.market_data[req_id], TickTypeEnum.idx2name[tick_type], value)
        setattr(self.market_data[req_id], "timestamp", tick_timestamp)
        if self.store_market_data_queue:
            _tick_data = OldTick(tick_timestamp, tick_type, value)
            self.market_data_dict[req_id].put(_tick_data)

    def tickOptionComputation(self, req_id: TickerId, tick_type: TickType, tickAttrib: int, implied_vol: float, delta: float, opt_price: float, pv_dividend: float,
                              gamma: float, vega: float, theta: float, und_price: float):
        timestamp = datetime.now()
        # if req_id in self.option_computations.keys():  # Can be optimized by try except
        #     self.option_computations[req_id].update(req_id=req_id, tick_type=tick_type, iv=implied_vol, delta=delta, opt_price=opt_price, pv_dividend=pv_dividend,
        #                                             gamma=gamma, vega=vega, theta=theta, und_price=und_price, timestamp=timestamp, option_contract=self.market_data_contracts[req_id])
        # else:
        self.option_computations[req_id] = OptionComputation(req_id=req_id, tick_type=tick_type, iv=implied_vol, delta=delta,
                                                                opt_price=opt_price, pv_dividend=pv_dividend, gamma=gamma, vega=vega, theta=theta,
                                                                und_price=und_price, timestamp=timestamp, option_contract=self.market_data_contracts[req_id])

    def nextValidId(self, order_id):
        if getattr(self, 'order_id_data', None) is None:
            self.order_id_data = queue.Queue()
        self.order_id_data.put(order_id)

    def openOrder(self, order_id: OrderId, contract: IBContract, order: IBOrder, order_state: OrderState):
        if order_id in self.orders.keys():
            self.orders[order_id].order_id = order_id
            self.orders[order_id].contract = contract
            self.orders[order_id].order = order
            self.orders[order_id].order_state = order_state
        else:
            # self.orders[order_id] = OrderInformation(order_id=order_id, contract=contract, order=order, order_state=order_state)
            print('####################################################################################################################################################################################')

    def orderStatus(self, order_id: OrderId, status: str, filled: float, remaining: float, avg_fill_price: float, perm_id: int,
                    parent_id: int, last_fill_price: float, client_id: int, why_held: str, mkt_cap_price: float):
        self.orders[order_id].status = status
        self.orders[order_id].filled = filled
        self.orders[order_id].remaining = remaining
        self.orders[order_id].avg_fill_price = avg_fill_price
        self.orders[order_id].perm_id = perm_id
        self.orders[order_id].parent_id = parent_id
        self.orders[order_id].last_fill_price = last_fill_price
        self.orders[order_id].client_id = client_id
        self.orders[order_id].why_held = why_held
        self.orders[order_id].mkt_cap_price = mkt_cap_price
        if status == 'Cancelled':
            del self.orders[order_id]
        if remaining == 0:
            commissions_paid = self.orders[order_id].order_state.commission
            if self.orders[order_id].order.action == "BUY":
                self.orders[order_id].avg_total_price = ((avg_fill_price * filled) + commissions_paid) / filled
            elif self.orders[order_id].order.action == "SELL":
                self.orders[order_id].avg_total_price = ((avg_fill_price * filled) - commissions_paid) / filled

    def openOrderEnd(self):
        for order_id in self.orders.keys():
            self.orders[order_id].open_order_end = True

    def execDetails(self, req_id: int, contract: IBContract, execution: Execution):
        try:
            overwrite = self.exec_overwrite_status[req_id]
            self.exec_order_map[execution.execId] = execution.orderId
            if not (req_id in self.requested_executions_req_ids and not overwrite):
                print(f'In here with req_id:{req_id}')
                try:
                    self.orders[execution.orderId].cumulative_quantity = execution.cumQty
                except KeyError:
                    if execution.clientId == self.client_id:
                        self.orders[execution.orderId] = OrderInformation(contract=contract, order=None, order_id=execution.orderId, order_state=None)
                        self.orders[execution.orderId].cumulative_quantity = execution.cumQty
                        self.orders[execution.orderId].status = 'Filled'
                        self.orders[execution.orderId].filled = execution.shares
                        self.orders[execution.orderId].remaining = 0
                        self.orders[execution.orderId].perm_id = execution.permId
                        self.orders[execution.orderId].avg_fill_price = execution.avgPrice
                        self.orders[execution.orderId].client_id = execution.clientId
                        self.orders[execution.orderId].side = execution.side
                        self.orders[execution.orderId].time = datetime.strptime(execution.time, '%Y%m%d  %H:%M:%S')
                if req_id not in self.executions.keys():
                    self.executions[req_id] = {}
                self.executions[req_id][execution.execId] = ExecutionInformation(req_id=req_id, contract=contract, execution_info=execution)
        except KeyError:
            pass

    def execDetailsEnd(self, req_id: int):
        self.executions_completed[req_id] = True
        self.requested_executions[req_id].put(FINISHED)
        self.requested_executions_req_ids.add(req_id)
        print('execDetailsEnd')

    def commissionReport(self, commission_report: CommissionReport):
        print(f"commissionReport called with {commission_report.__dict__}")
        filled = False
        time.sleep(0.02)
        if commission_report.execId in self.exec_order_map:
            order_id = self.exec_order_map[commission_report.execId]
            try:
                self.orders[order_id].commissions = self.orders[order_id].commissions + commission_report.commission
            except TypeError:
                self.orders[order_id].commissions = commission_report.commission
            except KeyError:
                pass
            if commission_report.realizedPNL < 1e100:
                try:
                    self.orders[order_id].realized_pnl = self.orders[order_id].realized_pnl + commission_report.realizedPNL
                except TypeError:
                    self.orders[order_id].realized_pnl = commission_report.realizedPNL
                except KeyError:
                    pass
        for req_id in self.executions.keys():
            if self.executions[req_id] == commission_report.execId:
                self.executions[req_id].commission_report = commission_report
                filled = True
        if not filled:
            self.rogue_commissions.append(CommissionInformation(commission_report))

    def position(self, account: str, contract: IBContract, position: float, avgCost: float):
        super().position(account, contract, position, avgCost)
        position_details = {
            'contract': contract,
            'con_id': contract.conId,
            'symbol': contract.symbol,
            'exchange': contract.exchange,
            'sec_type': contract.secType,
            'expiry': contract.lastTradeDateOrContractMonth,
            'right': contract.right,
            'strike': contract.strike,
            # 'Account': account,
            'position': position,
            'avg_cost': avgCost
        }
        self.positions_handler.add_position(position_details)

    def positionEnd(self):
        self.positions_handler.overwrite()
        # print('\nAll positions downloaded...\n')

    def fundamentalData(self, req_id: TickerId, data: str):
        super().fundamentalData(req_id, data)
        print("FundamentalData. ReqId:", req_id, "Data:", data)


class IBAppHistoricDataTimedOutError(LookupError):
    def __str__(self):
        return 'Error raised on historic data request getting timed out'


class IBAppNoHistoricDataError(LookupError):
    def __str__(self):
        return 'Error raised because no historic data found'


class Client(EClient):
    current_order_id = {}

    def __init__(self, wrapper, port_id, client_id):
        EClient.__init__(self, wrapper)
        self._market_data_q_dict = {}
        self._mkt_data_dict = {}
        self.port_id = port_id
        self.client_id = client_id
        self.requested_trades_dict = {}
        self.get_next_expiry = get_next_expiry
        self.get_weekly_expiry = get_weekly_expiry
        self.generate_bar_start_points = generate_bar_start_points

    def clock(self, return_type="str"):
        print("Getting time for IB...")
        time_storage = self.wrapper.time_queue = queue.Queue()
        self.reqCurrentTime()
        max_wait = 15
        try:
            current_time = time_storage.get(timeout=max_wait)
        except queue.Empty:
            print("Wrapper timed out")
            current_time = None
        while self.wrapper.is_error():
            print(self.wrapper.get_error())
        if return_type == "str" or return_type == "datetime":
            if isinstance(current_time, int) and current_time > 0:
                datetime_curr_time = datetime.fromtimestamp(current_time)
                if return_type == "datetime":
                    return datetime_curr_time
                if return_type == "str":
                    str_curr_time = datetime_curr_time.strftime("%Y-%b-%d %H:%M:%S.%f")
                    return str_curr_time
        if return_type == "int":
            return current_time

    def get_option_chain(self, contract: Contract, req_id = 1):
        if contract.secType == 'STK':
            self.reqSecDefOptParams(req_id, contract.symbol, "", "STK", contract.conId)
        else:
            print('ERROR')
            raise TypeError('The contract needs to be of type STK to fetch option chain')

    def resolve_contract(self, contract: Contract, req_id=DEFAULT_CONTRACT_ID, full_details=False):
        ib_symbol = contract.symbol
        # print("HIT 1")
        ib_symbol = ib_symbol.replace('&', '')
        ib_symbol = ib_symbol[:9]
        ib_contract = IBContract()
        ib_contract.symbol = ib_symbol
        ib_contract.currency = contract.currency
        ib_contract.exchange = contract.exchange
        ib_contract.secType = contract.sec_type
        if contract.sec_type == 'FUT':
            ib_contract.lastTradeDateOrContractMonth = contract.expiry
        if contract.sec_type == 'OPT':
            ib_contract.lastTradeDateOrContractMonth = contract.expiry
            if contract.strike:
                ib_contract.strike = contract.strike
                ib_contract.right = contract.right
        contract_details_queue = self.wrapper.contract_details[req_id] = queue.Queue()
        contract_details_queue = FinishableQueue(contract_details_queue)
        # print()
        # print(f"Getting contract reqId-  {req_id} \n")
        # print()
        # print(f'ib_contract : {ib_contract} \n')
        # print()
        self.reqContractDetails(req_id, ib_contract)
        max_wait = 30
        new_contract_details = contract_details_queue.get(timeout=max_wait)
        # print(new_contract_details[0])
        while self.wrapper.is_error():
            print('1')
            print(self.wrapper.get_error())
        if contract_details_queue.timed_out():
            print('2')
            print("Contract Details Queue Timed Out")
        if len(new_contract_details) == 0:
            print('3')
            print("Failed to get Additional Contracts")
            return ib_contract
        if len(new_contract_details) > 1:
            print('4')
            print(new_contract_details)
            print("Got Multiple Contracts")
        resolved_contract = new_contract_details[0]
        if full_details:
            return resolved_contract
        else:
            return resolved_contract.contract

    def get_historic_data(self, contract,
                          ticker_id=DEFAULT_HISTORIC_DATA_ID,
                          query_time=None,
                          duration="1 D",
                          bar_size="1 min",
                          what_to_show="TRADES",
                          use_rth=1,
                          format_date=1,
                          keep_up_to_date=False,
                          chart_options=None,
                          return_as_dict_list=True,
                          timeout_limit=30,
                          any_keep_up_to_date=False):
        global DEFAULT_HISTORIC_DATA_ID
        query_time_str = ''
        if keep_up_to_date:
            any_keep_up_to_date = True
        if query_time is None:
            if keep_up_to_date:
                query_time_str = ''
            else:
                query_time_str = datetime.now().strftime("%Y%m%d %H:%M:%S")
        if chart_options is None:
            chart_options = []
        DEFAULT_HISTORIC_DATA_ID = DEFAULT_HISTORIC_DATA_ID + 1
        historic_data_queue = self.wrapper.historic_data_queue_dict[ticker_id] = queue.Queue()
        historic_data_queue = FinishableQueue(historic_data_queue)
        self.reqHistoricalData(reqId=ticker_id,
                               contract=contract,
                               endDateTime=query_time_str,
                               durationStr=duration,
                               barSizeSetting=bar_size,
                               whatToShow=what_to_show,
                               useRTH=use_rth,
                               formatDate=format_date,
                               keepUpToDate=keep_up_to_date,
                               chartOptions=chart_options)
        max_wait = timeout_limit
        # print("Getting Historical Data for time " + query_time_str)
        historic_data = historic_data_queue.get(timeout=max_wait)
        while self.wrapper.is_error():
            print(self.wrapper.get_error())
        if historic_data_queue.timed_out():
            print("Historic Data Queue Timed Out")
            raise IBAppHistoricDataTimedOutError
        if not any_keep_up_to_date:
            print('Cancelling hist data...')
            self.cancelHistoricalData(ticker_id)
        else:
            self.wrapper.keep_up_to_date_granularity[ticker_id] = bar_size
        if return_as_dict_list: #flag
            historic_data = self.hist_data_to_dict_list(historic_data)
        # if any_keep_up_to_date:
        #     new_closes = []
        #     new_volumes = []
        #
        #     for data_point in historic_data:
        #         new_closes.append(data_point['c'])
        #         new_volumes.append(data_point['v'])
        #     self.wrapper.closes[ticker_id] = new_closes
        #     self.wrapper.volumes[ticker_id] = new_volumes
        self.wrapper.historic_data_dict[ticker_id] = historic_data
        return historic_data

    @staticmethod
    def hist_data_to_dict_list(hist_data):
        data_list = []
        #print("history data CHECK", hist_data[0][0], type(hist_data[0][0]))
        for j in hist_data:
            data_list.append({'time': datetime.strptime(j[0], '%Y%m%d %H:%M:%S'),
                              'o': j[1],
                              'h': j[2],
                              'l': j[3],
                              'c': j[4],
                              'v': j[5]})
        return data_list

    def start_mkt_data(self, resolved_contract: IBContract, ticker_id=DEFAULT_MARKET_DATA_ID, feed_type='full'):
        self._market_data_q_dict[ticker_id], self._mkt_data_dict[ticker_id] = self.wrapper.init_market_data(ticker_id)
        self.wrapper.market_data_contracts[ticker_id] = resolved_contract
        self.reqMktData(ticker_id, resolved_contract, "", False, False, [])
        return ticker_id

    def get_mkt_data(self, ticker_id):
        max_wait = 5
        mkt_data_q = self._market_data_q_dict[ticker_id]
        mkt_data = []
        finished = False
        while not finished:
            try:
                mkt_data.append(mkt_data_q.get(timeout=max_wait))
            except queue.Empty:
                finished = True
        return StreamOfTicks(mkt_data)

    def get_recent_ticks(self, ticker_id, no_of_ticks=70):
        max_wait = 5
        mkt_data_q = self._market_data_q_dict[ticker_id]
        mkt_data = []
        finished = False
        while not finished:
            try:
                mkt_data.append(mkt_data_q.get(timeout=max_wait))
                if len(mkt_data) > no_of_ticks:
                    return StreamOfTicks(mkt_data)
            except queue.Empty:
                finished = True
        return StreamOfTicks(mkt_data)

    @staticmethod
    def mkt_df_to_dict_list(pandas_mkt_df):
        data_list = []
        for i in range(len(pandas_mkt_df)):
            di = {}
            for tick_type in TickTypeEnum.idx2name.values():
                di[tick_type] = pandas_mkt_df.iloc[i][tick_type]
            data_list.append(di)
        return data_list

    def get_tick(self, ticker_id):
        return self._mkt_data_dict[ticker_id]

    def get_tick_as_pd(self, ticker_id):
        return self._mkt_data_dict[ticker_id].as_pd_row()

    def get_ltp(self, ticker_id):
        return self._mkt_data_dict[ticker_id].LAST

    def get_lts(self, ticker_id):
        return self._mkt_data_dict[ticker_id].LAST_SIZE

    def get_bid(self, ticker_id):
        return self._mkt_data_dict[ticker_id].BID

    def get_bid_size(self, ticker_id):
        return self._mkt_data_dict[ticker_id].BID_SIZE

    def get_ask(self, ticker_id):
        # print('---------------------------------------------------------------------------------')
        # # print(ticker_id)
        # # print()
        # # print(self._mkt_data_dict)
        # print()
        # print(self._mkt_data_dict[ticker_id].__dict__)
        # print()
        # print('----------------------------------------------------------------------------------')
        return self._mkt_data_dict[ticker_id].ASK

    def get_ask_size(self, ticker_id):
        return self._mkt_data_dict[ticker_id].ASK_SIZE

    def get_last_bid_df(self, bid_ticker_id, no_of_ticks=100):
        bid_ticks = self.get_recent_ticks(bid_ticker_id, no_of_ticks=no_of_ticks)
        bid_ticks = bid_ticks.as_pd_data_frame()
        bid_ticks = bid_ticks.resample('180S', label='right').last()
        bid_price = bid_ticks.iloc[-1]['BID']
        if np.isnan(bid_price):
            bid_price = self.get_last_bid_df(bid_ticker_id)
        return bid_price

    def get_highest_bid_df(self, bid_ticker_id, no_of_ticks=100):
        bid_ticks = self.get_recent_ticks(bid_ticker_id, no_of_ticks=no_of_ticks)
        bid_ticks = bid_ticks.as_pd_data_frame()
        bid_ticks = bid_ticks.resample('180S', label='right').max()
        bid_price = bid_ticks.iloc[-1]['BID_PRICE']
        if np.isnan(bid_price):
            bid_price = self.get_last_bid_df(bid_ticker_id)
        return bid_price

    def get_last_ask_df(self, ask_ticker_id, no_of_ticks=100):
        ask_ticks = self.get_recent_ticks(ask_ticker_id, no_of_ticks=no_of_ticks)
        ask_ticks = ask_ticks.as_pd_data_frame()
        ask_ticks = ask_ticks.resample('180S', label='right').last()
        ask_price = ask_ticks.iloc[-1]['ASK']
        if np.isnan(ask_price):
            ask_price = self.get_last_ask_df(ask_ticker_id)
        return ask_price

    def get_lowest_ask_df(self, ask_ticker_id, no_of_ticks=100):
        ask_ticks = self.get_recent_ticks(ask_ticker_id, no_of_ticks=no_of_ticks)
        ask_ticks = ask_ticks.as_pd_data_frame()
        ask_ticks = ask_ticks.resample('180S', label='right').min()
        ask_price = ask_ticks.iloc[-1]['ASK_PRICE']
        if np.isnan(ask_price):
            ask_price = self.get_last_ask_df(ask_ticker_id)
        return ask_price

    def get_ltp_df(self, ltp_ticker_id, no_of_ticks=100):
        ltp_ticks = self.get_recent_ticks(ltp_ticker_id, no_of_ticks=no_of_ticks)
        ltp_ticks = ltp_ticks.as_pd_data_frame()
        ltp_ticks = ltp_ticks.resample('180S', label='right').last()
        ltp_price = ltp_ticks.iloc[-1]['LAST']
        if np.isnan(ltp_price):
            ltp_price = self.get_ltp_df(ltp_ticker_id)
        return ltp_price

    def get_highest_ltp_df(self, ltp_ticker_id, no_of_ticks=100):
        ltp_ticks = self.get_recent_ticks(ltp_ticker_id, no_of_ticks=no_of_ticks)
        ltp_ticks = ltp_ticks.as_pd_data_frame()
        ltp_ticks = ltp_ticks.resample('180S', label='right').max()
        ltp_price = ltp_ticks.iloc[-1]['LAST']
        if np.isnan(ltp_price):
            ltp_price = self.get_ltp_df(ltp_ticker_id)
        return ltp_price

    def get_lowest_ltp_df(self, ltp_ticker_id, no_of_ticks=100):
        ltp_ticks = self.get_recent_ticks(ltp_ticker_id, no_of_ticks=no_of_ticks)
        ltp_ticks = ltp_ticks.as_pd_data_frame()
        ltp_ticks = ltp_ticks.resample('180S', label='right').min()
        ltp_price = ltp_ticks.iloc[-1]['LAST']
        if np.isnan(ltp_price):
            ltp_price = self.get_ltp_df(ltp_ticker_id)
        return ltp_price

    def stop_mkt_data(self, ticker_id):
        self.cancelMktData(ticker_id)
        time.sleep(0.5)
        # mkt_data = self.get_mkt_data(ticker_id)
        # while self.wrapper.is_error():
        #     print(self.wrapper.get_error())
        # return mkt_data

    @staticmethod
    def bar_size_to_min_converter(bar_size):
        units = bar_size.split()[1]
        size = int(bar_size.split()[0])
        if units == 'min' or units == 'mins':
            return size
        elif units == 'hour':
            return size * 60
        elif units == 'day':
            return size * 24 * 60

    def hist_data_mkt_update(self, contract: IBContract,
                             ticker_id=DEFAULT_HIST_STREAM_DATA_ID,
                             query_time=None,
                             duration="1 D",
                             bar_size="1 min",
                             what_to_show="TRADES",
                             use_rth=1,
                             format_date=1,
                             chart_options=None,
                             return_as_dict_list=True,
                             timeout_limit=30,
                             ):
        self.wrapper.mkt_keep_up_to_date_ids.add(ticker_id)
        hist_data = self.get_historic_data(contract=contract,
                                           ticker_id=ticker_id,
                                           query_time=query_time,
                                           duration=duration,
                                           bar_size=bar_size,
                                           what_to_show=what_to_show,
                                           use_rth=use_rth,
                                           format_date=format_date,
                                           keep_up_to_date=False,
                                           chart_options=chart_options,
                                           return_as_dict_list=return_as_dict_list,
                                           timeout_limit=timeout_limit,
                                           any_keep_up_to_date=True)
        self.wrapper.keep_up_to_date_granularity[ticker_id] = self.bar_size_to_min_converter(bar_size)
        exchange_data = Exchanges.__getattribute__(Exchanges(), contract.exchange)
        self.wrapper.mkt_data_update_candle_start_points[ticker_id] = self.generate_bar_start_points(exchange_data['start_time'], exchange_data['end_time'], self.wrapper.keep_up_to_date_granularity[ticker_id], calculation_style='ib')
        # print(self.wrapper.mkt_data_update_candle_start_points[ticker_id])
        
        if ticker_id not in self.wrapper.mkt_keep_up_to_date_last_update:
            # print(f"Key {ticker_id} missing, adding a it now to last_update with value {hist_data[-1]['time']} \n")
            self.wrapper.mkt_keep_up_to_date_last_update[ticker_id] = hist_data[-1]['time']
        for data in hist_data:
            try:
                self.wrapper.closes[ticker_id].append(data['c'])
                self.wrapper.opens[ticker_id].append(data['o'])
                self.wrapper.highs[ticker_id].append(data['h'])
                self.wrapper.lows[ticker_id].append(data['l'])
                self.wrapper.times[ticker_id].append(data['time'])
            except KeyError:
                self.wrapper.closes[ticker_id] = [data['c']]
                self.wrapper.opens[ticker_id] = [data['o']]
                self.wrapper.highs[ticker_id] = [data['h']]
                self.wrapper.lows[ticker_id] = [data['l']]
                self.wrapper.times[ticker_id] = [data['time']]
        candles_to_remove = 0
        for candle_start_time in self.wrapper.mkt_data_update_candle_start_points[ticker_id]:
            if self.wrapper.times[ticker_id][-1] > candle_start_time:
                candles_to_remove = candles_to_remove + 1
            else:
                break
        self.wrapper.mkt_data_update_candle_start_points[ticker_id] = self.wrapper.mkt_data_update_candle_start_points[ticker_id][candles_to_remove:]
        # print(f'Candle start points are = {self.wrapper.mkt_data_update_candle_start_points[ticker_id]} \n')
        # print(f'Starting mkt data for {ticker_id}...')
        self.start_mkt_data(contract, ticker_id=ticker_id)

    def get_option_computation(self, req_id):
        return self.wrapper.tick_option_computation[req_id]

    def get_next_broker_order_id(self):
        order_id_q = self.wrapper.order_id_data = queue.Queue()
        self.reqIds(-1)
        max_wait = 5
        try:
            broker_order_id = order_id_q.get(timeout=max_wait)
        except queue.Empty:
            print("Wrapper timed out- Waited for broker orderId")
            broker_order_id = TIME_OUT
        while self.wrapper.is_error():
            print(self.wrapper.get_error(timeout=max_wait))
        return broker_order_id

    def place_new_order(self, contract: IBContract, order: Order, order_id=None):
        ib_order = IBOrder()
        ib_order.action = order.action
        ib_order.totalQuantity = order.quantity
        ib_order.orderType = order.order_type
        ib_order.tif = order.validity
        ib_order.eTradeOnly = False
        ib_order.firmQuoteOnly = False
        
        if order.order_type == 'LMT':
            # ib_order.lmtPrice = order.lmt_price
            ib_order.lmtPrice = order.lmt_price
            print(f'order is of type LMT and limit price is {order.lmt_price}')
        if order_id is None:
            # print("Getting order ID")
            order_id = Client.current_order_id[self.port_id]
            Client.current_order_id[self.port_id] = Client.current_order_id[self.port_id] + 1
            while order_id is TIME_OUT:
                print('OrderID timed out... Trying again in 1μs...')
                time.sleep(1E-6)
                order_id = self.get_next_broker_order_id()
                # raise Exception("Couldn't get order ID nor provided with one")
        # print(f"Using order id - {order_id}")
        
        self.wrapper.orders[order_id] = OrderInformation(contract=contract, order=ib_order, order_id=order_id, order_state=OrderState())
        # Convert Order to IBOrder here...
        self.placeOrder(order_id, contract, ib_order)
        return order_id

    def any_open_orders(self):
        return len(self.get_open_orders()) > 0

    def get_open_orders(self):  # TODO: Order handling is VERY VERY dirty... NEED TO IMPROVE THIS
        start_time = datetime.now()
        for order_id in self.wrapper.orders:
            self.wrapper.orders[order_id].open_order_end = False
        self.reqOpenOrders()
        max_wait = 15
        while True:
            if len(self.wrapper.orders) == 0:
                return {}
            for order_id in self.wrapper.orders:
                if not self.wrapper.orders[order_id].open_order_end:
                    if (datetime.now() - start_time).seconds > max_wait:
                        print('No open orders found...')
                        return {}
                    continue
                else:
                    return self.wrapper.orders
            time.sleep(0.2)

    def cancel_order(self, order_id):
        self.cancelOrder(order_id)
        start_time = datetime.now()
        max_wait = 10
        finished = False
        while not finished:
            if order_id not in self.wrapper.orders.keys():
                finished = True
            if (datetime.now() - start_time).seconds > max_wait:
                print('Failed to cancel...')
                finished = True

    def cancel_all_orders(self):
        self.reqGlobalCancel()
        start_time = datetime.now()
        max_wait = 30
        finished = False
        while not finished:
            if not self.any_open_orders():
                finished = True
            if (datetime.now() - start_time).seconds > max_wait:
                print("Timed out waiting for confirmation of Global Order Cancel")
                finished = True

    def get_executions_and_commissions(self, req_id=DEFAULT_EXEC_TICKER, execution_filter=ExecutionFilter(), overwrite=False):
        self.wrapper.exec_overwrite_status[req_id] = overwrite
        execution_queue = self.wrapper.requested_executions[req_id] = queue.Queue()
        execution_queue = FinishableQueue(execution_queue)
        self.reqExecutions(req_id, execution_filter)
        max_wait = 10
        execution_queue.get(timeout=max_wait)
        while self.wrapper.is_error():
            print(self.wrapper.get_error())
        if execution_queue.timed_out():
            print("Timed out waiting for executions and commissions")
        while not self.wrapper.executions_completed[req_id]:
            time.sleep(0.02)
        executions = self.wrapper.executions[req_id]
        return executions

    def get_all_trades(self, req_id=DEFAULT_EXEC_TICKER, execution_filter=ExecutionFilter()):
        exec_comm = self.get_executions_and_commissions(req_id=req_id, execution_filter=execution_filter)
        lost_order_state = OrderState()
        lost_order_state.status = "LOST"
        if req_id not in self.requested_trades_dict.keys():
            self.requested_trades_dict[req_id] = {}
        for exec_comm_key in exec_comm.keys():
            if (exec_comm[exec_comm_key].client_id, exec_comm[exec_comm_key].order_id) not in self.requested_trades_dict[req_id].keys():
                exec_list = []
                try:
                    order_details = self.wrapper.orders[exec_comm[exec_comm_key].order_id]
                    if self.wrapper.orders[exec_comm[exec_comm_key].client_id] == exec_comm[exec_comm_key].client_id:
                        self.requested_trades_dict[req_id][(exec_comm[exec_comm_key].client_id, exec_comm[exec_comm_key].order_id)] = {"Order Details": order_details, "Executions": exec_list}
                    else:
                        raise KeyError
                except KeyError:
                    self.requested_trades_dict[req_id][(exec_comm[exec_comm_key].client_id, exec_comm[exec_comm_key].order_id)] = \
                        {"Order Details": OrderInformation(exec_comm[exec_comm_key].order_id, exec_comm[exec_comm_key].contract, IBOrder(), lost_order_state), "Executions": exec_list}
            else:
                self.requested_trades_dict[req_id][(exec_comm[exec_comm_key].client_id, exec_comm[exec_comm_key].order_id)]["Executions"].append(exec_comm[exec_comm_key])

    def get_open_positions(self):
        # self.wrapper.positions_handler.clear()        
        self.reqPositions()

    def get_fundamental_data(self, req_id, contract, report_type='ReportsFinSummary'):
        self.reqFundamentalData(req_id, contract, report_type, [])


class App(Wrapper, Client):
    def __init__(self, ip_address, port_id, client_id, store_market_data_queue=False):
        Wrapper.__init__(self, client_id=client_id)
        Client.__init__(self, wrapper=self, port_id=port_id, client_id=client_id)
        self.disconnect = Client.disconnect
        self.ip_address = ip_address
        self.port_id = port_id
        self.client_id = client_id
        self.wrapper.store_market_data = store_market_data_queue
        self.app_type = 'IB'
        self.connect(ip_address, port_id, client_id)
        thread = Thread(target=self.run)
        thread.start()
        setattr(self, "_thread", thread)
        self.wrapper.errors = queue.Queue()
        if self.isConnected():
            print(f"\n\nConnected @ {self.clock()} \n")
        Client.current_order_id[port_id] = self.get_next_broker_order_id()
        print(f"Current Order ID sequence starting from {Client.current_order_id} \n")

    def set_store_market_data_queue_flag(self, store):
        self.wrapper.store_market_data = store

    def reconnect_if_disconnected(self):
        connected = self.isConnected()
        disconnect_flag = False
        if connected:
            print("NO CONNECTION PROBLEM SO FAR...")
        if not connected:
            self.connect(self.ip_address, self.port_id, self.client_id)
            print("DISCONNECT FACED... TRYING TO RECONNECT...")
            disconnect_flag = True
        time.sleep(10)
        connected = self.isConnected()
        if connected and disconnect_flag:
            print("RECONNECTED...")
