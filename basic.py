# from pyNTS.App.lot_map import lot_map
# from pyNTS.App.common import Order, Contract
from lot_map import lot_map
from common import Order, Contract
from threading import Thread
from nts_trade import NTSTrade
from influxdb import InfluxDBClient
# from ib_insync import Stock

import json
import yaml
import collections
from collections.abc import Mapping
import os
import sys
import time
import statistics
import warnings
import pickle
import pandas as pd
# from tqdm import tqdm
warnings.filterwarnings("ignore")


from colorama import init, Style, Fore, Back 
init()

import numpy as np
import statistics as st
from datetime import datetime
from dateutil.relativedelta import relativedelta

stocks_position_closed_list = []

class TechnicalFilters(Thread):
    def __init__(self, config_file_name, broker_type, api_key, token, management_topic, broadcast_topic, kafka_bootstrap_servers, monitor_stocks_class=None, execute_trades_class=None):
        Thread.__init__(self)
        self.status = 'Starting up...'
        self.strategy_version = ''
        self.basic_sleep_time = 5e-5
        if broker_type == 'IB':
            from IB_app import App
            self.app = App(ip_address=api_key, port_id=int(token.split(':')[0]), client_id=int(token.split(':')[1]))
        else:
            pass 
        if monitor_stocks_class is None:
            self.monitor_class = Monitor
        else:
            self.monitor_class = monitor_stocks_class
        if execute_trades_class is None:
            self.execute_trades_class = ExecuteTrades
        else:
            self.execute_trades_class = execute_trades_class
            
        #TODO : Reading the yaml file.
        self.strat_config = yaml.safe_load(open(f"{config_file_name}.yaml"))
        # print(self.strat_config)

        defaults = self.strat_config['default']
        self.portfolio_settings = self.strat_config['portfolio']
        del self.strat_config['portfolio']
        del self.strat_config['default']
        self.threads = min(1, 4)
        # print(f'Threads = {self.threads} \n')

        def update(d, u):
            for k, v in u.items():
                if isinstance(v, Mapping):
                    d[k] = update(d.get(k, {}), v)
                else:
                    d[k] = v
            return d

        for stk in self.strat_config.keys():
            temp_dict = defaults.copy()
            temp_dict = update(temp_dict, self.strat_config[stk])
            self.strat_config[stk] = temp_dict

        self.nts_contracts = {}
        self.broker_contracts = {}
        self.stock_contracts = {}
        self.open_positions = {}
        self.open_lots = {}
        self.average_cost = {}
        self.trade_data = {}
        self.order_id_no = {}
        self.portfolio_stk_position = {}
        self.top_stock_list = 0
        assign_mkt_id_1 = 201
        
        #TODO : use to get nxt key (stock).
        self.test_key = next(iter(self.strat_config))
        
        #TODO : Some important times
        mkt_open_hour = int(self.strat_config[self.test_key]['contract']['mkt_open'].split(':')[0])
        mkt_open_min = int(self.strat_config[self.test_key]['contract']['mkt_open'].split(':')[1])
        mkt_open_sec = int(self.strat_config[self.test_key]['contract']['mkt_open'].split(':')[2])
        mkt_close_hour = int(self.strat_config[self.test_key]['contract']['mkt_close'].split(':')[0])
        mkt_close_min = int(self.strat_config[self.test_key]['contract']['mkt_close'].split(':')[1])
        mkt_close_sec = int(self.strat_config[self.test_key]['contract']['mkt_close'].split(':')[2])
        
        position_closing_start_hour = int(self.strat_config[self.test_key]['contract']['position_closing_start'].split(':')[0])
        position_closing_start_min = int(self.strat_config[self.test_key]['contract']['position_closing_start'].split(':')[1])
        position_closing_start_sec = int(self.strat_config[self.test_key]['contract']['position_closing_start'].split(':')[2])
        position_closing_end_hour = int(self.strat_config[self.test_key]['contract']['position_closing_end'].split(':')[0])
        position_closing_end_min = int(self.strat_config[self.test_key]['contract']['position_closing_end'].split(':')[1])
        position_closing_end_sec = int(self.strat_config[self.test_key]['contract']['position_closing_end'].split(':')[2])
        
        position_taking_start_hour = int(self.strat_config[self.test_key]['contract']['position_taking_start'].split(':')[0])
        position_taking_start_min = int(self.strat_config[self.test_key]['contract']['position_taking_start'].split(':')[1])
        position_taking_start_sec = int(self.strat_config[self.test_key]['contract']['position_taking_start'].split(':')[2])
        
        tz_offset_hour = 0
        tz_offset_min = 0
        
        self.mkt_open = datetime.now().replace(hour=mkt_open_hour, minute=mkt_open_min, second=mkt_open_sec, microsecond=0) + relativedelta(hours=tz_offset_hour, minutes=tz_offset_min)
        self.mkt_close = datetime.now().replace(hour=mkt_close_hour, minute=mkt_close_min, second=mkt_close_sec, microsecond=0) + relativedelta(hours=tz_offset_hour, minutes=tz_offset_min)
        self.pre_mkt_close = datetime.now().replace(hour=mkt_close_hour, minute=mkt_close_min - 1, second=mkt_close_sec + 20, microsecond=0) + relativedelta(hours=tz_offset_hour, minutes=tz_offset_min)
        
        self.position_closing_start = datetime.now().replace(hour=position_closing_start_hour, minute=position_closing_start_min, second=position_closing_start_sec, microsecond=0) + relativedelta(hours=tz_offset_hour, minutes=tz_offset_min)
        self.position_closing_end = datetime.now().replace(hour=position_closing_end_hour, minute=position_closing_end_min, second=position_closing_end_sec, microsecond=0) + relativedelta(hours=tz_offset_hour, minutes=tz_offset_min)
        
        self.position_taking_start = datetime.now().replace(hour=position_taking_start_hour, minute=position_taking_start_min, second=position_taking_start_sec, microsecond=0) + relativedelta(hours=tz_offset_hour, minutes=tz_offset_min)
        # print("===========================================================================")
        # print(f'mkt id        : {assign_mkt_id_1} \n')

        #TODO : Converting lot-size file according to IB (9 letters & etc)
        # print(lot_map)
        new_lot_map = {}
        for lot_key in lot_map:
            # print(lot_key)
            if len(lot_key) > 9:
                new_lot_map[lot_key[:9]] = lot_map[lot_key]
                # print(lot_map[lot_key])
            if '&' in lot_key:
                new_key = lot_key.replace('&', '')
                new_lot_map[new_key] = lot_map[lot_key]
        # print(new_lot_map)
        lot_map.update(new_lot_map)
        # print(f'Lot map       : {lot_map} \n')

#TODO : assigning mkt id to each key (stock) & Making contract 
        for stk in self.strat_config.keys():
            
            self.strat_config[stk]['mkt_id'] = (assign_mkt_id_1, )
            new_contract = Contract(self.strat_config[stk]['symbol'], self.strat_config[stk]['type'])
            if new_contract.sec_type == 'FUT' or new_contract.sec_type == 'OPT':
                # print(1)
                self.strat_config[stk]['quantum_type'] = 'lot'
                # print(self.strat_config[stk])
                self.expiry = self.app.get_next_expiry()
                new_contract.expiry = self.expiry
            if new_contract.sec_type == 'STK':
                # print(2)
                self.strat_config[stk]['quantum_type'] = 'exposure'
            elif new_contract.sec_type == 'IND' or new_contract.sec_type == 'INDEX':
                # print(3)
                self.strat_config[stk]['quantum_type'] = None
            self.nts_contracts[stk] = new_contract
            # print(f'New contract for {stk} stock : {new_contract.__dict__}')
            # print(f'Trying to create new contract for- {Style.BRIGHT} {Fore.GREEN} {stk} .. {Style.RESET_ALL}')
            
            # print(self.strat_config[stk])
            # print("=========================================================================================================================================")
            print(stk)
            self.broker_contracts = {}
            self.stock_contracts[stk] = self.app.resolve_contract(new_contract)
            # self.app.get_option_chain(self.stock_contracts[stk], assign_mkt_id_1)   
            assign_mkt_id_1 = assign_mkt_id_1 + 1
            
            print(f'New contract created {stk}- {self.stock_contracts[stk]} \n')    
            self.open_positions[stk] = 0
            self.open_lots[stk] = 0
            self.average_cost[stk] = 0

        self.data_threads = []
        self.updating_portfolio = False
        self.monitor_threads = []
        self.time = 1
    
    def run(self):
        splits = self.split_stocks(self.threads, self.strat_config)
        if datetime.now() >= self.position_closing_end:
            self.update_portfolio()
            self.download_all_historic(splits)  
            top_stocks_list = self.top_stock_list
            for stk in self.strat_config.keys():
                if self.strat_config[stk]['type'] == 'FUT' or self.strat_config[stk]['type'] == 'OPT':
                    self.normalize_exposure(stk)
            print("FIRING UP THE THREADS... \n")
            for ind, stk_group in enumerate(top_stocks_list):
                new_thread = self.monitor_class(stk_group, self)
                self.monitor_threads.append(new_thread)
                new_thread.start()
        elif datetime.now() <= self.position_closing_end:
            for ind, stk_group in enumerate(splits):
                new_thread = self.monitor_class(stk_group, self)
                self.monitor_threads.append(new_thread)
                new_thread.start()

    @staticmethod
    def split_stocks(how_many, strat_config):
        split_op = []
        for i in range(how_many):
            split_op.append([])
        for ind, stk in enumerate(strat_config):
            split_op[ind % how_many].append(stk)
        # print(f'Split into {how_many} groups...')
        for group_no, group in enumerate(split_op):
            # print(f'Group {group_no} has {len(group)} elements...')
            print()
        return split_op

# TODO : to assign lotsize to each stock in yaml file only for FUT & OPT
    def normalize_exposure(self, stk):
        self.strat_config[stk]['quantum_type'] = lot_map[self.strat_config[stk]['symbol']]
        # print(f"checking quantum for {self.strat_config[stk]['symbol']} : {lot_map[self.strat_config[stk]['symbol']]} \n")

    def download_all_historic(self, splits):
        print()
        
        for group in splits:
            # print(group)
            t = Thread(target=self.download_group_hist_data, args=(group, ))
            self.data_threads.append(t)
            
            t.start()
            time.sleep(1)
        for t in self.data_threads:
            t.join()
        # print("Downloaded all hist data groups\n")

    def download_group_hist_data(self, group):
        stk_list = []
        percent_chng_list = []
        for stk in group:
            print(f'Starting historic data download for {Style.BRIGHT} {Fore.GREEN} {stk} {Style.RESET_ALL}')
            self.app.hist_data_mkt_update(
                self.stock_contracts[stk],
                ticker_id=self.strat_config[stk]['mkt_id'][0],
                bar_size=self.strat_config[stk]['candle_size'],
                duration="2 D",
                timeout_limit=30
            )
            # print(self.app.closes[self.strat_config[stk]['mkt_id'][0]])
            time.sleep(0.5)
            # print(f'Downloaded historical data for {Style.BRIGHT} {Fore.GREEN} {stk} {Style.RESET_ALL}')
            
            # TODO : FINDING TOP 50 PERCENT CHANGE STOCKS AND CANCELING SUBCRIPTION OF MKT DATA
            # TODO : AFTERWARD PUT ALL THIS IN NEW CLASS AND MAKE GOOD CODE.
            stk_list.append(stk)
            price_recent = self.app.closes[self.strat_config[stk]['mkt_id'][0]][-1]
            price_yesterday = self.app.closes[self.strat_config[stk]['mkt_id'][0]][24]
            percent_chng = (((price_recent - price_yesterday) / price_yesterday) * 100) 
            percent_chng_list.append(percent_chng)
            
            self.app.stop_mkt_data(self.strat_config[stk]['mkt_id'][0]) 

        
        df = pd.DataFrame({'Stock' : stk_list, '% Change' : percent_chng_list})
        df_new = df.sort_values(by='% Change', ascending=False)
        top_stk_list = df_new[:50]['Stock'].to_list()
        print()
        print(df_new[:50])
        print()
        self.top_stock_list = [top_stk_list]
        # print(self.top_stock_list)

    def write_trade_data(self):
        file = 'strategy.trade_data'
        with open(file, 'wb') as f:
            pickle.dump(self.trade_data, f)

    def update_portfolio(self, forced_data=None):
        while self.updating_portfolio:  # Ensuring thread safety
            time.sleep(1)
        self.updating_portfolio = True
        temp = self.app.get_open_positions()
        # print(f"Printing out open position : {temp} \n")
        time.sleep(1)
        for stk in self.strat_config:
            try:
                # print('||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
                # print(self.portfolio_stk_position)
                # TODO : Dataframe having all portfolio's stocks information.
                pos = self.app.positions_handler.positions
                # print(f'pos : {pos} \n')
                # TODO : details of contract of specific stock.
                try:
                    con = self.broker_contracts[stk]
                    # print(f'con : {con} \n')
                except:
                    # print(f'{stk} CON_ID NOT FOUND')
                    # print()
                    pass
                # TODO : getting information of specific stock in portfolio. 
                try:
                    stk_pos = pos[pos['con_id'] == con.conId].to_dict(orient='records')[0]
                    # print(f'stk_pos : {stk_pos} \n')
                except:
                    # print(f'{stk} HAS NO POSITION IN PORTFOLIO')
                    pass
                # TODO : getting information of specific stock holding no of position in portfolio. 
                try:
                    self.open_positions[stk] = stk_pos['position']
                    # print(f"OPEN Positions of {stk} = {self.open_positions[stk]} \n")
                except:
                    self.open_positions[stk] = self.portfolio_stk_position[stk]
                    # print(f"OPEN Positions of {stk} = {self.open_positions[stk]} \n")
                # TODO : If it is FUT/OPT then select lotsize or go with single position in spot market. 
                # print(self.strat_config[stk])
                if self.strat_config[stk]['contract']['security_type'] == 'FUT' or self.strat_config[stk]['contract']['security_type'] == 'OPT':
                    # print(lot_map[self.strat_config[stk]['symbol']])
                    # self.open_lots[stk] = stk_pos['position'] / lot_map[self.strat_config[stk]['symbol']]
                    self.open_lots[stk] = self.portfolio_stk_position[stk] / lot_map[self.strat_config[stk]['symbol']]
                    
                    # print(f'''$$$$$$
                    # =============
                    # OPEN LOTS OF {stk} = {self.open_lots[stk]}
                    # =============
                    #     $$$$$$''')
                else:
                    self.open_lots[stk] = self.portfolio_stk_position[stk]
                    
                    # print(f'''$$$$$$
                    # =============
                    # OPEN LOTS OF {stk} = {self.open_lots[stk]}
                    # =============
                    #     $$$$$$''')
                # TODO : Average cost of specific stock in portfolio.
                # self.average_cost[stk] = stk_pos['avg_cost']
                # print(f"\nAVERAGE cost {stk} = {self.average_cost[stk]}\n")
                # print('||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
            except (KeyError, IndexError):
            # except Exception as e:
            #     print(e)
                self.open_positions[stk] = 0
                self.open_lots[stk] = 0
                self.average_cost[stk] = 0
                # print(f"\n AVERAGE cost {stk} = {self.average_cost[stk]} EXCEPTION \n")
                # print('||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
        if forced_data is not None:
            for stk in forced_data:
                self.open_positions[stk] = forced_data[stk]['position']
                # print('================================================================')
                # print(lot_map[self.strat_config[stk]['symbol']])
                # print('================================================================')
                self.open_lots[stk] = forced_data[stk]['position'] / lot_map[self.strat_config[stk]['symbol']]
                self.average_cost[stk] = forced_data[stk]['avg_cost']
                # print(f'''Forced_data inside For loop {stk}
                # Open positions = {self.open_positions[stk]}
                # Open Lots = {self.open_lots[stk]}
                # Average cost = {self.average_cost[stk]}''')\
        self.updating_portfolio = False
        time.sleep(2)


class Monitor(Thread):

    def __init__(self, stocks_list, main_strat_obj: TechnicalFilters):
        super().__init__()
        self.stocks_list = stocks_list
        self.stocks_list_copy = self.stocks_list
        self.forced_sleep_time = 1/len(stocks_list)
        self.main_strat = main_strat_obj
        self.strat_config = main_strat_obj.strat_config
        self.app = main_strat_obj.app
        self.stock_functions = {}
        self.stocks_on_hold = {}
        self.highest_pnl_perc = {}
        self.highest_pnl = {}
        self.last_traded_stk_price = {}
        self.stock_state = {}
        self.prev_price = {}
        self.full_state_details = {}
        self.trade_new_data = {}

        self.stop_loss = {}
        self.stop_flag = False
        self.terminate_flag = False
        self.candle_mins = {}
        self.peak_price = {}
        self.print_at = {}
        self.last_entered_time = {}
        self.position_type = {}
        self.candle_count = {}
        self.strat_config_map = {}
        self.last_check_side = {}
        self.close_pos_req_id = {}
        self.order_ids = {}
        self.flag = 1
        
        self.assign_mkt_id_1 = 301
        print('\n')
                    
        if self.main_strat.position_closing_start <= datetime.now() <= self.main_strat.position_closing_end:
            self.read_trade_data()
            print('flag 1')
            for stk in self.main_strat.trade_data:
                stocks_position_closed_list.append(stk) 
                self.app.hist_data_mkt_update(
                    self.main_strat.trade_data[stk].contract,
                    ticker_id=self.close_pos_req_id[stk],
                    bar_size=self.strat_config[stk]['candle_size'],
                    # bar_size="1 min",
                    duration="1 D",
                    timeout_limit=10
                )
            # start_time = time.time()
            while len(stocks_position_closed_list) > 0:        
                print(f'Stocks for trading : {stocks_position_closed_list}')
                # if start_time in []
                for stk in stocks_position_closed_list:
                    self.check_signal(self.close_pos_req_id[stk], stk)
                    self.close_position(stk, self.close_pos_req_id[stk])
                time.sleep(20)
                for stk in stocks_position_closed_list:
                    self.closing_order_status(stk)
                    time.sleep(1)
            
        elif datetime.now() <= self.main_strat.position_closing_start:
            self.read_trade_data()
            print('flag 2')
            print(f"Waiting for the market to open...")
            time.sleep((self.main_strat.position_closing_start - datetime.now()).seconds)
            
            for stk in self.main_strat.trade_data:
                stocks_position_closed_list.append(stk) 
                self.app.hist_data_mkt_update(
                    self.main_strat.trade_data[stk].contract,
                    ticker_id=self.close_pos_req_id[stk],
                    bar_size=self.strat_config[stk]['candle_size'],
                    duration="1 D",
                    timeout_limit=10
                )
            # start_time = time.time()
            while len(stocks_position_closed_list) > 0:        
                print(f'Stocks for trading : {stocks_position_closed_list}')
                # if start_time in []
                for stk in stocks_position_closed_list:
                    self.check_signal(self.close_pos_req_id[stk], stk)
                    self.close_position(stk, self.close_pos_req_id[stk])
                time.sleep(20)
                for stk in stocks_position_closed_list:
                    self.closing_order_status(stk)
                    time.sleep(1)
            
        elif datetime.now() >= self.main_strat.mkt_close:
            print(f"Market Closed...")
            
        else:
            pass
            
        file =  'strategy.trade_data'
        if os.path.isfile(file):
            os.remove(file)
        time.sleep(60)
            
        # self.main_strat.write_trade_data()    
        
        for stk in self.stocks_list:

            self.stock_functions[stk] = {
                # TODO: Enter your technical functions map here
            }

            self.stock_state[stk] = self.get_stock_state(stk)
            self.last_entered_time[stk] = ''
            self.position_type[stk] = ''
            self.print_at[stk] = 0
            self.last_check_side[stk] = 0

            self.strat_config_map[stk] = {}                                     # FOR EXAMPLE = {'STK_NAME': MKT_ID OF OPTIONS}
            mkt_id = self.strat_config[stk]['mkt_id'][0]                        # MKT_ID : MKT_ID OF THE STK NOT MKT_ID OF OPTIONS
            # print(f'STOCK == {stk}')
            # print(f'MARKET_ID == {mkt_id}')
            # self.creating_option_contracts(stk, mkt_id)                         # CLASS WHERE YOU CREATED OPTION CONTRACT AND REQUESTED HISTORICAL DATA
            # self.candle_count[stk] = len(self.app.closes[mkt_id])               # COUNT THE TOTAL NO OF CANDLE GENERATED TILL NOW
            
        # self.main_strat.update_portfolio()
            
        self.state_map = {
            'waiting to enter': self.check_signal,
            'entered': self.check_signal,
        }
        self.whisper_state_map = {
            'Entered': 'entered',
            'Exited': 'waiting to enter'
        }
        # self.getting_top_ten()

    def get_stock_state(self, stk):  # ONLY USED AS REDUNDANCY IF FILE IS NOT FOUND... Should be called very rarely...
        if self.main_strat.open_positions[stk] == 0:
            return 'waiting to enter'
        else:
            return 'entered'


    def run(self): # Change according to strategy
        
        print("stocks are : ", self.stocks_list)
        print()
        
        for stk in self.stocks_list:
            self.app.get_option_chain(self.main_strat.stock_contracts[stk], self.strat_config[stk]['mkt_id'][0])  
        time.sleep(5) 
                
        while datetime.now() <= self.main_strat.position_taking_start:
            print(f'Trading begins at {self.main_strat.position_taking_start}')
            time.sleep((self.main_strat.position_taking_start - datetime.now()).seconds)
            
            if self.flag == 1:
                self.main_strat.download_all_historic([self.stocks_list])  
                for i in self.main_strat.top_stock_list:
                    print('--------------')
                    print(i)
                    self.stocks_list = i[:5]
                    print(self.stocks_list)
                    print('--------------')
                                        
                for stk in self.stocks_list:
                    self.creating_option_contracts(stk, self.strat_config[stk]['mkt_id'][0])                         # CLASS WHERE YOU CREATED OPTION CONTRACT AND REQUESTED HISTORICAL DATA
                    self.candle_count[stk] = len(self.app.closes[self.strat_config[stk]['mkt_id'][0]])               # COUNT THE TOTAL NO OF CANDLE GENERATED TILL NOW
                self.main_strat.update_portfolio()
                self.flag = 0
            
            for stk_ind, stk in enumerate(self.stocks_list):
                signal, order_desc, price_at_trigger, stp_limit, exec_type, addn_data = self.state_map[self.stock_state[stk]](self.strat_config_map[stk], stk)
                if (signal == 1) or (signal == -1):
                    self.last_entered_time[stk] = datetime.now().strftime('%H:%M:%S')
                    if 'ENTRY' in order_desc:
                        self.position_type[stk] = "E"
                    if 'SL' in order_desc:
                        self.position_type[stk] = "SL"
                    if 'REVERSAL' in order_desc:
                        self.position_type[stk] = "R"
                    # print(f'stk : {stk}')
                    # new_execute_thread = self.main_strat.execute_trades_class(stk, signal , self, order_desc, stp_limit, signal, exec_type=exec_type)
                    new_execute_thread = self.main_strat.execute_trades_class(stk, signal * lot_map[self.strat_config[stk]['symbol']], self, order_desc, stp_limit, signal, exec_type=exec_type, addn_data=addn_data)
                    new_execute_thread.start()
                    self.stocks_on_hold[stk] = stk
                    time.sleep(1)
                    del self.stocks_list[stk_ind]
                    # print("stocks list ", self.stocks_list)
                    
        while self.main_strat.position_taking_start <= datetime.now():
            
            if self.flag == 1:
                self.main_strat.download_all_historic([self.stocks_list])  
                for i in self.main_strat.top_stock_list:
                    print('==================')
                    print(i)
                    self.stocks_list = i[:5]
                    print(self.stocks_list)
                    print('===================')
                     
                for stk in self.stocks_list:
                    self.creating_option_contracts(stk, self.strat_config[stk]['mkt_id'][0])                         # CLASS WHERE YOU CREATED OPTION CONTRACT AND REQUESTED HISTORICAL DATA
                    self.candle_count[stk] = len(self.app.closes[self.strat_config[stk]['mkt_id'][0]])               # COUNT THE TOTAL NO OF CANDLE GENERATED TILL NOW
                self.main_strat.update_portfolio()
                self.flag = 0
            
            for stk_ind, stk in enumerate(self.stocks_list):
                signal, order_desc, price_at_trigger, stp_limit, exec_type, addn_data = self.state_map[self.stock_state[stk]](self.strat_config_map[stk], stk)
                if (signal == 1) or (signal == -1):
                    self.last_entered_time[stk] = datetime.now().strftime('%H:%M:%S')
                    if 'ENTRY' in order_desc:
                        self.position_type[stk] = "E"
                    if 'SL' in order_desc:
                        self.position_type[stk] = "SL"
                    if 'REVERSAL' in order_desc:
                        self.position_type[stk] = "R"
                    # print(f'stk : {stk}')
                    # new_execute_thread = self.main_strat.execute_trades_class(stk, signal , self, order_desc, stp_limit, signal, exec_type=exec_type)
                    new_execute_thread = self.main_strat.execute_trades_class(stk, signal * lot_map[self.strat_config[stk]['symbol']]
                                                                              * int(self.strat_config[stk]['contract']['contract_size']), self, order_desc, stp_limit,
                                                                              signal, exec_type=exec_type, addn_data=addn_data)
                    new_execute_thread.start()
                    self.stocks_on_hold[stk] = stk
                    time.sleep(1)
                    del self.stocks_list[stk_ind]
                    print("stocks list ", self.stocks_list)
                      
    def read_trade_data(self):
        file = 'strategy.trade_data'
        if not os.path.exists(file):
            return
        with open(file, 'rb') as f:
            trade_data = pickle.load(f)
        # print(trade_data)
        for stk in trade_data:
            self.close_pos_req_id[stk] = self.assign_mkt_id_1
            self.assign_mkt_id_1 += 1
            if stk not in self.main_strat.trade_data:
                self.main_strat.trade_data[stk] =  NTSTrade(trade_data[stk].contract)
                self.main_strat.trade_data[stk].current_pos_in_lots = trade_data[stk].current_pos_in_lots
            # print(f"Current TRADE DATA {self.main_strat.trade_data[stk].__dict__}")
            if self.main_strat.trade_data[stk].contract.right == 'C' and self.main_strat.trade_data[stk].current_pos_in_lots > 0:
                self.last_check_side[stk] = 1
            elif self.main_strat.trade_data[stk].contract.right == 'P' and self.main_strat.trade_data[stk].current_pos_in_lots < 0:
                self.last_check_side[stk] = -1
 
    def trade_callback(self, stk, filled_order_id, whisper):
        old_state = self.stock_state[stk]
        new_state = self.whisper_state_map[whisper]
        # print(f'|||| {stk} going from {old_state} to {new_state}')
        self.stock_state[stk] = self.whisper_state_map[whisper]
        self.highest_pnl_perc[stk] = self.highest_pnl[stk] = 0.0
        self.stocks_list.append(self.stocks_on_hold[stk])


    def check_signal(self, req_id, stk):
        try:
            price = self.app.closes[req_id][-1]
            bid_price = self.app.get_bid(req_id)
            ask_price = self.app.get_ask(req_id)
            mid_price = (bid_price + ask_price) / 2
            time.sleep(self.main_strat.basic_sleep_time)
            time.sleep(self.forced_sleep_time)
        except:
            price = 0
            # print(f'Price is not available for {stk}')

        # if self.main_strat.open_lots[stk] ==  0 and datetime.now() >= self.main_strat.position_taking_start and price != 0:
        if self.main_strat.position_taking_start <= datetime.now() <= self.main_strat.pre_mkt_close and price != 0:
            return 1, 'ENTRY', price, price, 'LMT', bid_price
        elif self.main_strat.pre_mkt_close <= datetime.now() <= self.main_strat.mkt_close and price != 0:
            return 1, 'ENTRY', price, price, 'MKT', bid_price
        else:
            return 0, 'ENTRY', price, price, 'LMT', bid_price
    
    def creating_option_contracts(self, stk, req_id):
        side = ['C', 'P']

        atm_strike = min(self.app.strike_chain[req_id], key=lambda x: abs(x - self.app.closes[req_id][-1]))
        
        new_contract = Contract(self.strat_config[stk]['symbol'], self.strat_config[stk]['contract']['security_type'])
        expiry = self.app.get_next_expiry()
        
        no_of_contract = int(self.strat_config[stk]['contract']['contract_size'])
        
        self.strat_config_map[stk] = self.assign_mkt_id_1
        
        # new_contract.req_id = req_id
        new_contract.expiry = expiry
        new_contract.strike = atm_strike
        new_contract.right = 'P'

        self.main_strat.broker_contracts[stk] = self.app.resolve_contract(new_contract)
        
        print(f'Resolved contract for {stk} : {self.main_strat.broker_contracts[stk]}')
        
        print(f'Starting historic options data download for {Style.BRIGHT} {Fore.GREEN} {stk} {Style.RESET_ALL}')
        try:
            self.app.hist_data_mkt_update(
                self.main_strat.broker_contracts[stk],
                ticker_id=self.assign_mkt_id_1,
                bar_size=self.strat_config[stk]['candle_size'],
                duration="1 D",
                timeout_limit=5
            )
        except:
            print("Market data is not available.")
            
        self.assign_mkt_id_1 += 1
        
        print(f'New contract created- {Style.BRIGHT} {Fore.GREEN} {stk} {Style.RESET_ALL} \n')
        
    def close_position(self, stk, req_id):    
        
        pos = self.main_strat.trade_data[stk].current_pos_in_lots
        lotsize = lot_map[self.strat_config[stk]['symbol']]

        price_ltp = int(self.app.closes[req_id][-1])
        bid = self.app.get_bid(req_id)
        ask = self.app.get_ask(req_id)
        mid = (bid + ask) / 2
        spread_BID_ASK = ask - bid  
        diff_percent = ((ask - bid) / ask) * 100
        
        print('')
        print('')
        print(f'STOCK : {stk}')
        # print(f'PRICE : {price_ltp}')
        print(f'BID : {bid}')
        print(f'ASK : {ask}')  
        # print(f'MID : {mid}')  
        # print(f'Spread between BID ASK : {spread_BID_ASK}') 
        
        if ask > 0:
            if pos > 0:
                self.send_order(stk, -1 * lotsize, "ENTRY", price_ltp, -1, ask, addn_data = self.main_strat.trade_data[stk].contract)
            elif pos < 0:
                self.send_order(stk,  1 * lotsize, "ENTRY", price_ltp,  1, ask, addn_data = self.main_strat.trade_data[stk].contract)
            self.last_check_side[stk] = 0  
            # stocks_position_closed_list.remove(stk) 
            # print(stocks_position_closed_list)
        
    def calc_trade_needed(self, stk, position, addn_data):
        trade_to_make = position
        
        if trade_to_make < 0:
            return abs(trade_to_make), 'SELL'
        else:
            return abs(trade_to_make), 'BUY'
        
    def send_order(self, stk, position, order_desc, price_at_trigger, signal, lmt_price, exec_type='LMT', addn_data=None):
        trade_quantity, trade_signal = self.calc_trade_needed(stk, position, addn_data)
        
        change_order = Order(trade_signal, exec_type, trade_quantity, lmt_price)   #where action is trade_signal, order_type is 'MKT' and quantity is trade_quantity
        change_order.product_type = 'NORMAL'
        order_time = datetime.now()
        print(f"{Style.BRIGHT}{Back.WHITE}{Fore.RED} Placing {trade_signal} order for {stk} at {order_time.strftime('%Y-%m-%d %H:%M:%S.%f')} {Style.RESET_ALL}")

        
        self.order_ids['ENTRY_MKT'] = self.app.place_new_order(
            contract=addn_data, 
            order=change_order)
        self.main_strat.order_id_no[stk] = self.order_ids['ENTRY_MKT']

    def closing_order_status(self, stk):
        # try:
        order_details = self.app.orders[self.main_strat.order_id_no[stk]]
        # print(f"Type of order details {type(order_details)} \n\n\n\n properties of order_details {order_details.__dict__}\n\n\n\n")
        print('****************************************************')
        print(f'Order {order_details.status} for {stk}')
        print(f'Order {order_details.filled} for {stk}')

        if order_details.filled > 0:
            print(f'Order is Half Filled for {stk}')
        
        if order_details.status == 'Filled' or order_details.filled > 0:
            print(f'order successful for {stk}')
            stocks_position_closed_list.remove(stk)
        else:
            print(f'order rejected for {stk}')
            self.main_strat.update_portfolio()
            self.main_strat.app.cancel_order(self.main_strat.order_id_no[stk])
            # stocks_position_closed_list.append(stk)
        print('****************************************************')
        print()

        # except:
        #     print(f"{stk} don't have bid ask data")
    def stop(self):
        self.stop_flag = True
  
        
class ExecuteTrades(Thread):

    def __init__(self, stk, position, monitor_parent: Monitor, order_desc, price_at_trigger, signal, exec_type='LMT', 
                 addn_data=None):
        super().__init__()

        # print('hello')
        
        self.stk = stk
        self.order_ids = {}
        self.monitor_parent = monitor_parent            #monitor
        self.trade_app = monitor_parent.app             
        self.position = position
        self.strategy_version = monitor_parent.main_strat.strategy_version
        self.portfolio_stk_position = monitor_parent.main_strat.portfolio_stk_position
        self.order_desc = order_desc
        self.exec_type = exec_type
        self.price_at_trigger = price_at_trigger
        self.signal = signal
        self.addn_data = addn_data
        self.trade_function_map = {
            self.exec_type: self.place_market_order,
        }
        self.desc_whisper_map = {
            'ENTRY': 'Entered',
            'REVERSAL': 'Entered',
            'LONG': 'Entered',
            'SHORT': 'Entered',
            'TP': 'Exited',
            'SL': 'Exited',
        }


    def calc_trade_needed(self):
        #trade_to_make = self.position - self.monitor_parent.main_strat.open_positions[self.stk]
        trade_to_make = self.position
        # print(f'self.stk : {self.stk}')
        # print(f'self.stk : {self.monitor_parent.main_strat.open_positions[self.stk]}')
        
        self.portfolio_stk_position[self.stk] = self.position
        # print('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
        # print(f'POSITION OF {self.stk} : {self.portfolio_stk_position}')
        # print('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
        
        # print(f"{self.stk} || CURRENT STATE {self.monitor_parent.main_strat.open_positions[self.stk]} \n"
        #       f"{self.stk} || NEEDED STATE {self.position} \n"
        #       f"{self.stk} || TRADE TO MAKE {trade_to_make} \n"
        #       f"{self.stk} || LOT SIZE IS {lot_map[self.monitor_parent.strat_config[self.stk]['symbol']]}")
        
        if self.signal == 1:
            return abs(trade_to_make), 'BUY'
        else:
            return abs(trade_to_make), 'SELL'
        # if trade_to_make < 0:
        #     return abs(trade_to_make), 'SELL'
        # else:
        #     return abs(trade_to_make), 'BUY'

    def run(self):
        self.trade_function_map[self.exec_type]()
        time.sleep(30)
        self.order_status()
        time.sleep(2)
        # self.trade_app.cancel_all_orders()

    def place_market_order(self):
        trade_quantity, trade_signal = self.calc_trade_needed()
        
        change_order = Order(trade_signal, self.exec_type, trade_quantity, self.addn_data)   #where action is trade_signal, order_type is 'MKT' and quantity is trade_quantity

        order_time = datetime.now()
        # time.sleep(60)
        print(f"{Style.BRIGHT}{Fore.GREEN} Placing BUY order for {self.stk} at {order_time.strftime('%Y-%m-%d %H:%M:%S.%f')} {Style.RESET_ALL}")

        # print(f'MONITOR_PARENT : {self.monitor_parent.main_strat.broker_contracts[self.stk]}')
        # print(self.stk)
        self.order_ids['ENTRY_MKT'] = self.trade_app.place_new_order(
            contract=self.monitor_parent.main_strat.broker_contracts[self.stk], 
            order=change_order)
        self.monitor_parent.main_strat.order_id_no[self.stk] = self.order_ids['ENTRY_MKT']
        # print(f"Placed order {self.monitor_parent.main_strat.order_id_no[self.stk]}")
        print()

    def order_status(self):   
        order_details = self.monitor_parent.app.orders[self.monitor_parent.main_strat.order_id_no[self.stk]]
        # print(f"Order details ==== {order_details} \n  type of order details {type(order_details)} \n\n\n\n properties of order_details {order_details.__dict__}\n\n\n\n")
        print('****************************************************')
        print(f'Order {order_details.status} for {self.stk}')
        print(f'Order {order_details.filled} for {self.stk}')
        print('****************************************************')

        # if order_details.status == 'Filled' or order_details.status == 'Submitted':
        if order_details.status == 'Filled' or order_details.filled > 0:
            print(f'order successful for {self.stk}')
            
            self.monitor_parent.main_strat.trade_data[self.stk] = NTSTrade(self.monitor_parent.main_strat.broker_contracts[self.stk])
            self.monitor_parent.main_strat.trade_data[self.stk].current_pos_in_lots = self.portfolio_stk_position[self.stk] / lot_map[self.stk]
            self.monitor_parent.main_strat.write_trade_data()  
            self.monitor_parent.last_check_side[self.stk] = 1
            
            self.monitor_parent.main_strat.update_portfolio()
        else:
            print(f'order rejected for {self.stk}')
            f_data = {self.stk: {
                'position': self.position,
                'avg_cost': self.price_at_trigger,
            }}
            self.monitor_parent.main_strat.update_portfolio()
            
            if datetime.now() <= self.monitor_parent.main_strat.mkt_close:
                self.trade_app.cancel_order(self.monitor_parent.main_strat.order_id_no[self.stk])
                self.monitor_parent.stocks_list.append(self.stk)
            elif self.monitor_parent.main_strat.mkt_close < datetime.now():
                self.trade_app.cancel_order(self.monitor_parent.main_strat.order_id_no[self.stk])
                
            
        print(self.monitor_parent.stocks_list)
            
            # self.monitor_parent.trade_callback(self.stk, {'ENTRY_MKT': self.monitor_parent.main_strat.order_id_no[self.stk]},
            #                                 whisper=self.desc_whisper_map[self.order_desc])  
            
            
            
