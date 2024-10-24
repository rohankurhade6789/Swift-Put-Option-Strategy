import numpy as np


class NTSTrade:

    """ Constructor
    """
    def __init__(self, nts_contract):
        self.contract = nts_contract
        self.current_pos_in_lots = 0
        # self.order_status = None
        # self.last_trade_price = np.nan
        self.is_complete = False
        # self.time_elapsed_since_trade = 0
        # self.last_trade_time = None


    def print(self):
        print(self.contract)
        print('current_pos_in_lots =', self.current_pos_in_lots)
        print('order_status =', self.order_status)
        print('last_trade_price =', self.last_trade_price)
        print('is_complete =', self.is_complete)
        print('last_trade_time =',self.last_trade_time)
        # print('time_elapsed_since_trade =', self.time_elapsed_since_trade)
