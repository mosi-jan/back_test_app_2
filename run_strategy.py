
from my_lib import PrintColored
from symboldataset import SymbolDataSet
from signal import Signal
from indicators import Indicator


class RunStrategy:
    def __init__(self, database_info, data_count, log_obj, params):

        self.colored_print = PrintColored(default_color='red')

        self.init_error = self.check_params(params)
        if self.init_error is not None:
            return

        # if self.init_error is False:

        self.database_info = database_info
        self.data_count = data_count
        self.log_obj = log_obj
        self.params = params

        self.data_set = SymbolDataSet(database_info=database_info,
                                      en_symbol_12_digit_code=params['en_symbol_12_digit_code'],
                                      start_date_time=params['start_date_time'],
                                      today_date_time=params['today_date_time'],
                                      time_frame=params['time_frame'],
                                      data_type=params['data_type'],
                                      adjust_today_candle=params['adjust_today_candle'],
                                      adjusted_type=params['adjusted_type'],
                                      data_count=data_count,
                                      log_obj=log_obj)

        self.signals = Signal(self.data_set)
        self.ind = Indicator(data_set=self.data_set)

        self.run_strategy_error = None

    def get_init_error(self):
        return self.init_error

    def check_params(self, data):
        keys = ['en_symbol_12_digit_code', 'start_date_time', 'today_date_time', 'time_frame',
                 'data_type', 'adjust_today_candle', 'adjusted_type', 'strategy']

        for k in keys:
            if k not in data:
                return 'invalid key param: {}'.format(k)

        return None

    def a(self):
        #print(self.params['strategy'][0])
        #print(self.params['strategy'][1])
        return self.strategy()
        #return self.run_strategy(strategy_variable=self.params['strategy'][0], strategy_context=self.params['strategy'][1])

    def run_strategy(self, strategy_variable, strategy_context):

        body_1 = '''

self.signals.reset()
self.run_strategy_error = None
'''
        body_2 = '''

for i in range(self.data_set.get_origin_candle_count()):

'''

        nested_str = '    '
        s_v = strategy_variable
        s_c = nested_str + strategy_context.replace('\n', '\n' + nested_str)

        body = body_1 + s_v + body_2 + s_c
        try:
            exec(body)
        except Exception as e:
            self.run_strategy_error = str(e.args)

        if self.run_strategy_error is None:
            # create output
            print('s')
            pass

        return self.run_strategy_error

    def strategy(self):
        self.signals.reset()
        self.run_strategy_error = None

        sma = self.ind.SMA(26)

        for i in range(self.data_set.get_origin_candle_count()):

            if sma.d(i) > i:
                self.signals.buy(i)
            else:
                self.signals.sell(i)

        return self.signals.get_order(1,1)

    def strategy0(self):
        self.signals.reset()
        self.run_strategy_error = None

        macd = MACD(26, 12, 9, self.data_set)

        for i in range(self.data_set.get_origin_candle_count()):

            if macd.macd_line(i) > macd.signal_line(i):
                self.signals.buy(i)

            elif macd.macd_line(i) < macd.signal_line(i):
                self.signals.sell(i)


