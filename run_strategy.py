
from my_lib import PrintColored
from symboldataset import SymbolDataSet
from signal import Signal


class RunStrategy:
    def __init__(self, database_info, en_symbol_12_digit_code, start_date_time, today_date_time, time_frame,
                 data_type, adjust_today_candle, adjusted_type, strategy, data_count, log_obj):

        self.colored_print = PrintColored(default_color='red')
        self.strategy = strategy

        self.data_set = SymbolDataSet(database_info=database_info,
                                      en_symbol_12_digit_code=en_symbol_12_digit_code,
                                      start_date_time=start_date_time,
                                      today_date_time=today_date_time,
                                      time_frame=time_frame,
                                      data_type=data_type,
                                      adjust_today_candle=adjust_today_candle,
                                      adjusted_type=adjusted_type,
                                      data_count=data_count,
                                      log_obj=log_obj)

        self.signals = Signal(self.data_set)

        self.run_strategy_error = None

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
            pass

        return self.run_strategy_error

    def strategy(self):
        self.signals.reset()
        self.run_strategy_error = None

        macd = MACD(26, 12, 9, self.data_set)

        for i in range(self.data_set.get_origin_candle_count()):

            if macd.macd_line(i) > macd.signal_line(i):
                self.signals.buy(i)

            elif macd.macd_line(i) < macd.signal_line(i):
                self.signals.sell(i)


