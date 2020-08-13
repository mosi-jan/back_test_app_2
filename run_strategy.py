import numpy as np
from my_lib import PrintColored
from symboldataset import SymbolDataSet
from signal import Signal
from indicators import Indicator

from constants import time_frame_s1, time_frame_m1, time_frame_h1, time_frame_d1, time_frame_mn1, time_frame_y1
from time import sleep

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
                'data_type', 'adjust_today_candle', 'adjusted_type', 'strategy', 'order_same', 'order_total']

        for k in keys:
            if k not in data:
                return 'invalid key param: {}'.format(k)

        return None

    def a(self):
        # print(self.params['strategy'][0])
        # print(self.params['strategy'][1])
        return self.strategy()
        # return self.run_strategy(strategy_variable=self.params['strategy'][0],
        # strategy_context=self.params['strategy'][1])

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

        return self.signals.get_order(1, 1)

    def exec(self,today_index=None, start_index=None, strategy=None):
        if today_index is None:
            today_index = 0
        if start_index is None:
            start_index = self.data_set.get_origin_candle_count()

        # run strategy
        # self.run_strategy(strategy_variable=self.params['strategy'][0], strategy_context=self.params['strategy'][1])
        self.strategy()
        w = self.signals.analyze_order(part=self.params['order_same'], total=self.params['order_total'], split_time_frame=time_frame_y1)
        print(w)
        #order, all_coeff = self.signals.get_order(part=self.params['order_same'], total=self.params['order_total'])
        #print(order)
        #print(all_coeff)
        # create output data
        #q = self.analyze_order(orders=order, split_time_frame=time_frame_y1)
        #print(q)
        #print(q.shape)
        sleep(0.10)

    # -------------------------------------
    def analyze_order(self, orders, split_time_frame):
        buy_date, sell_date, buy_price, sell_price, coeff, candle_count, buy_index, sell_index = range(8)

        result = list()
        # time_frame_start = 0
        time_frame_end = 0

        if len(orders) > 0:
            splited_time_frame = self.splited_time_frame(split_time_frame = split_time_frame,
                                                         start_candle_index=orders[0][buy_index],
                                                         end_candle_index=orders[-1][sell_index])
        else:
            return np.array(0)
        print('splited_time_frame')
        print(splited_time_frame)
        print(len(splited_time_frame))

        for time_frame in splited_time_frame:
            time_frame_start = time_frame[1]

            max_coeff = -100
            min_coeff = 100

            profit_order_count = 0
            profit_order_sum_coeff = 0
            profit_order_sum_candle_count = 0
            profit_order_coeff = 1

            lose_order_count = 0
            lose_order_sum_coeff = 0
            lose_order_sum_candle_count = 0
            lose_order_coeff = 1

            none_pl_order_count = 0
            none_pl_order_sum_candle_count = 0

            for order in orders:
                if order[buy_index] <= time_frame_end:
                    order_start = -1
                    order_end = -1

                elif order[buy_index] <= time_frame_start:
                    order_start = order[buy_index]
                    if order[sell_index] >= time_frame_end:
                        order_end = order[sell_index]
                    else:
                        order_end = time_frame_end
                else:
                    if order[sell_index] >= time_frame_end:
                        order_start = time_frame_start
                        order_end = order[sell_index]

                    else:
                        order_start = -1
                        order_end = -1

                # calculate params
                order_coeff = float(order_start - order_end) / float(order[candle_count])

                if order_coeff <= 0:
                    continue  # next order

                # min max coeff
                if (order[coeff] - 1) * order_coeff + 1 > max_coeff:
                    max_coeff = (order[coeff] - 1) * order_coeff + 1

                if (order[coeff] - 1) * order_coeff + 1 < min_coeff:
                    min_coeff = (order[coeff] - 1) * order_coeff + 1

                # order count
                if (order[coeff] - 1) * order_coeff + 1 > 1:
                    profit_order_count += order_coeff
                    profit_order_sum_coeff += (order[coeff] - 1) * order_coeff + 1
                    profit_order_sum_candle_count += order[candle_count] * order_coeff
                    profit_order_coeff *= (order[coeff] - 1) * order_coeff + 1

                elif (order[coeff] - 1) * order_coeff + 1 < 1:
                    lose_order_count += order_coeff
                    lose_order_sum_coeff += (order[coeff] - 1) * order_coeff + 1
                    lose_order_sum_candle_count += order[candle_count] * order_coeff
                    lose_order_coeff *= (order[coeff] - 1) * order_coeff + 1

                else:
                    none_pl_order_count += order_coeff
                    none_pl_order_sum_candle_count += order[candle_count] * order_coeff

            profit_order_sum_candle_count = int(profit_order_sum_candle_count)
            lose_order_sum_candle_count = int(lose_order_sum_candle_count)
            none_pl_order_sum_candle_count = int(none_pl_order_sum_candle_count)

            if max_coeff == -100:
                max_coeff = 1

            if min_coeff == 100:
                min_coeff = 1
            # ------------------
            total_order_count = profit_order_count + lose_order_count + none_pl_order_count
            total_order_candle_count = profit_order_sum_candle_count + lose_order_sum_candle_count + \
                                       none_pl_order_sum_candle_count

            if total_order_count > 0:
                profit_order_count_percent = profit_order_count / total_order_count
                lose_order_count_percent = lose_order_count / total_order_count
                none_pl_order_count_percent = none_pl_order_count / total_order_count
            else:
                profit_order_count_percent = 0
                lose_order_count_percent = 0
                none_pl_order_count_percent = 0

            if total_order_candle_count > 0:
                profit_order_sum_candle_count_percent = profit_order_sum_candle_count / total_order_candle_count
                lose_order_sum_candle_count_percent = lose_order_sum_candle_count / total_order_candle_count
                none_pl_order_sum_candle_count_percent = none_pl_order_sum_candle_count / total_order_candle_count
            else:
                profit_order_sum_candle_count_percent = 0
                lose_order_sum_candle_count_percent = 0
                none_pl_order_sum_candle_count_percent = 0

            if profit_order_count > 0:
                profit_order_sum_coeff_average = profit_order_sum_coeff / profit_order_count
            else:
                profit_order_sum_coeff_average = 0

            if lose_order_count > 0:
                lose_order_sum_coeff_average = lose_order_sum_coeff / lose_order_count
            else:
                lose_order_sum_coeff_average = 0

            result.append([[time_frame_start, time_frame_end], [total_order_count,
                                                                total_order_candle_count,
                                                                profit_order_count_percent,
                                                                lose_order_count_percent,
                                                                none_pl_order_count_percent,
                                                                profit_order_sum_candle_count_percent,
                                                                lose_order_sum_candle_count_percent,
                                                                none_pl_order_sum_candle_count_percent,
                                                                profit_order_sum_coeff_average,
                                                                lose_order_sum_coeff_average]])

            time_frame_end = time_frame_start

            #print('********************************************************')
            #print('********************************************************')
            #print('total_order: {}'.format(total_order_count))
            #print('total_order_candle_count: {}'.format(total_order_candle_count))
            #print('max_coeff: {}'.format(max_coeff))
            #print('min_coeff: {}'.format(min_coeff))
            #print('-----------')

            #print('profit_order_count: {}'.format(profit_order_count))
            #print('profit_order_sum_coeff: {}'.format(profit_order_sum_coeff))
            #print('profit_order_sum_candle_count: {}'.format(profit_order_sum_candle_count))
            #print('profit_order_coeff: {}'.format(profit_order_coeff))

            #print('-----------')
            #print('lose_order_count: {}'.format(lose_order_count))
            #print('lose_order_sum_coeff: {}'.format(lose_order_sum_coeff))
            #print('lose_order_sum_candle_count: {}'.format(lose_order_sum_candle_count))
            #print('lose_order_coeff: {}'.format(lose_order_coeff))

            #print('-----------')
            #print('none_pl_order_count: {}'.format(none_pl_order_count))
            #print('none_pl_order_sum_candle_count: {}'.format(none_pl_order_sum_candle_count))
            # print('-----------')
            #print('\n+++++++++++++++++++++++++')
            #print('total_order_count: {}'.format(total_order_count))
            #print('total_order_candle_count: {}'.format(total_order_candle_count))
            #print('max_coeff: {}'.format(max_coeff))
            #print('min_coeff: {}'.format(min_coeff))
            #print('profit_order_count_percent: {}'.format(profit_order_count_percent))
            #print('lose_order_count_percent: {}'.format(lose_order_count_percent))
            #print('none_pl_order_count_percent: {}'.format(none_pl_order_count_percent))
            #print('profit_order_sum_candle_count_percent: {}'.format(profit_order_sum_candle_count_percent))
            #print('lose_order_sum_candle_count_percent: {}'.format(lose_order_sum_candle_count_percent))
            #print('none_pl_order_sum_candle_count_percent: {}'.format(none_pl_order_sum_candle_count_percent))
            #print('profit_order_sum_coeff_average: {}'.format(profit_order_sum_coeff_average - 1))
            #print('lose_order_sum_coeff_average: {}'.format(1 - lose_order_sum_coeff_average))

            #print('+++++++++++++++++++++++++\n')

        #return result
        res = np.array(result)
        return res

    def splited_time_frame(self, split_time_frame, start_candle_index, end_candle_index):
        from constants import time_frame_s1, time_frame_m1, time_frame_h1, time_frame_d1, time_frame_mn1, time_frame_y1

        result = list()
        data = self.data_set.get_raw_data(candle_index=end_candle_index,
                                          candle_count=start_candle_index - end_candle_index + 1)

        period = 1
        if split_time_frame == time_frame_s1:
            coeff = 1
        elif split_time_frame == time_frame_m1:
            coeff = 100
        elif split_time_frame == time_frame_h1:
            coeff = 10000
        elif split_time_frame == time_frame_d1:
            coeff = 1000000
        elif split_time_frame == time_frame_mn1:
            coeff = 100000000
        elif split_time_frame == 'MN3':
            coeff = 100000000
            period = 3
        elif split_time_frame == time_frame_y1:
            coeff = 10000000000
        else:
            coeff = 1

        m = data[0, 0] // coeff
        for i in range(data.shape[0]):
            if data[i, 0] // coeff != m:
                result.append([data[i - 1, 0], end_candle_index + i - 1])
                m = data[i, 0] // coeff

        if data[-1, 0] // coeff == m:
            result.append([data[data.shape[0] - 1, 0], end_candle_index + data.shape[0] - 1])

        return result

    def analyze_order0(self, order):
        buy_date, sell_date, buy_price, sell_price, coeff, candle_count = range(6)

        max_coeff = -100
        min_coeff = 100

        profit_order_count = 0
        profit_order_sum_coeff = 0
        profit_order_sum_candle_count = 0
        profit_order_coeff = 1


        lose_order_count = 0
        lose_order_sum_coeff = 0
        lose_order_sum_candle_count = 0
        lose_order_coeff = 1


        none_pl_order_count = 0
        none_pl_order_sum_candle_count = 0


        for item in order:
            # min max coeff
            if item[coeff] > max_coeff:
                max_coeff = item[coeff]
            elif item[coeff] < min_coeff:
                min_coeff = item[coeff]

            # order count
            if item[coeff] > 1:
                profit_order_count += 1
                profit_order_sum_coeff += item[coeff]
                profit_order_sum_candle_count += item[candle_count]
                profit_order_coeff *= item[coeff]

            elif item[coeff] < 1:
                lose_order_count += 1
                lose_order_sum_coeff += item[coeff]
                lose_order_sum_candle_count += item[candle_count]
                lose_order_coeff *= item[coeff]

            else:
                none_pl_order_count += 1
                none_pl_order_sum_candle_count += item[candle_count]


        total_order_count = len(order)
        total_order_candle_count = profit_order_sum_candle_count + lose_order_sum_candle_count + none_pl_order_sum_candle_count

        if total_order_count > 0:
            profit_order_count_percent = profit_order_count / total_order_count
            lose_order_count_percent = lose_order_count / total_order_count
            none_pl_order_count_percent = none_pl_order_count / total_order_count
        else:
            profit_order_count_percent = 0
            lose_order_count_percent = 0
            none_pl_order_count_percent = 0

        if total_order_candle_count > 0:
            profit_order_sum_candle_count_percent = profit_order_sum_candle_count / total_order_candle_count
            lose_order_sum_candle_count_percent = lose_order_sum_candle_count / total_order_candle_count
            none_pl_order_sum_candle_count_percent = none_pl_order_sum_candle_count / total_order_candle_count
        else:
            profit_order_sum_candle_count_percent = 0
            lose_order_sum_candle_count_percent = 0
            none_pl_order_sum_candle_count_percent = 0

        if profit_order_count > 0:
            profit_order_sum_coeff_average = profit_order_sum_coeff / profit_order_count
        else:
            profit_order_sum_coeff_average = 0

        if lose_order_count > 0:
            lose_order_sum_coeff_average = lose_order_sum_coeff / lose_order_count
        else:
            lose_order_sum_coeff_average = 0




        print('--------------------------------------------------')
        print('total_order: {}'.format(total_order_count))
        print('total_order_candle_count: {}'.format(total_order_candle_count))
        print('max_coeff: {}'.format(max_coeff))
        print('min_coeff: {}'.format(min_coeff))
        print('-----------')

        print('profit_order_count: {}'.format(profit_order_count))
        print('profit_order_sum_coeff: {}'.format(profit_order_sum_coeff))
        print('profit_order_sum_candle_count: {}'.format(profit_order_sum_candle_count))
        print('profit_order_coeff: {}'.format(profit_order_coeff))

        print('-----------')
        print('lose_order_count: {}'.format(lose_order_count))
        print('lose_order_sum_coeff: {}'.format(lose_order_sum_coeff))
        print('lose_order_sum_candle_count: {}'.format(lose_order_sum_candle_count))
        print('lose_order_coeff: {}'.format(lose_order_coeff))

        print('-----------')
        print('none_pl_order_count: {}'.format(none_pl_order_count))
        print('none_pl_order_sum_candle_count: {}'.format(none_pl_order_sum_candle_count))
        #print('-----------')
        print('\n+++++++++++++++++++++++++')
        print('total_order_count: {}'.format(total_order_count))
        print('total_order_candle_count: {}'.format(total_order_candle_count))
        print('max_coeff: {}'.format(max_coeff))
        print('min_coeff: {}'.format(min_coeff))
        print('profit_order_count_percent: {}'.format(profit_order_count_percent))
        print('lose_order_count_percent: {}'.format(lose_order_count_percent))
        print('none_pl_order_count_percent: {}'.format(none_pl_order_count_percent))
        print('profit_order_sum_candle_count_percent: {}'.format(profit_order_sum_candle_count_percent))
        print('lose_order_sum_candle_count_percent: {}'.format(lose_order_sum_candle_count_percent))
        print('none_pl_order_sum_candle_count_percent: {}'.format(none_pl_order_sum_candle_count_percent))
        print('profit_order_sum_coeff_average: {}'.format(profit_order_sum_coeff_average-1))
        print('lose_order_sum_coeff_average: {}'.format(1-lose_order_sum_coeff_average))



        print('+++++++++++++++++++++++++\n')
