import operator
from global_settind import sell_wage, buy_wage
import numpy as np


class Signal:
    date = 'date'
    price = 'price'
    action = 'action'
    candle_index = 'candle_index'
    action_sell = 'sell'
    action_buy = 'buy'

    def __init__(self, data_set):
        self.data_set = data_set
        self.signal_list = list()

    def reset(self):
        self.signal_list = []

    def sell(self, candle_index):
        data = self.data_set.get_raw_data(candle_index=candle_index, candle_count=1)
        self.signal_list.append({'candle_index': candle_index, 'date': data[0, 0],
                                 'price': data[0, 1], 'action': self.action_sell})

    def buy(self, candle_index):
        data = self.data_set.get_raw_data(candle_index=candle_index, candle_count=1)
        self.signal_list.append({'candle_index': candle_index, 'date': data[0, 0],
                                 'price': data[0, 1], 'action': self.action_buy})

    def get_signal(self, sort_type='asc'):
        if sort_type == 'asc':
            self.signal_list.sort(key=operator.itemgetter('date'))

        elif sort_type == 'desc':
            self.signal_list.sort(key=operator.itemgetter('date'), reverse=True)

        return self.signal_list

    def get_order(self, part, total):
        # print('get_order')
        all_coeff = 1
        self.signal_list = self.get_signal()
        order = list()
        last_signal = None
        sig = list()
        stack = list()
        for i in range(len(self.signal_list)):
            if len(stack) >= total:
                stack.pop(0)

            stack.append(self.signal_list[i][self.action])
            if stack.count(stack[-1]) >= part:
                if last_signal != stack[-1]:
                    last_signal = stack[-1]
                    sig.append(self.signal_list[i])
                    # sig.append({'date': self.signal_list[i][self.date],
                    #            'price': self.signal_list[i][self.price],
                    #            'action': self.signal_list[i][self.action]})

        if len(sig) > 0 and sig[0][self.action] == self.action_sell:
            sig.pop(0)
        if len(sig) > 0 and sig[-1][self.action] == self.action_buy:
            sig.pop()

        if len(sig) > 0:
            data = self.data_set.get_adjusted_function_list(0, sig[0][self.candle_index] + 1)
            for i in range(0, len(sig), 2):
                buy_index = sig[i][self.candle_index]
                sell_index = sig[i + 1][self.candle_index]
                buy_p = data[buy_index, 1] * sig[i][self.price] * buy_wage + data[buy_index, 2]
                sell_p = data[sell_index, 1] * sig[i + 1][self.price] * sell_wage + data[sell_index, 2]

                coeff = sell_p / buy_p

                # print('buy_index:{} sell_index:{} buy_p:{} sell_p:{} coeff:{}'
                # .format(buy_index, sell_index, buy_p, sell_p,coeff))
                # [buy_date, sell_date, buy_price, sell_price, benefit_coeff,
                # order_day_count, buy_candle_index, sell_candle_index]
                order.append([sig[i][self.date],
                              sig[i + 1][self.date],
                              sig[i][self.price],
                              sig[i + 1][self.price],
                              coeff,
                              buy_index - sell_index,
                              buy_index,
                              sell_index])

                all_coeff *= coeff

        return order, all_coeff

    def analyze_order(self, part, total, split_time_frame):
        buy_date, sell_date, buy_price, sell_price, coeff, candle_count, buy_index, sell_index = range(8)

        result = list()
        # time_frame_start = 0
        time_frame_end = 0

        orders, all_coeff = self.get_order(part=part, total=total)

        result_total = self.analyze_order_total(orders)

        if len(result_total) > 0:
            result.append(result_total[0])

        if len(orders) > 0:
            splited_time_frame = self.splited_time_frame(split_time_frame=split_time_frame,
                                                         start_candle_index=orders[0][buy_index],
                                                         end_candle_index=orders[-1][sell_index])
        else:
            return np.zeros(0)
        # print('splited_time_frame')
        # print(splited_time_frame)
        # print(len(splited_time_frame))

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
                                                                profit_order_coeff * lose_order_coeff,
                                                                max_coeff,
                                                                min_coeff,
                                                                profit_order_count,
                                                                lose_order_count,
                                                                none_pl_order_count,
                                                                profit_order_count_percent,
                                                                lose_order_count_percent,
                                                                none_pl_order_count_percent,
                                                                profit_order_sum_candle_count_percent,
                                                                lose_order_sum_candle_count_percent,
                                                                none_pl_order_sum_candle_count_percent,
                                                                profit_order_sum_coeff_average,
                                                                lose_order_sum_coeff_average]])

            time_frame_end = time_frame_start

            # print('********************************************************')
            # print('********************************************************')
            # print('total_order: {}'.format(total_order_count))
            # print('total_order_candle_count: {}'.format(total_order_candle_count))
            # print('max_coeff: {}'.format(max_coeff))
            # print('min_coeff: {}'.format(min_coeff))
            # print('-----------')

            # print('profit_order_count: {}'.format(profit_order_count))
            # print('profit_order_sum_coeff: {}'.format(profit_order_sum_coeff))
            # print('profit_order_sum_candle_count: {}'.format(profit_order_sum_candle_count))
            # print('profit_order_coeff: {}'.format(profit_order_coeff))

            # print('-----------')
            # print('lose_order_count: {}'.format(lose_order_count))
            # print('lose_order_sum_coeff: {}'.format(lose_order_sum_coeff))
            # print('lose_order_sum_candle_count: {}'.format(lose_order_sum_candle_count))
            # print('lose_order_coeff: {}'.format(lose_order_coeff))

            # print('-----------')
            # print('none_pl_order_count: {}'.format(none_pl_order_count))
            # print('none_pl_order_sum_candle_count: {}'.format(none_pl_order_sum_candle_count))
            # print('-----------')
            # print('\n+++++++++++++++++++++++++')
            # print('total_order_count: {}'.format(total_order_count))
            # print('total_order_candle_count: {}'.format(total_order_candle_count))
            # print('max_coeff: {}'.format(max_coeff))
            # print('min_coeff: {}'.format(min_coeff))
            # print('profit_order_count_percent: {}'.format(profit_order_count_percent))
            # print('lose_order_count_percent: {}'.format(lose_order_count_percent))
            # print('none_pl_order_count_percent: {}'.format(none_pl_order_count_percent))
            # print('profit_order_sum_candle_count_percent: {}'.format(profit_order_sum_candle_count_percent))
            # print('lose_order_sum_candle_count_percent: {}'.format(lose_order_sum_candle_count_percent))
            # print('none_pl_order_sum_candle_count_percent: {}'.format(none_pl_order_sum_candle_count_percent))
            # print('profit_order_sum_coeff_average: {}'.format(profit_order_sum_coeff_average - 1))
            # print('lose_order_sum_coeff_average: {}'.format(1 - lose_order_sum_coeff_average))

            # print('+++++++++++++++++++++++++\n')

        # return result
        res = np.array(result)
        return res

    def splited_time_frame(self, split_time_frame, start_candle_index, end_candle_index):
        from constants import time_frame_s1, time_frame_m1, time_frame_h1, time_frame_d1, time_frame_mn1, time_frame_y1

        result = list()
        data = self.data_set.get_raw_data(candle_index=end_candle_index,
                                          candle_count=start_candle_index - end_candle_index + 1)

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

    def analyze_order_total(self, order):
        if len(order) <= 0:
            return np.zeros(0)

        buy_date, sell_date, buy_price, sell_price, coeff, candle_count, buy_index, sell_index = range(8)

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

        result = list()
        result.append([[order[0][buy_index], order[-1][sell_index] - 1], [total_order_count,
                                                                          total_order_candle_count,
                                                                          profit_order_coeff * lose_order_coeff,
                                                                          max_coeff,
                                                                          min_coeff,
                                                                          profit_order_count,
                                                                          lose_order_count,
                                                                          none_pl_order_count,
                                                                          profit_order_count_percent,
                                                                          lose_order_count_percent,
                                                                          none_pl_order_count_percent,
                                                                          profit_order_sum_candle_count_percent,
                                                                          lose_order_sum_candle_count_percent,
                                                                          none_pl_order_sum_candle_count_percent,
                                                                          profit_order_sum_coeff_average,
                                                                          lose_order_sum_coeff_average]])
        # print('--------------------------------------------------')
        # print('total_order: {}'.format(total_order_count))
        # print('total_order_candle_count: {}'.format(total_order_candle_count))
        # print('max_coeff: {}'.format(max_coeff))
        # print('min_coeff: {}'.format(min_coeff))
        # print('-----------')

        # print('profit_order_count: {}'.format(profit_order_count))
        # print('profit_order_sum_coeff: {}'.format(profit_order_sum_coeff))
        # print('profit_order_sum_candle_count: {}'.format(profit_order_sum_candle_count))
        # print('profit_order_coeff: {}'.format(profit_order_coeff))

        # print('-----------')
        # print('lose_order_count: {}'.format(lose_order_count))
        # print('lose_order_sum_coeff: {}'.format(lose_order_sum_coeff))
        # print('lose_order_sum_candle_count: {}'.format(lose_order_sum_candle_count))
        # print('lose_order_coeff: {}'.format(lose_order_coeff))

        # print('-----------')
        # print('none_pl_order_count: {}'.format(none_pl_order_count))
        # rint('none_pl_order_sum_candle_count: {}'.format(none_pl_order_sum_candle_count))
        # print('-----------')
        # print('\n+++++++++++++++++++++++++')
        # print('total_order_count: {}'.format(total_order_count))
        # print('total_order_candle_count: {}'.format(total_order_candle_count))
        # print('max_coeff: {}'.format(max_coeff))
        # print('min_coeff: {}'.format(min_coeff))
        # print('profit_order_count_percent: {}'.format(profit_order_count_percent))
        # print('lose_order_count_percent: {}'.format(lose_order_count_percent))
        # print('none_pl_order_count_percent: {}'.format(none_pl_order_count_percent))
        # print('profit_order_sum_candle_count_percent: {}'.format(profit_order_sum_candle_count_percent))
        # print('lose_order_sum_candle_count_percent: {}'.format(lose_order_sum_candle_count_percent))
        # print('none_pl_order_sum_candle_count_percent: {}'.format(none_pl_order_sum_candle_count_percent))
        # print('profit_order_sum_coeff_average: {}'.format(profit_order_sum_coeff_average-1))
        # print('lose_order_sum_coeff_average: {}'.format(1-lose_order_sum_coeff_average))

        # print('+++++++++++++++++++++++++\n')

        return result
