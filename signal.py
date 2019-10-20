import operator
from global_settind import sell_wage, buy_wage


class Signal:
    date = 'date'
    price = 'price'
    action = 'action'

    def __init__(self, data_set):
        self.data_set = data_set
        self.signal_list = list()

    def reset(self):
        self.signal_list = []

    def sell(self, candle_index):
        data = self.data_set.get_raw_data(candle_index=candle_index, candle_count=1)
        self.signal_list.append({'date': data[0, 0], 'price': data[0, 1], 'action': 'sell'})

    def buy(self, candle_index):
        data = self.data_set.get_raw_data(candle_index=candle_index, candle_count=1)
        self.signal_list.append({'date': data[0, 0], 'price': data[0, 1], 'action': 'buy'})

    # def hold(self, candle_index):
    #    data = self.data_set.get_raw_data(candle_index=candle_index, candle_count=1)
    #    self.signal_list.append({'date': data[0, 0], 'price': data[0, 1], 'action': 'hold'})

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
                    sig.append({'date': self.signal_list[i][self.date],
                                'price': self.signal_list[i][self.price],
                                'action': self.signal_list[i][self.action]})

        if sig[0][self.action] == 'sell':
            sig.pop(0)
        if sig[-1][self.action] == 'buy':
            sig.pop()

        if len(sig) > 0:
            index = self.data_set.get_candle_index(sig[0][self.date])
            # print(index)
            data = self.data_set.get_adjusted_function_list(0, index + 1)
            for i in range(0, len(sig), 2):
                buy_index = self.data_set.get_candle_index(sig[i][self.date])
                sell_index = self.data_set.get_candle_index(sig[i + 1][self.date])
                buy_p = data[buy_index, 1] * sig[i][self.price] * buy_wage + data[buy_index, 2]
                sell_p = data[sell_index, 1] * sig[i + 1][self.price] * sell_wage + data[sell_index, 2]

                coeff = sell_p / buy_p

                # print('buy_index:{} sell_index:{} buy_p:{} sell_p:{} coeff:{}'
                # .format(buy_index, sell_index, buy_p, sell_p,coeff))
                # [buy_date, sell_date, buy_price, sell_price, benefit_coeff, order_day_count]
                order.append([sig[i][self.date],
                              sig[i + 1][self.date],
                              sig[i][self.price],
                              sig[i + 1][self.price],
                              coeff, buy_index - sell_index])

                all_coeff *= coeff

        return order, all_coeff
