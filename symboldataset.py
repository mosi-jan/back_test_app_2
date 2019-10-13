from Log import Logging
from my_lib import PrintColored
from database import DataBase
import numpy as np
import constants


class SymbolDataSet:
    def __init__(self, database_info, en_symbol_12_digit_code, start_date_time, today_date_time, time_frame,
                 data_type, adjust_today_candle, adjusted_type, data_count=0, log_obj=None):
        # init params
        self.colored_print = PrintColored(default_color='red')

        if log_obj is None:
            self.log = Logging()
            self.log.logConfig(account_id=database_info['db_username'])
        else:
            self.log = log_obj
        self.db = DataBase(database_info, self.log)

        self.en_symbol_12_digit_code = en_symbol_12_digit_code
        self.start_date_time = start_date_time
        self.today_date_time = today_date_time  # day 0
        self.time_frame = time_frame
        self.data_type = data_type

        self.adjusted_type = adjusted_type
        self.adjust_today_candle = adjust_today_candle
        self.data_count = data_count

        self.col_date_time, self.col_open_price, self.col_high_price, self.col_low_price, self.col_close_price, \
            self.col_trade_volume, self.col_trade_value, self.col_trade_count = range(8)
        # init data objects
        self.no_any_data = False
        self.init_error = False

        self.__load_init_data()

        self.last_adjusted_type = None
        self.adjusted_data = list()

    def __load_init_data(self):
        # load time_frame_data ------------------------------------
        second_date, error = self.db.get_second_data(en_symbol_12_digit_code=self.en_symbol_12_digit_code,
                                                     start_date=self.start_date_time, end_date=self.today_date_time)
        if error is not None:
            self.init_error = True
            return
        self.time_frame_data = np.array(self.__second_to_time_frame(second_date))

        # load adjusted_data ------------------------------------
        # do_data, coefficient, adjusted_type, old_data, new_data
        all_adjusted_data, error = self.db.get_all_adjusted_data(en_symbol_12_digit_code=self.en_symbol_12_digit_code)
        if error is not None:
            self.init_error = True
            return

        # do_data, coefficient, adjusted_type, old_data, new_data
        self.all_adjusted_data = all_adjusted_data

    def __second_to_time_frame(self, second_data):
        date_time, open_price, high_price, low_price, close_price, trade_count, trade_volume, trade_value = range(8)

        time_series = list()
        source = second_data
        if len(source) > 0:
            open = 0
            close = 0
            high = 0
            low = 0
            count = 0
            volume = 0
            value = 0
            start_p1 = 0
            start_time_date = 0

            start = True

            for item in source:
                p1 = 0
                p1_time = p1

                if self.time_frame == 'S1':
                    p1 = item[date_time]
                    p1_time = p1

                elif self.time_frame == 'M1':
                    p1 = int(item[date_time] / 100)
                    p1_time = p1 * 100

                elif self.time_frame == 'H1':
                    p1 = int(item[date_time] / 10000)
                    p1_time = p1 * 10000

                elif self.time_frame == 'D1':
                    p1 = int(item[date_time] / 1000000)
                    p1_time = p1 * 1000000

                elif self.time_frame == 'MN1':
                    p1 = int(item[date_time] / 100000000)
                    # p1_time = (p1 * 100 + 1) * 1000000
                    p1_time = p1 * 100000000 + 1000000

                elif self.time_frame == 'Y1':
                    p1 = int(item[date_time] / 10000000000)
                    p1_time = p1 * 10000000000

                if start is True:
                    start = False
                    open = item[open_price]
                    close = item[close_price]
                    high = item[high_price]
                    low = item[low_price]
                    volume = 0
                    value = 0
                    count = 0
                    start_p1 = p1
                    start_time_date = p1_time

                if p1 == start_p1:
                    open = item[open_price]

                    if item[low_price] < low:
                        low = item[low_price]

                    if item[high_price] > high:
                        high = item[high_price]

                    count += item[trade_count]
                    volume += item[trade_volume]
                    value += item[trade_value]

                else:
                    # end = round(float(value) / volume)
                    time_series.append([start_time_date, int(open), int(high), int(low), int(close),
                                        volume, value, count])

                    open = item[open_price]
                    close = item[close_price]
                    high = item[high_price]
                    low = item[low_price]
                    count = item[trade_count]
                    volume = item[trade_volume]
                    value = item[trade_value]

                    start_p1 = p1
                    start_time_date = p1_time

            if volume != 0:
                # end = round(float(value) / volume)
                time_series.append([start_time_date, int(open), int(high), int(low), int(close), volume, value, count])

        return time_series

    @staticmethod
    def __find_index(np_array, item):
        start = 0
        end = np_array.shape[0] - 1
        while True:
            mid = (start + end) // 2

            if np_array[mid] == item:
                return mid

            elif np_array[mid] > item:
                # end = mid -1
                start = mid + 1

            elif np_array[mid] < item:
                # start = mid + 1
                end = mid - 1

            if start > end:
                return start

    def get_data(self, candle_index, candle_count, adjust_today_candle=None, adjusted_type=None):
        time_series = list()

        if adjust_today_candle is None:
            adjust_today_candle = self.adjust_today_candle

        if adjusted_type is None:
            adjusted_type = self.adjusted_type

        if candle_count < 1:
            res = time_series
            return res, None

        self.__grow_time_frame_data(candle_index, candle_count)

        raw_data = self.get_raw_data_type(data_type=self.data_type,
                                          candle_index=candle_index, candle_count=candle_count)

        adjusted_list = self.get_adjusted_coefficient_list(adjust_today_candle, adjusted_type,
                                                           candle_index, candle_count)

        raw_data[:, 1] *= adjusted_list
        raw_data = raw_data.astype(np.uint64)

        return raw_data

    def get_raw_data_type(self, data_type, candle_index, candle_count):
        data = self.time_frame_data[candle_index:candle_index + candle_count]

        if data_type == constants.data_type_all:
            return data.astype(dtype='float64')

        elif data_type == constants.data_type_time:
            return data[:, (self.col_date_time, self.col_date_time)].astype(dtype='float64')

        elif data_type == constants.data_type_open:
            return data[:, (self.col_date_time, self.col_open_price)].astype(dtype='float64')

        elif data_type == constants.data_type_close:
            return data[:, (self.col_date_time, self.col_close_price)].astype(dtype='float64')

        elif data_type == constants.data_type_high:
            return data[:, (self.col_date_time, self.col_high_price)].astype(dtype='float64')

        elif data_type == constants.data_type_low:
            return data[:, (self.col_date_time, self.col_low_price)].astype(dtype='float64')

        elif data_type == constants.data_type_volume:
            return data[:, (self.col_date_time, self.col_trade_volume)].astype(dtype='float64')

        elif data_type == constants.data_type_value:
            return data[:, (self.col_date_time, self.col_trade_value)].astype(dtype='float64')

        elif data_type == constants.data_type_median:
            res = np.zeros((data.shape[0], 2))
            res[:, 0] = data[:, self.col_date_time]
            res[:, 1] = (data[:, self.col_high_price] + data[:, self.col_low_price]) / 2
            return res

        elif data_type == constants.data_type_typical:
            res = np.zeros((data.shape[0], 2))
            res[:, 0] = data[:, self.col_date_time]
            res[:, 1] = (data[:, self.col_high_price] + data[:, self.col_low_price] + data[:, self.col_close_price]) / 3
            return res

        elif data_type == constants.data_type_weighted:
            res = np.zeros((data.shape[0], 2))
            res[:, 0] = data[:, self.col_date_time]
            res[:, 1] = (data[:, self.col_high_price] + data[:, self.col_low_price] +
                         data[:, self.col_close_price] * 2) / 4
            return res

    def get_adjusted_coefficient_list(self, adjust_today_candle, adjusted_type, candle_index, candle_count):
        coeff = np.ones((self.time_frame_data[candle_index:candle_index + candle_count].shape[0], 2))
        coeff[:, 0] = self.time_frame_data[candle_index:candle_index + candle_count, 0]

        adj = self.get_adjusted_data(adjusted_type=adjusted_type)

        source_date = 0
        if adjust_today_candle == constants.adjust_today_candle_this_time:
            source_date = coeff[0, 0]
        elif adjust_today_candle == constants.adjust_today_candle_all_time:
            source_date = self.time_frame_data[0, 0]

        for item in adj:
            if item[0] * 1000000 > source_date or item[0] * 1000000 < coeff[-1, 0]:
                continue

            coeff_index = self.__find_index(np_array=coeff[:, 0], item=item[0] * 1000000)
            coeff[coeff_index:, 1] *= item[1]

        return coeff[:, 1]

    def get_adjusted_data(self, adjusted_type):
        if self.last_adjusted_type != adjusted_type:
            if adjusted_type == constants.adjusted_type_all:
                adj = (constants.adjusted_type_capital_increase, constants.adjusted_type_take_profit)
            else:
                adj = (adjusted_type,)

            self.adjusted_data = list()

            for item in self.all_adjusted_data:
                if item[2] in adj:
                    self.adjusted_data.append([item[0], item[1]])
            self.last_adjusted_type = adjusted_type

        return self.adjusted_data

    # ---------------------------------
    def __grow_time_frame_data(self, candle_index, candle_count):

        while candle_index + 1 + candle_count > self.time_frame_data.shape[0]:
            today_date_time = self.time_frame_data[-1, 0]
            candle_needed = candle_index + candle_count - self.time_frame_data.shape[0] + 1
            start_date_time = self.estimate_time(base_time=today_date_time,
                                                 part_count=candle_needed,
                                                 time_frame=self.time_frame)

            second_date, error = self.db.get_second_data_for_grow(en_symbol_12_digit_code=self.en_symbol_12_digit_code,
                                                                  start_date=start_date_time,
                                                                  end_date=today_date_time)
            if error is not None:
                self.init_error = True
                return

            if len(second_date) == 0:
                self.no_any_data = True
                return

            # self.time_frame_data = np.array(self.__second_to_time_frame(second_date))
            np.vstack((self.time_frame_data, np.array(self.__second_to_time_frame(second_date))))

    def estimate_time(self, base_time, part_count, time_frame):
        import datetime
        second = base_time % 100
        minute = int(base_time / 100) % 100
        hour = int(base_time / 10000) % 100
        day = int(base_time / 1000000) % 100
        month = int(base_time / 100000000) % 100
        year = int(base_time / 10000000000) % 10000
        r = datetime.datetime(year=year, month=month, day=day, hour=hour, minute=minute, second=second)

        if time_frame == constants.time_frame_s1:
            s = part_count % (3.5 * 3600)
            d = part_count // (3.5 * 3600)
            if r - datetime.timedelta(seconds=s) < datetime.datetime(year=year, month=month, day=day,
                                                                     hour=9, minute=0, second=0):
                ts = (d * 24 + 20.5) * 3600 + s
            else:
                ts = d * 24 * 3600 + s

            r -= datetime.timedelta(seconds=ts)
            return r

        elif time_frame == constants.time_frame_m1:
            s = part_count % (3.5 * 60)
            d = part_count // (3.5 * 60)
            if r - datetime.timedelta(minutes=s) < datetime.datetime(year=year, month=month, day=day,
                                                                     hour=9, minute=0, second=0):
                ts = (d * 24 + 20.5) * 60 + s
            else:
                ts = d * 24 * 60 + s

            r -= datetime.timedelta(minutes=ts)
            return r

        elif time_frame == constants.time_frame_h1:
            s = part_count % 4
            d = part_count // 4
            print(s)
            print(d)
            if r - datetime.timedelta(hours=s) < datetime.datetime(year=year, month=month, day=day,
                                                                   hour=9, minute=0, second=0):
                ts = (d * 24 + 20) + s
            else:
                ts = d * 24 + s

            r -= datetime.timedelta(hours=ts)
            return r

        elif time_frame == constants.time_frame_d1:
            r -= datetime.timedelta(days=part_count)
            return r

        elif time_frame == constants.time_frame_mn1:
            r -= datetime.timedelta(days=30 * part_count)
            return r

        elif time_frame == constants.time_frame_y1:
            r -= datetime.timedelta(days=365 * part_count)
            return r


    def __load_over_data(self, candle_index, candle_count):
        error = None
        res = True

        if self.no_any_data is True:
            error = 'no any data'
            return res, error

        end_date = self.candle_date_list[-1]
        i=0
        while candle_index + 1 + candle_count > len(self.candle_date_list):
            coeff = 2**i
            # need load any data
            now_candle_count = coeff * (candle_index + 1 + candle_count - len(self.candle_date_list))
            # self.print_c(now_candle_count)

            start_date = self.__get_time(end_date, now_candle_count, self.time_frame)
            add, add_error = self.db.get_share_second_data(self.en_symbol_12_digit_code, start_date, end_date)
            if add_error is not None:
                error = add_error
                res = False
                return res, error
            self.all_raw_second_data = self.all_raw_second_data + add
            self.candle_date_list, candle_date_list_error = self.__get_candle_date_list(self.time_frame)
            if candle_date_list_error is not None:
                error = candle_date_list_error
                res = False
                return res, error
            self.max_candle = len(self.candle_date_list)

            have_any_data , have_any_data_error = self.db.have_any_data(
                self.en_symbol_12_digit_code, self.candle_date_list[-1])
            if have_any_data_error is not None:
                error = add_error
                res = False
                return res, error

            if have_any_data is False:
                self.no_any_data = True
                error = 'no any data'
                res = True
                break

            end_date = start_date
            i += 1

        return res, error

    # ---------------------------------


def max_profit(data):
    buy_wage = 1 - 0.1
    sell_wage = 1 + 0.1
    # res = list()
    extremum = list()

    # find extremum points
    data_count = data.shape[0]

    if data_count > 1:
        if data[0] > data[1]:
            # res.append([data[1], data[0]])
            extremum.append(data[0])
        if len(data) > 2:
            for i in range(1, data_count - 1):
                if data[i] > data[i - 1] and data[i] > data[i + 1]:
                    extremum.append(data[i])

                elif data[i] < data[i - 1] and data[i] < data[i + 1]:
                    extremum.append(data[i])

        if data[-1] < data[-2]:
            extremum.append(data[-1])

    else:
        pass

    wage = np.array([sell_wage, buy_wage] * (len(extremum) // 2))
    step_np = np.array(extremum)
    step_np *= wage

    for i in range(1, step_np.shape[0] - 1, 2):
        if step_np[i] > step_np[i + 1]:
            step_np[i] = 0
            step_np[i + 1] = 0





if __name__ == '__main__':
    import time
    start = time.time()

    from database_info import get_database_info, laptop_local_access
    database_info = get_database_info(laptop_local_access, 'bourse_analyze_server_0.1')
    en_symbol_12_digit_code = 'IRO1ABDI0001'
    start_date_time = 20081215114810
    start_date_time = 20190702122959
    today_date_time = 20190703122950
    time_frame = 'S1'
    data_type = constants.data_type_close
    adjust_today_candle = constants.adjust_today_candle_this_time
    adjusted_type = constants.adjusted_type_all
    data_count = 0
    log_obj = None

    print('start')

    a = SymbolDataSet(database_info=database_info, en_symbol_12_digit_code=en_symbol_12_digit_code,
                      start_date_time=start_date_time, today_date_time=today_date_time, time_frame=time_frame,
                      data_type=data_type, adjust_today_candle=adjust_today_candle, adjusted_type=adjusted_type,
                      data_count=data_count, log_obj=log_obj)

    a.colored_print.print(text='aa')
    for j in range(1):
        # print(j)
        for i in range(1):
            print(i + 1000 * j)
            b = a.get_data(candle_index=i, candle_count=500)

            #print(b[0:2, :])
            print(b)
            print(b.shape)
            print(a.no_any_data)


    print(time.time() - start)
