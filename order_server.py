
import threading
import sys
# import trace

from my_lib import PrintColored
from database import OrderDataSetDB
from time import sleep
import ast
from copy import deepcopy
import os

from run_strategy import RunStrategy

from constants import adjust_today_candle_this_time


server_status_running = 'running'
server_status_stopping = 'stopping'
server_status_stop = 'stop'

server_status_shutting_down = 'shutting down'
server_status_shutdown = 'shutdown'

server_status_sleeping = 'sleeping'
server_status_waiting = 'waiting'


class StrategyThread(threading.Thread):
    #def __init__(self, database_info, en_symbol_12_digit_code, start_date_time, today_date_time, time_frame,
    #             data_type, adjust_today_candle, adjusted_type, strategy, data_count, log_obj):
    def __init__(self, database_info, data_count, log_obj, params):
        threading.Thread.__init__(self)
        self.print_c = PrintColored(default_color='red')
        self.killed = False

        self.database_info = database_info
        self.data_count = data_count
        self.log_obj = log_obj

        self.obj = RunStrategy(database_info=self.database_info, data_count=self.data_count, log_obj=None, params=params)
        self.init_error = self.obj.get_init_error()
        if self.init_error is not None:
            print(self.init_error)
            return



    def get_init_error(self):
        return self.init_error

    def globaltrace(self, frame, event, arg):
        if event == 'call':
            return self.localtrace
        else:
            return None

    def localtrace(self, frame, event, arg):
        if self.killed:
            if event == 'line':
                raise SystemExit()
        return self.localtrace

    def kill(self):
        self.killed = True

    def process(self):
        self.obj.exec()
        #order, coeff = self.obj.a()
        #print(order)
        #print(coeff)
        #self.obj.analyze_order(order, 'MN1')
        #self.obj.analyze_order_total(order)
        # self.print_c.print('running thread {0}'.format(threading.current_thread().getName()))
        sleep(0.1)
        # run strategy
        return True

    def run(self):
        sys.settrace(self.globaltrace)

        # self.print_c.print('start thread {0}'.format(threading.current_thread().getName()))
        result = self.process()
        # self.print_c.print('finished thread {0}'.format(threading.current_thread().getName()))

        return result


class RunOrder:
    def __init__(self, web_database_info, analyze_database_info, order_run_time, max_worker, p_name, log_obj=None):
        self.max_worker = max_worker
        self.web_database_info = web_database_info
        self.analyze_database_info = analyze_database_info
        self.order_run_time = order_run_time
        self.process_name = p_name

        self.status_file_name = '.single_server_status/' + self.process_name
        self.lock = threading.Lock()
        self.waiting_list = list()
        self.running_list = list()
        self.complete_list = list()

        self.web_db = OrderDataSetDB(db_info=self.web_database_info, log_obj=log_obj)
        self.print_c = PrintColored(default_color='green')

        self.init_status_file()
        self.set_status(server_status_shutdown)

    def init_status_file(self):
        if not os.path.exists(self.status_file_name):
            if os.path.dirname(self.status_file_name) != '':
                if not os.path.exists(os.path.dirname(self.status_file_name)):
                    os.makedirs(os.path.dirname(self.status_file_name))
            f = open(self.status_file_name, 'w', encoding='utf_8')
            f.close()

    def remove_status_file(self):
        if os.path.exists(self.status_file_name):
            os.remove(self.status_file_name)

    def set_status(self, text):
        f = open(self.status_file_name, 'w', encoding='utf_8')
        f.write(text)
        f.close()

    def get_status(self):
        f = open(self.status_file_name, 'r', encoding='utf_8')
        res = f.readline()
        f.close()
        return res

    def run(self):
        #order = None
        order_id = None
        username = None
        input_params = None

        try_num = 0
        max_try = 3
        clean_sub_order_result = False
        try_again = False
        self.set_status(server_status_running)
        while self.get_status() != server_status_shutting_down:
            # print('start cycle')
            # print(try_again)
            # print(try_num)
            while self.get_status() in [server_status_stopping, server_status_stop]:
                self.set_status(server_status_stop)
                sleep(10)
            if self.get_status() == server_status_shutting_down:
                continue
            # get order from database
            if try_again is False:
                #order, error = self.get_order()
                order_id, username, input_params, error = self.get_order()
                # print('get order order: {} error: {}'.format(order, error))
                if error is not None:
                    if self.get_status() in [server_status_stopping, server_status_shutting_down]:
                        continue

                    if error == 'no any order':
                        # clear sub result table
                        # self.db_web_order.clean_sub_order_table()
                        self.set_status(server_status_waiting)
                        self.print_c.print('{0}: no any order: wait 60 second'.format(self.process_name))
                        sleep(20)

                    else:
                        self.set_status(server_status_sleeping)
                        self.print_c.print('{0}: error: {1} --> wait 10 second'.format(self.process_name, error))
                        sleep(10)
                    continue

            self.set_status(server_status_running)
            try_num += 1

            # ------------------------------
            self.running_list.clear()
            self.complete_list.clear()
            self.waiting_list = deepcopy(input_params['accepted_symbol_list'])
            waiting_list_count = len(self.waiting_list)

            # print(self.waiting_list)
            # ------------------------------
            # run order on each symbol in thread method
            i = 0
            j = 0
            break_order = False
            while len(self.waiting_list) > 0:
                if self.get_status() != server_status_running:
                    try_again = True
                    break_order = True
                    break
                # check order exist
                if i == 0:
                    # exit when order complete in another process
                    if self.web_db.exist_order(order_id) is not True:
                        try_again = False
                        break_order = True
                        break
                    i += 1
                elif i > 20:
                    i = 0
                else:
                    i += 1

                if threading.active_count() < self.max_worker + 1:
                    j = 0
                    # get symbol
                    self.lock.acquire()
                    while True:
                        if len(self.waiting_list) == 0:
                            en_symbol_12_digit_code = None
                            self.lock.release()
                            break

                        en_symbol_12_digit_code = self.waiting_list.pop()
                        # check execute symbol in order
                        res = self.web_db.exist_sub_order_result(order_id, en_symbol_12_digit_code)
                        if res is True:
                            continue

                        self.running_list.append(en_symbol_12_digit_code)
                        self.lock.release()
                        break

                    if en_symbol_12_digit_code is None:
                        try_again = False
                        break_order = True
                        break

                    # print(order)
                    # create new thread
                    t = StrategyThread(database_info=self.analyze_database_info,
                                       data_count=0,
                                       log_obj=None,
                                       params=self.get_order_params(input_params, en_symbol_12_digit_code))

                    t.setName(en_symbol_12_digit_code)
                    t.start()
                    while not t.is_alive():
                        sleep(0.1)
                else:
                    j += 1
                    if j == 1:
                        self.print_c.print('{0}: wait for free thread. thread count: {1}'
                                           .format(self.process_name, threading.active_count() - 1))
                    elif j >= 100:
                        j = 0

                    sleep(0.01)

            if break_order is True:
                # terminate all thread
                self.terminate_all()
                # go to run nex order
                continue

            # wait to finished thread
            i = 0
            while threading.active_count() > 1:
                if i == 0:
                    if self.web_db.exist_order(order_id) is not True:
                        break_order = True
                        break
                    self.print_c.print('{0}: wait to finish thread. thread count: {1}'
                                       .format(self.process_name, threading.active_count() - 1))
                    i += 1

                elif i >= 10:
                    i = 0
                else:
                    i += 1

                sleep(1)

            if break_order is True:
                self.terminate_all()
                # go to run nex order
                continue

            # calculate total output
            all_result, err = self.web_db.get_all_sub_result(order_id)
            # symbol, result, start_time, run_time
            if err is not None:
                # go to run nex order
                # error = err
                # result = False
                self.print_c.print('{0}: error: {1}'.format(self.process_name, err))
                continue

            if len(all_result) < waiting_list_count:
                if self.web_db.exist_order(order_id) is not True:
                    try_again = False
                    continue

                if try_num < max_try:
                    try_again = True
                else:
                    try_num = 0
                    try_again = False
                continue

            if len(all_result) > 0:
                start_time = all_result[0][2]
                end_time = all_result[0][2]
                # run_time = 0
                opt = list()
                sum_run_time = 0
                for item in all_result:
                    if start_time > item[2]:
                        start_time = item[2]

                    if end_time < item[2] + item[3]:
                        end_time = item[2] + item[3]

                    sum_run_time += item[3]
                    opt_item = ast.literal_eval(item[1])
                    opt.append(opt_item)
                # self.print_c('2')
                # error = None
                result = opt
                # insert result on database
                order_run_time = end_time - start_time
                res, err = self.web_db.insert_web_order_result(order_id=order_id,
                                                               username=username,
                                                               input_param=str(input_params),
                                                               result=str(result),
                                                               start_time=start_time,
                                                               order_run_time=order_run_time,
                                                               sum_run_time=sum_run_time)
                if err is not None:
                    if self.get_status() in [server_status_stopping, server_status_shutting_down]:
                        self.print_c.print('{0}: cant insert output: error: {1}'.format(self.process_name, err))
                        self.print_c.print('{0}: shutting down'.format(self.process_name, err))
                        return
                    else:
                        self.print_c.print('{0}: cant insert output: error: {1} --> wait 10 second'
                                           .format(self.process_name, err))
                        sleep(10)
                        self.set_status(server_status_sleeping)
                        continue

                # remove order from waiting_order table
                # self.web_db.remove_order(order_id)
                self.print_c.print('{0}: finish run order: {1} : result: {2}'
                                   .format(self.process_name, order_id, result))

                # remove sub order from sub_order_result table
                if clean_sub_order_result is True:
                    self.web_db.clean_sub_order_result(order_id)

        self.set_status(server_status_shutdown)

    def terminate_all(self):
        self.print_c.print('terminate all thread')
        for t in threading.enumerate():
            if t != threading.main_thread():
                t.kill()

        for t in threading.enumerate():
            if t != threading.main_thread():
                t.join()

    def get_order(self):
        result, error = self.web_db.get_new_order(order_run_time=self.order_run_time)
        if error is not None:
            return None, None, None, error

        order_id = result[0]
        username = result[1]
        input_params = ast.literal_eval(result[2])

        error = self.check_input_params(input_params)
        if error is not None:
            return None, None, None, error

        return order_id, username, input_params, error

    def check_input_params(self, input_param):
        keys = ['accepted_symbol_list', 'start_date_time', 'end_date_time', 'time_frame',
                'data_type', 'adjusted_type', 'strategy',
                'current_strategy_name', 'strategy_variable', 'strategy_context',
                'output_format', 'order_same', 'order_total']

        for k in keys:
            if k not in input_param:
                return 'invalid key param: {}'.format(k)

        return None

    def get_order_params(self, input_params, en_symbol_12_digit_code):
        res = {'en_symbol_12_digit_code': en_symbol_12_digit_code,
               'start_date_time': input_params['start_date_time'],
               'today_date_time': input_params['end_date_time'],
               'time_frame': input_params['time_frame'],
               'data_type': input_params['data_type'],
               'adjust_today_candle': adjust_today_candle_this_time,
               'adjusted_type': input_params['adjusted_type'],
               'strategy': [input_params['strategy_variable'], input_params['strategy_context']],
               'order_same': input_params['order_same'],
               'order_total': input_params['order_total']}

        return res


    @ staticmethod
    def unpack_order(order):
        order_id = order[0]
        username = order[1]
        input_params = ast.literal_eval(order[2])

        adjusted_type = input_params['adjusted_type']
        start_date_time = input_params['start_date_time']
        end_date_time = input_params['end_date_time']
        time_frame = input_params['time_frame']
        order_total = input_params['order_total']
        order_same = input_params['order_same']
        data_type = input_params['data_type']
        accepted_symbol_list = input_params['accepted_symbol_list']
        output_format = input_params['output_format']
        strategy = input_params['strategy']
        current_strategy_name = input_params['current_strategy_name']
        strategy_variable = input_params['strategy_variable']
        strategy_context = input_params['strategy_context']

        return order_id, username, input_params, adjusted_type, start_date_time, end_date_time, \
            time_frame, order_total, order_same, data_type, accepted_symbol_list, output_format, \
            strategy, current_strategy_name, strategy_variable, strategy_context

    @ staticmethod
    def pack_order(order_id, username, adjusted_type, start_date_time, end_date_time, time_frame,
                   order_total, order_same, data_type, accepted_symbol_list, output_format, strategy,
                   current_strategy_name, strategy_variable, strategy_context):

        input_pa = {'adjusted_type': adjusted_type,
                    'start_date_time': start_date_time,
                    'end_date_time': end_date_time,
                    'time_frame': time_frame,
                    'order_total': order_total,
                    'order_same': order_same,
                    'data_type': data_type,
                    'accepted_symbol_list': accepted_symbol_list,
                    'output_format': output_format,
                    'strategy': strategy,
                    'current_strategy_name': current_strategy_name,
                    'strategy_variable': strategy_variable,
                    'strategy_context': strategy_context
                    }

        order = list()
        order.append(order_id)
        order.append(username)
        order.append(input_pa)

        return order


if __name__ == '__main__':
    from database_info import get_database_info, laptop_local_access

    web_database_info = get_database_info(laptop_local_access, 'bourse_web_order_0.1')
    analyze_database_info = get_database_info(laptop_local_access, 'bourse_analyze_server_0.1')

    server = RunOrder(web_database_info=web_database_info,
                      analyze_database_info=analyze_database_info,
                      order_run_time=10, max_worker=1,
                      p_name='server 1', log_obj=None)

    server.run()
