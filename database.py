from pymysql import connect, cursors
from Log import Logging
import constants

# OrderDataSetDB
from my_time import get_now_time_second


class BaseDB:
    def __init__(self, db_info, log_obj=None):
        try:
            if log_obj is None:
                self.log = Logging()
                self.log.logConfig(account_id=db_info['db_username'])
            else:
                self.log = log_obj

            self.log.trace()
            self.db_host_name = db_info['db_host_name']
            self.db_username = db_info['db_username']
            self.db_user_password = db_info['db_user_password']
            self.db_name = db_info['db_name']
            self.db_port = db_info['db_port']

        except Exception as e:
            self.log.error('cant create database object: ', str(e))
            return

    def get_connection(self):
        try:
            if self.db_port is None:
                con = connect(host=self.db_host_name, user=self.db_username,
                              password=self.db_user_password, db=self.db_name)
                # con = pymysql.connect(host=self.db_host_name, user=self.db_username,
                # password=self.db_user_password, db=self.db_name, connect_timeout=6000,
                # read_timeout=6000, write_timeout=6000)
            else:
                con = connect(host=self.db_host_name, user=self.db_username,
                              password=self.db_user_password, db=self.db_name, port=self.db_port)
                # con = pymysql.connect(host=self.db_host_name, user=self.db_username,
                # password=self.db_user_password, db=self.db_name, port=self.db_port,
                # connect_timeout=6000, read_timeout=6000, write_timeout=6000)
            # con.query('SET GLOBAL connect_timeout=6000')
            # con.query('SET GLOBAL net_read_timeout=6000')
            # con.query('SET GLOBAL interactive_timeout=6000')
            # con.query('SET GLOBAL wait_timeout=6000')

            return con, None
        except Exception as e:
            self.log.error('cant create connection: {} {} {}'.
                           format(self.db_host_name, self.db_name, self.db_username), str(e))
            return False, str(e)

    def select_query(self, query, args, fetchall=True, write_log=True):
        if write_log is True:
            self.log.trace()
        if query == '':
            if write_log is True:
                self.log.error('{} {}'.format(self.db_host_name, self.db_name), 'query is empty')
            return False, 'SQL ERROR: query is empty'

        con = None
        try:
            con, err = self.get_connection()
            if err is not None:
                raise Exception(err)
            db = con.cursor()
            db.execute(query, args)
            con.close()
        except Exception as e:
            if write_log is True:
                self.log.error('except select_query: error:{0} query:{1}, args:{2}'.format(e, query, args))
            try:
                if con.open is True:
                    con.close()
            finally:
                return False, 'SQL ERROR:{0} query:{1} args:{2}'.format(str(e), query, args)

        if fetchall is not True:
            return db, None
        else:
            return db.fetchall(), None

    def select_query_dictionary(self, query, args, fetchall=True, write_log=True):
        if write_log is True:
            self.log.trace()
        if query == '':
            if write_log is True:
                self.log.error('{} {}'.format(self.db_host_name, self.db_name), 'query is empty')
            error = 'SQL ERROR: query is empty'
            return False, error
        con = None
        try:
            con, err = self.get_connection()
            if err is not None:
                raise Exception(err)
            db = con.cursor(cursors.DictCursor)
            db.execute(query, args)
            con.close()
        except Exception as e:
            if write_log is True:
                self.log.error('except select_query_dictionary: error:{0} query:{1}, args:{2}'.format(e, query, args))
            try:
                if con.open is True:
                    con.close()
            finally:
                return False, 'SQL ERROR:{0} query:{1} args:{2}'.format(str(e), query, args)

        if fetchall is not True:
            return db, None
        else:
            return db.fetchall(), None

    def command_query(self, query, args, write_log=True):
        if write_log is True:
            self.log.trace()
        if query == '':
            if write_log is True:
                self.log.error('{} {}'.format(self.db_host_name, self.db_name), 'query is empty')
            return False, 'SQL ERROR: query is empty'
        con = None
        try:
            con, err = self.get_connection()
            if err is not None:
                raise Exception(err)

            db = con.cursor()
            db._defer_warnings = True
            db.autocommit = False
            db.execute(query, args)
            con.commit()
            con.close()
            return True, None
        except Exception as e:
            if write_log is True:
                self.log.error('except command_query. error:{0} query:{1}, args:{2}'.format(e, query, args))
            try:
                if con.open is True:
                    con.rollback()
                    con.close()
            finally:
                return False, 'SQL ERROR:{0} query:{1} args:{2}'.format(str(e), query, args)

    def command_query_many(self, query, args, write_log=True):
        if write_log is True:
            self.log.trace()
        if query == '':
            if write_log is True:
                self.log.error('{} {}'.format(self.db_host_name, self.db_name), 'query is empty')
            return False, 'SQL ERROR: query is empty'
        con = None
        try:
            con, err = self.get_connection()
            if err is not None:
                raise Exception(err)

            db = con.cursor()
            db._defer_warnings = True
            db.autocommit = False
            db.executemany(query, args)
            con.commit()
            con.close()
            return True, None

        except Exception as e:
            if write_log is True:
                self.log.error('except command_query_many: error:{0} query:{1}, args:{2}'.format(e, query, args))
            try:
                if con.open is True:
                    con.rollback()
                    con.close()
            finally:
                return False, 'SQL ERROR:{0} query:{1} args:{2}'.format(str(e), query, args)


class SymbolDataSetDB(BaseDB):
    def __init__(self, db_info, log_obj=None):
        BaseDB.__init__(self, db_info, log_obj)

    def get_second_data(self, en_symbol_12_digit_code, start_date, end_date):
        # date_time, open_price, high_price, low_price, close_price, trade_count, trade_volume, trade_value
        fields = ' date_time, open_price, high_price, low_price, close_price, trade_count, trade_volume, trade_value '
        query = 'select {0} from {1} where en_symbol_12_digit_code = %s and date_time <= {2} and date_time > {3} order by {4} desc'\
            .format(fields, 'share_second_data', end_date, start_date, 'date_time')
        args = en_symbol_12_digit_code
        return self.select_query(query=query, args=args, fetchall=True, write_log=True)

    def get_second_data_for_grow(self, en_symbol_12_digit_code, start_date, end_date):
        # date_time, open_price, high_price, low_price, close_price, trade_count, trade_volume, trade_value
        fields = ' date_time, open_price, high_price, low_price, close_price, trade_count, trade_volume, trade_value '
        query = 'select {0} from {1} where en_symbol_12_digit_code = %s and date_time < {2} and date_time >= {3} order by {4} desc'\
            .format(fields, 'share_second_data', end_date, start_date, 'date_time')
        args = en_symbol_12_digit_code
        return self.select_query(query=query, args=args, fetchall=True, write_log=True)

    def have_any_data(self, en_symbol_12_digit_code, date_time):
        query = 'select count(*) from share_second_data ' \
                'where en_symbol_12_digit_code = %s and date_time < %s'

        args = (en_symbol_12_digit_code, date_time)
        res, error = self.select_query(query, args)
        if error is not None:
            return None, error

        if res[0][0] > 0:
            return True, None
        else:
            return False, None

    def get_adjusted_data(self, en_symbol_12_digit_code, adjust_type):
        if adjust_type == constants.adjusted_type_none:
            res = list()
            return res, None

        elif adjust_type == constants.adjusted_type_capital_increase:
            query = 'select do_data, coefficient from  share_adjusted_data ' \
                    'where en_symbol_12_digit_code = %s and adjusted_type = %s order by do_data desc'
            args = (en_symbol_12_digit_code, constants.adjusted_type_capital_increase)

        elif adjust_type == constants.adjusted_type_take_profit:
            query = 'select do_data, coefficient from  share_adjusted_data ' \
                    'where en_symbol_12_digit_code = %s and adjusted_type = %s order by do_data desc'
            args = (en_symbol_12_digit_code, constants.adjusted_type_take_profit)

        elif adjust_type == constants.adjusted_type_all:
            query = 'select do_data, coefficient from  share_adjusted_data ' \
                    'where en_symbol_12_digit_code = %s order by do_data desc'
            args = en_symbol_12_digit_code

        else:
            return None, 'invalid adjusted type'

        return self.select_query(query=query, args=args, fetchall=True, write_log=True)

    def get_all_adjusted_data(self, en_symbol_12_digit_code):
        query = 'select do_data, coefficient, adjusted_type, old_data, new_data from  share_adjusted_data ' \
                'where en_symbol_12_digit_code = %s order by do_data desc'
        args = en_symbol_12_digit_code
        return self.select_query(query=query, args=args, fetchall=True, write_log=True)



class OrderDataSetDB(BaseDB):
    def __init__(self, db_info, log_obj=None):
        BaseDB.__init__(self, db_info, log_obj)

    def get_new_order(self, order_run_time, write_log=True):
        query = ''
        args = ''
        error = None
        con = None
        try:
            con, err = self.get_connection()
            if err is not None:
                raise Exception(err)

            db = con.cursor()
            db._defer_warnings = True
            db.autocommit = False

            query = 'select * from waiting_order where expire_time < %s  order by add_time limit 0, 1'
            args = (get_now_time_second())
            db.execute(query, args)

            order = db.fetchall()
            if len(order) == 0:  # no any order
                con.commit()
                con.close()
                return True, 'no any order'

            order = order[0]

            query = 'update waiting_order set expire_time=%s where order_id=%s'
            args = (order_run_time + get_now_time_second(), order[0])  # order_id
            db.execute(query, args)

            con.commit()
            con.close()
            return order, error

        except Exception as e:
            if write_log is True:
                self.log.error('get_new_order. error:{0} query:{1}, args:{2}'.format(e, query, args))
            try:
                if con.open is True:
                    con.rollback()
                    con.close()
            finally:
                return False, 'SQL ERROR: get_new_order: {0} query:{1} args:{2}'.format(str(e), query, args)

    def exist_order(self, order_id):
        query = 'select count(*) from waiting_order where order_id = %s'
        args = (order_id)

        res, err = self.select_query(query=query, args=args, fetchall=True, write_log=True)
        if err is None:
            if res[0][0] > 0:
                return True
            else:
                return False
        return err

    def exist_sub_order_result(self, order_id, en_symbol_12_digit_code):
        query = 'select count(*) from sub_order_result where order_id = %s and symbol=%s'
        args = (order_id, en_symbol_12_digit_code)

        res, err = self.select_query(query=query, args=args, fetchall=True, write_log=True)
        if err is None:
            if res[0][0] > 0:
                return True
            else:
                return False
        return err

    def get_all_sub_result(self, order_id):
        query = 'select symbol, result, start_time, run_time from sub_order_result where order_id = %s order by symbol'

        args = (order_id)

        return self.select_query(query=query, args=args, fetchall=True, write_log=True)

    def remove_order(self, order_id):
        query = 'delete from waiting_order where order_id=%s'
        args = (order_id)

        return self.command_query(query, args, True)

    def clean_sub_order_result(self, order_id):
        query = 'delete from sub_order_result where order_id=%s'
        args = (order_id)

        return self.command_query(query, args, True)

    def insert_web_order_result(self, order_id, username, input_param, result, start_time, order_run_time, sum_run_time):
        query = 'insert IGNORE into back_test_order_result (order_id, username, input_param, result, start_time, order_runtime, sum_runtime) ' \
                'values (%s, %s, %s, %s, %s, %s, %s)'
        args = (order_id, username, input_param, result, start_time, order_run_time, sum_run_time)

        return self.command_query(query, args, True)

