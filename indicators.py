


class BaseIndicator:
    def __init__(self, data_set):
        self.data_set = data_set


class SMA(BaseIndicator):
    def __init__(self, data_set, period):
        BaseIndicator.__init__(self, data_set)





class Indicator:
    def __init__(self, data_set):
        self.data_set = data_set
        self.indicators_list = list()  # [name, params, object]


    def find_indicator(self, name, param):
        for obj in self.indicators_list:
            if obj[0] == name:
                if obj[1] == param:
                    return obj[2]
        return None



    def SMA(self, period):
        obj = self.find_indicator('SMA', str(period))
        if obj is None:
            obj = SMA(data_set=self.data_set, period=period)
            self.indicators_list.append(['SMA', str(period), obj])
        return obj



