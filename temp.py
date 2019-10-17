import sys
# import trace
import threading
import time


class thread_with_trace(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.killed = False

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

    def run(self):
        sys.settrace(self.globaltrace)

        self.func()

    def func(self):
        while True:
            try:
                e = self.name
                print('thread running')
                time.sleep(1)
            except Exception as e:
                print(e)
            finally:
                #print('finaly: {}')
                print('finaly: {}'.format(e))



class thread_with_trace1(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.killed = False

    def start0(self):
        self.__run_backup = self.run
        self.run = self.__run

        threading.Thread.start(self)

    def __run(self):
        sys.settrace(self.globaltrace)
        self.__run_backup()
        self.run = self.__run_backup

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

    def run(self):
        sys.settrace(self.globaltrace)

        self.func()

    def func(self):
        while True:
                try:
                    e = 'a'
                    print('thread running')
                    time.sleep(1)
                except Exception as e:
                    print(e)
                finally:
                    #print('finaly: {}')
                    print('finaly: {}'.format(e))


def func():
    while True:
        print('thread running')
        time.sleep(1)


t1 = thread_with_trace()
t1.start()
time.sleep(2.1)
t1.kill()
t1.join()
if not t1.isAlive():
    print('---------------------------------- thread killed')
