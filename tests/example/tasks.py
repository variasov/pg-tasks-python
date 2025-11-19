import threading


class SomeTask(threading.local):

    def run(self):
        print('SomeTask run')


class AnotherTask(threading.local):

    def run(self):
        print('AnotherTask run')
