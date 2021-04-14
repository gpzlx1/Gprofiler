import os
import csv
import json
import time
import pickle
import threading
from typing import Any
from queue import Queue
from multiprocessing import Manager

global global_profiler
global_profiler = None

def get_profiler():
    global global_profiler
    return global_profiler


def set_profiler(output_file=None, enable=True, basetime_now=True):
    global global_profiler
    if output_file is None:
        output_file = str(os.getpid())
    global_profiler = profiler(output_file, enable=enable, basetime_now=basetime_now)
    return global_profiler


class profiler(object):
    def __init__(self, output_name, enable=True, basetime_now=True):
        self.output_name = output_name
        self.enable = enable
        self.queue = Queue()
        self.records = []
        self._processed = False
        self._register_record_func = Manager().dict()
        if basetime_now:
            self.base = time.time()

    def __repr__(self):
        return "{}, {}, {}".format(self.output_name, self.enable, self.base)

    def __record__(self, record):
        self.queue.put(record)

    def __process__(self):
        if self._processed:
            return 

        while not self.queue.empty():
            record = self.queue.get()
            self.records.append(record)

        for item in self.records:

            item[1] = (item[1] - self.base) * 1e6
            item[2] = (item[2] - self.base) * 1e6

        self._processed = True

    def __check_no_thread_running__(self):
        if len(self._register_record_func) == 0:
            return
        for key, value in self._register_record_func.items():
            if value == 'Running':
                print("[Warning] tid {} still running".format(key))

    def save_as_csv(self):
        self.__check_no_thread_running__()
        self.__process__()
        with open(self.output_name + '.csv', 'w') as csvfile:
            w = csv.writer(csvfile, delimiter=',')
            w.writerow(["name", "begin_time", "end_time"])
            w.writerows(self.records)
            
    def save_as_tracing_json(self):
        self.__check_no_thread_running__()
        self.__process__()
        tracing_json = []
        for record in self.records:
            item = {}
            item["name"] = record[0]
            item["ph"] = 'X'
            item["ts"] = record[1]
            item["dur"] = record[2] - record[1]
            item["tid"] = record[3]
            item["pid"] = self.output_name
            item["args"] = {}
            tracing_json.append(json.dumps(item))

        with open(self.output_name + ".json", "w") as f:
            f.write('[\n')
            for i in range(len(tracing_json) - 1):
                f.write("\t" + tracing_json[i] + ",\n")
            if len(tracing_json) > 0:
                f.write("\t" + tracing_json[-1] + "\n")
            f.write(']')

    def save_as_gporfiler(self):
        self.__check_no_thread_running__()
        with open(self.output_name + ".gpout", "w") as f:
            f.write(pickle.dumps(self))


class record_function(object):
    def __init__(self, name: str):
        self.name = name
        self.begin = None
        self.end = None
        self.threadID = threading.current_thread().ident

        global global_profiler
        self.enable = global_profiler.enable and not global_profiler._processed
      
        
    def __enter__(self):
        if not self.enable:
            return 
        global global_profiler

        if global_profiler is None:
            set_profiler()
        if global_profiler.base is None:
            global_profiler.base = time.time()

        global_profiler._register_record_func[self.threadID] = 'Running'
        self.begin = time.time()
        
        
    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any):
        if not self.enable:
            return 
        global global_profiler
        self.end = time.time()
        global_profiler._register_record_func[self.threadID] = 'Stop'
        global_profiler.__record__([self.name, self.begin, self.end, self.threadID])