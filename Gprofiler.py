import csv
import json
import time
from typing import Any


global global_profiler
global_profiler = None

def get_profiler():
    global global_profiler
    return global_profiler


def set_profiler(output_file='Gprofiler', enable=True, basetime_now=True):
    global global_profiler
    global_profiler = profiler("Gprofiler", enable=True, basetime_now=True)


class profiler(object):
    def __init__(self, output_name, enable=True, basetime_now=True):
        self.output_name = output_name
        self.enable = enable
        self.records = []
        self._processed = False
        if basetime_now:
            self.base = time.time()

    def __repr__(self):
        return "{}, {}, {}".format(self.output_name, self.enable, self.base)

    def __record__(self, name, begin, end):
        self.records.append((name, begin, end))

    def __process__(self):
        if self._processed:
            return 
        new_records = []
        for item in self.records:
            name = item[0]
            begin = (item[1] - self.base) * 1e6
            end = (item[2] - self.base) * 1e6
            new_records.append((name, begin, end))
        self.records = new_records
        self._processed = True


    def save_as_csv(self):
        self.__process__()
        with open(self.output_name + '.csv', 'w') as csvfile:
            w = csv.writer(csvfile, delimiter=',')
            w.writerow(["name", "begin_time", "end_time"])
            w.writerows(self.records)
            
    def save_as_tracing_json(self):
        tracing_json = []
        for record in self.records:
            item = {}
            item["name"] = record[0]
            item["ph"] = 'X'
            item["ts"] = record[1]
            item["dur"] = record[2] - record[1]
            item["tid"] = 1
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
            


class record_function(object):
    def __init__(self, name: str):
        self.name = name
        self.begin = None
        self.end = None
      
        
    def __enter__(self):
        global global_profiler

        if global_profiler is None:
            set_profiler()
        if global_profiler.base is None:
            global_profiler.base = time.time()

        self.begin = time.time()
        
        
    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any):
        global global_profiler
        self.end = time.time()
        global_profiler.__record__(self.name, self.begin, self.end)