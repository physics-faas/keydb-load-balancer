import subprocess
import threading
import re
import time
import socket
from keydb import KeyDB
from prometheus_client import start_http_server, Gauge
import logging
import configparser

class KeyDBMonitor:
    def __init__(self, cfg):
        self.cfg = configparser.ConfigParser()
        self.cfg.read('cfg.ini')
        logging.basicConfig(format=self.cfg['LOGGING']['format'], level=logging.INFO, datefmt="%H:%M:%S")
        self.prometheus_port = int(self.cfg['KEYDB']['port_metrics'])
        self.monitor_port = int(self.cfg['MONITOR']['port'])
        self.keys = {}  #key - slot
        self.access_count = {} #{ slot: instance: {get: ,set: ,del:} ...}
        self.instances = self.cfg['KEYDB']['instances'].split(', ')
        
        self.db = KeyDB(host=self.instances[0].split(':')[0], port=self.instances[0].split(':')[1], password=None)
        
        self.monitor_exporters = []
        self.monitoring_metrics_processor = []
        thread_id = 0
        for instance in self.instances:            
            self.monitor_exporters.append(threading.Thread(target=self.monitor_exporter, args=(instance, self.monitor_port)))
            self.monitor_exporters[thread_id].start()
            time.sleep(6)
            self.monitoring_metrics_processor.append(threading.Thread(target=self.monitor_operations, args=(instance, self.monitor_port )))
            self.monitoring_metrics_processor[thread_id].start()

            self.monitor_port+=1
            thread_id+=1

        self.exporter_thread = threading.Thread(target=self.prometheusExporter)
        self.exporter_thread.start()

    def monitor_exporter(self, instance:str, port:int):        
        logging.info('Monitor: '+ instance +' -- '+ str(port))
        command = f"ssh {instance.split(':')[0]} ' sudo pkill -9 nc; {self.cfg['KEYDB']['path_keydb_cli']}  -p {instance.split(':')[1]} monitor | nc -l -p {port}'"
        # command = f"ssh {instance.split(':')[0]} keydb-cli -p {instance.split(':')[1]} monitor | nc -l -p {port}"
        subprocess.run(command, shell=True)

    def monitor_operations(self, instance:str, port:str):        
        pattern = re.compile(r'^\d+\.\d+ \[\d+ \d+\.\d+\.\d+\.\d+:\d+\] "(get|set|del)" ".*"$')        
        print(instance)      
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.connect((instance.split(':')[0], port)) 
        except:
            time.sleep(2)
            self.monitor_operations(instance, port)

        print('Correct connection: '+ str(port))

        while 1:
            key = s.recv(1024).decode('utf-8').lower()
            if pattern.match(key):                
                op = key.split('"')[1]
                key = key.split('"')[3]             
                self.handle_operation(key, op, instance)


    def handle_operation(self, key: str, operation: str, instance:str):        
        if key not in self.keys:
            slot_info = int(self.db.cluster('KEYSLOT', key))
            self.keys[key] = slot_info

        if self.keys[key] not in self.access_count:
            self.access_count[self.keys[key]] = {instance: {'get': 0, 'set': 0,'del': 0}}
        elif instance not in self.access_count[self.keys[key]]:
            self.access_count[self.keys[key]][instance] =  {'get': 0, 'set': 0,'del': 0}
        
        self.access_count[self.keys[key]][instance][operation] += 1        

    def prometheusExporter(self):
        start_http_server(self.prometheus_port)
        g = Gauge('keydb_slot_ops', 'Number of ops per slot each 5 seconds', ['instance', 'slot', 'operation_type'])
        while True:  
            aux_access_count = self.access_count.copy()
            # print(aux_access_count)
            for nslot in aux_access_count:
                for instance in aux_access_count[nslot]:
                    g.labels(instance, nslot, 'get').set(float(aux_access_count[nslot][instance]['get']))       
                    g.labels(instance, nslot, 'set').set(float(aux_access_count[nslot][instance]['set']))       
                    g.labels(instance, nslot, 'del').set(float(aux_access_count[nslot][instance]['del']))       
                    g.labels(instance, nslot, 'all').set(float(aux_access_count[nslot][instance]['get']) + float(aux_access_count[nslot][instance]['set']) + float(aux_access_count[nslot][instance]['del']))                       
                    self.access_count[nslot][instance]['get'] = 0
                    self.access_count[nslot][instance]['set'] = 0
                    self.access_count[nslot][instance]['del'] = 0        
            time.sleep(float(self.cfg['PROMETHEUS']['metric_export_interval']))

