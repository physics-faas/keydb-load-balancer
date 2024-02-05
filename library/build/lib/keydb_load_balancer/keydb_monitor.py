import threading
import re
import time
from keydb import KeyDB
from prometheus_client import start_http_server, Gauge

class KeyDBMonitor:
    def __init__(self, host: str, port:int):
        self.port = port
        self.host = host
        self.keys = {}  #key - slot
        self.access_count = {} #slot - nops
        self.db = KeyDB(host=host, port=port, password=None)
        self.monitor_thread = threading.Thread(target=self.monitor_operations)
        self.monitor_thread.start()
        self.exporter_thread = threading.Thread(target=self.prometheusExporter)
        self.exporter_thread.start()

    def monitor_operations(self):
        pattern = re.compile(r'^\d+\.\d+ \[\d+ \d+\.\d+\.\d+\.\d+:\d+\] "(get|set|del|GET|SET|DEL)" ".*"$')
        while True:
            monitor_result = self.db.execute_command("MONITOR")        
            if pattern.match(monitor_result.decode('utf-8')):
                key = monitor_result.decode('utf-8').split('"')[3]
                print(key)
                self.handle_operation(key)

    def handle_operation(self, key: str):        
        if key not in self.keys:
            slot_info = int(self.db.cluster('KEYSLOT', key))
            self.keys[key] = slot_info

        if self.keys[key] not in self.access_count:
            self.access_count[self.keys[key]] = 1
        else:
            self.access_count[self.keys[key]] += 1         
        
        print(self.access_count)

    def prometheusExporter(self):
        start_http_server(8085)
        g = Gauge('keydb_slot_ops', 'Number of ops per slot each 5 seconds', ['instance', 'slot'])
        while True:
            auxAccess_count = self.access_count
            self.access_count = {}
            for nop in auxAccess_count:
                g.labels(self.host+':'+str(self.port), nop).set(float(auxAccess_count[nop]))
            time.sleep(5)


