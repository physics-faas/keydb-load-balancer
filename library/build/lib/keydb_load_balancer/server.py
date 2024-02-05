import  logging
import configparser
from .keydb_monitor import KeyDBMonitor

class Server:
    def __init__(self):
        self.cfg = configparser.ConfigParser()
        self.cfg.read('cfg.ini')
        logging.basicConfig(format=self.cfg['LOGGING']['format'], level=logging.INFO, datefmt="%H:%M:%S")
        self.monitor_thread = KeyDBMonitor(self.cfg['KEYDB']['ip'], self.cfg['KEYDB']['port'])
    
    def monitor(self):
        self.monitor_thread = KeyDBMonitor(self.cfg['KEYDB']['ip'], self.cfg['KEYDB']['port'])

