import  logging
import configparser
from .keydb_monitor import KeyDBMonitor
from .load_balancer import Loadbalancer

class Server:
    def __init__(self):
        self.cfg = configparser.ConfigParser()
        self.cfg.read('cfg.ini')
        logging.basicConfig(format=self.cfg['LOGGING']['format'], level=logging.INFO, datefmt="%H:%M:%S")        
    
    def monitor(self):
        self.monitor_thread = KeyDBMonitor(self.cfg)
        # self.monitor_thread = KeyDBMonitor(self.cfg)
    
    def load_balancer(self):
        self.balancer_thread = Loadbalancer(self.cfg)
