
import configparser
import logging


class Loadbalancer:
    def __init__(self):
        self.cfg = configparser.ConfigParser()
        self.cfg.read('cfg.ini')
        logging.basicConfig(format=self.cfg['LOGGING']['format'], level=logging.INFO, datefmt="%H:%M:%S") 