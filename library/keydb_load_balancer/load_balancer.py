
import configparser
import logging
import time
import threading
from prometheus_api_client import PrometheusConnect
from keydb import KeyDB
import redis
from datetime import datetime
class Loadbalancer:
    def __init__(self, cfg):        
        self.cfg = cfg
        self.prometheus_connection = PrometheusConnect(self.cfg['PROMETHEUS']['protocol']+self.cfg['PROMETHEUS']['ip']+':'+self.cfg['PROMETHEUS']['port'])
        
        self.instances = self.cfg['KEYDB']['instances'].split(', ')
        self.db = KeyDB(host=self.instances[0].split(':')[0], port=int(self.instances[0].split(':')[1]), password=None)

        nodes = self.db.cluster('NODES')
        metric_cpu_cores_by_instance = self.prometheus_connection.custom_query('redis_server_threads_total{job="redis_exporter_targets"}')

        self.cores_capacity_node_by_instance = {}
        for instance in metric_cpu_cores_by_instance:            
            n_cores = int(instance['value'][1])
            cpu_capacity = float(cfg['LOAD_BALANCER']['cpu_overloaded_percent']) * n_cores
            node_id = nodes[instance['metric']['instance'].split('//')[1]]['node_id']
            self.cores_capacity_node_by_instance[instance['metric']['instance'].split('//')[1]] = [n_cores, cpu_capacity, node_id] # {instance: [n_cores, %max_cpu, node_id], ...} 

        print('****************')
        print(self.cores_capacity_node_by_instance)
        print('****************')

        self.balancer_thread = threading.Thread(target=self.balancer)
        self.balancer_thread.start()

    def balancer(self):
        while True:

            slots_moved = []
            start = time.time()

            metric_cpu_usage_percent_by_instance = self.prometheus_connection.custom_query('sort_desc(100 * (rate(redis_cpu_sys_seconds_total{job="redis_exporter_targets"}[1m]) +rate(redis_cpu_user_seconds_total{job="redis_exporter_targets"}[1m])))')
            metric_memory_usage_by_instance = self.prometheus_connection.custom_query('sum(redis_memory_used_bytes{job="redis_exporter_targets"}) by (instance) / 1024 / 1024')

            instance_more_loaded_cpu = next(iter(metric_cpu_usage_percent_by_instance))
            instance_more_loaded_memory = next(iter(metric_memory_usage_by_instance))
            instance_less_loaded_cpu = list(metric_cpu_usage_percent_by_instance)[-1] 
            instance_less_loaded_memory = list(metric_memory_usage_by_instance)[-1] 
            
            instance_more_loaded = [instance_more_loaded_cpu['metric']['instance'], instance_more_loaded_cpu['value'][1], instance_more_loaded_memory['value'][1], -1] # [instance, %cpu, mem_used, slot_more_used]
            instance_less_loaded = [instance_less_loaded_cpu['metric']['instance'], instance_less_loaded_cpu['value'][1], instance_less_loaded_memory['value'][1]] # [instance, %cpu, mem_used]
            
            instance_name_over = instance_more_loaded[0].split('//')[1]
            # print(instance_more_loaded[1])
            if float(instance_more_loaded[1]) > float(self.cores_capacity_node_by_instance[instance_name_over][1]): #and (float(instance_less_loaded[1]) <= float(self.cores_capacity_node_by_instance[instance_name_over][1]) and (float(instance_more_loaded[2])/1024) > float(self.cfg['LOAD_BALANCER']['memory_overloaded_gb']) and (float(instance_less_loaded[2])/1024) <= float(self.cfg['LOAD_BALANCER']['memory_overloaded_gb'])):
                query = 'sort_desc(sum_over_time(keydb_slot_ops{operation_type="all", exported_instance="%s"}[3m]))' % instance_name_over
                metric_nops_by_slot = self.prometheus_connection.custom_query(query)
                instance_more_loaded[3] = int(next(iter(metric_nops_by_slot))['metric']['slot'])
                logging.info('the instance: ' + instance_more_loaded[0] + ' and slot: ' + str(instance_more_loaded[3]) + ' - ' + instance_less_loaded[0])
            
            logging.info('load balancer decision time: ' + str((time.time() - start)*1000) + 'ms')
            print('instance overload cpu: '+ str(instance_more_loaded[1]))
            instance_name_nonover = instance_less_loaded[0].split('//')[1]
            if instance_more_loaded[3] >= 0:
                print(datetime.utcnow().strftime('%F %T.%f')[:-3])
                start_time_total = time.time()
                with KeyDB(host=instance_name_over.split(':')[0], port=int(instance_name_over.split(':')[1]), decode_responses=True) as source_node:
                    with KeyDB(host=instance_name_nonover.split(':')[0], port=int(instance_name_nonover.split(':')[1]), decode_responses=True) as destination_node:
                        try:                                
                            slot_id = instance_more_loaded[3]
                            destination_node.cluster('setslot', slot_id, 'IMPORTING', self.cores_capacity_node_by_instance[instance_name_over][2])
                            source_node.cluster('setslot', slot_id, 'MIGRATING', self.cores_capacity_node_by_instance[instance_name_nonover][2])                            
                            keys_slot = source_node.cluster('getkeysinslot', slot_id, int(self.cfg['LOAD_BALANCER']['number_keys_migrate']))
                            total_keys_move = len(keys_slot)
                            while keys_slot:
                                source_node.migrate(instance_name_nonover.split(':')[0], int(instance_name_nonover.split(':')[1]), keys_slot, self.cfg['LOAD_BALANCER']['destination_db'], self.cfg['LOAD_BALANCER']['timeout'])
                                keys_slot = source_node.cluster('getkeysinslot', slot_id, int(self.cfg['LOAD_BALANCER']['number_keys_migrate']))
                                total_keys_move += len(keys_slot)
                            destination_node.cluster('setslot', slot_id, 'NODE', self.cores_capacity_node_by_instance[instance_name_nonover][2])
                            source_node.cluster('setslot', slot_id, 'NODE', self.cores_capacity_node_by_instance[instance_name_nonover][2])
                        except redis.exceptions.ResponseError as e:
                            logging.error(f"Error when moving the key: {e}")
                logging.info('load balancer total move: ' + str((round((time.time() - start_time_total)*1000), 2)) + 'ms and '+ str(total_keys_move)+ " keys were moved")
                print(datetime.utcnow().strftime('%F %T.%f')[:-3])
            time.sleep(float(self.cfg['LOAD_BALANCER']['inverval_sec']))
