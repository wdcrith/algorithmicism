import os
import sys

filepath = os.path.join(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, os.path.join(filepath, '..'))
import task_scheduler

ts = task_scheduler.TaskScheduler()
ts.load_yaml(os.path.join(filepath, 'yaml_files', 'resources_not_enough_cores.yaml'),
             os.path.join(filepath, 'yaml_files', 'tasks_not_enough_cores.yaml')) 
ts.find_schedule()
