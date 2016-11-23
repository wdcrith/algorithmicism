import os
import sys

filepath = os.path.join(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, os.path.join(filepath, '..'))
import task_scheduler

ts = task_scheduler.TaskScheduler()
ts.load_yaml(os.path.join(filepath, 'yaml', 'resources1.yaml'),
             os.path.join(filepath, 'yaml', 'tasks_circular_dependency.yaml')) 
ts.find_schedule()
