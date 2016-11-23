import os
import sys

sys.path.insert(0, '..')
import task_scheduler

ts = task_scheduler.TaskScheduler()
ts.load_yaml(os.path.join('yaml', 'resources_not_enough_cores.yaml'),
             os.path.join('yaml', 'tasks_not_enough_cores.yaml')) 
ts.find_schedule()