import os
import sys

sys.path.insert(0, '..')
import task_scheduler

ts = task_scheduler.TaskScheduler()
ts.load_yaml(os.path.join('yaml', 'resources2.yaml'),
             os.path.join('yaml', 'tasks2.yaml')) 
ts.find_schedule()
