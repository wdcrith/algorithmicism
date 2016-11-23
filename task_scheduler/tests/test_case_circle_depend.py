import os
import sys

sys.path.insert(0, '..')
import task_scheduler

ts = task_scheduler.TaskScheduler()
ts.load_yaml(os.path.join('yaml', 'resources1.yaml'),
             os.path.join('yaml', 'tasks_circular_dependency.yaml')) 
ts.find_schedule()
