import os
import sys

filepath = os.path.join(os.path.dirname(os.path.realpath(__file__)))

def check_python3():
    if sys.version_info < (3, 0):
        sys.stdout.write('\nRequires Python3, ABORTING\n')
        sys.exit(1)

if __name__ == 'task_scheduler':
    check_python3();

class TaskScheduler:
    def __init__(self):
        self.tasks = {}
        self.resources = []

    class Task:
        def __init__(self, name, cores_required, execution_time):
            self.name = name
            self.cores_required = cores_required
            self.execution_time = execution_time
            self.parents = []
            self.children = []

        def add_parent(self, parent):
            if isinstance(parent, self.__class__):
                self.parents.append(parent)

        def add_child(self, child):
            if isinstance(child, self.__class__):
                self.children.append(child)

        def get_full_execution_time(self, exclude_parents=[]):
            full_execution_time = self.execution_time
            # We want to ignore parents that overlap between dependent tasks
            # due to including them leading to multiples of the same execution times
            # being includes in the full execution times
            for p in list(set(self.parents) - set(exclude_parents)):
                full_execution_time += p.get_full_execution_time(exclude_parents+self.parents)
            return full_execution_time

        def is_done(self):
            return self.execution_time <= 0

        def is_parents_done(self):
            if len(self.parents):
                return all(map(lambda p: p.is_done(), self.parents))
            return True

        def is_ready(self):
            return not self.is_done() and self.is_parents_done()

        def iterate(self):
            if self.is_ready():
                self.execution_time -= 1


    class ComputeResource:
        def __init__(self, name, cores_total):
            self.name = name
            self.cores_total = cores_total
            self.cores_used = 0
            self.tasks_in_progress = []

        def task_count(self):
            return len(self.tasks_in_progress)

        def cores_available(self):
            return self.cores_total - self.cores_used

        def add_task(self, task):
            if task.cores_required <= self.cores_available():
                self.tasks_in_progress.append(task)
                #Print out task name and what compute resource it's on
                print(task.name + ': ' + self.name)
                self.cores_used += task.cores_required
                return True
            return False

        def remove_task(self, task):
            try:
                self.tasks_in_progress.remove(task)
                self.cores_used -= task.cores_required
            except ValueError:
                pass # If the task doesn't exist in our list ignore it

        def iterate(self):
            """ Iterate all tasks in progress removing tasks once done """
            task_completed = False
            for task in self.tasks_in_progress:
                task.iterate()
                if task.is_done():
                    self.remove_task(task)
                    task_completed = True
            return task_completed

    def get_priority_metric(self, name):
        """ Our priority metric is the full execution time of a task
         (execution time of the task plus all ancestor tasks execution times)
          multiplied by cores required so that long tasks get priority
         and core heavy tasks will get an extra advantage """
        return (self.tasks[name].cores_required *
            self.tasks[name].get_full_execution_time())

    def prioritize_tasks(self):
        """ Prioritize tasks based on priority metric """
        prioritized_tasks =\
            sorted(self.tasks, key=self.get_priority_metric, reverse=True)
        return prioritized_tasks

    def find_free_compute_resource(self, task):
        """ Find the compute resource based on cores available non-greedy """
        for resource in sorted(self.resources, key=lambda r: r.cores_available()):
            if resource.cores_available() >= task.cores_required:
                return resource
        return None

    def load_yaml(self, resources_path, tasks_path):
        """ Load resources and task yaml description files and
            initialize classes for each """
        # Attempt to use the system's pyyaml, if that fails use our own
        try:
            import yaml
        except ImportError:
            sys.path.insert(0, os.path.join(filepath, 'pyyaml'))
            import yaml

        # Try and load yaml files in separate try blocks so we can error our
        # more descriptively
        try:
            resource_dict = yaml.safe_load(open(resources_path, 'r'))
        except:
            print('\nERROR: "{0}", failed to load\n'.format(resources_path))
            exit(-1)
        try:
            task_dict = yaml.safe_load(open(tasks_path, 'r'))
        except:
            print('\nERROR: "{0}", failed to load\n'.format(tasks_path))
            exit(-1)

        # Initialize task objects from dict
        for name, value in task_dict.items():
            try:
                self.tasks[name] = self.Task(name, value['cores_required'],
                    value['execution_time'])
            except KeyError:
                print('\nERROR: task "{0}", expected keys in load tasks yaml are not where expected\n'.format(name))
                exit(-1)

        # Link parents<->children
        for name, value in task_dict.items():
            for pname in [p.strip() for p in value.get('parent_tasks', '').split(',')]:
                if pname:
                    parent = self.tasks.get(pname, None)
                    if parent is not None:
                        self.tasks[name].add_parent(parent)
                        parent.add_child(self.tasks[name])
                    else:
                        print('\nERROR: {0}\'s parent task: "{1}", does not exist in task list\n'.format(name, pname))
                        exit(-1)

        # Initialize sorted (by name) compute resources list
        for name, cores in sorted(resource_dict.items(), key=lambda r: r[0]):
            if not isinstance(cores, int):
                print('\nERROR: resource "{0}" cores count is not an interger value\n'.format(name))
                exit(-1)
            self.resources.append(self.ComputeResource(name, cores))

    def check_circular_dependencies(self):
        """ Simple circular build dependency check by counting depedents
            seen and comparing to total task count"""
        for name, task in self.tasks.items():
            import copy
            dependents = copy.copy(task.children)
            for idx, depend in enumerate(dependents):
                if idx > len(self.tasks.keys()):
                    print('\nERROR: Circular dependencies detected between tasks\n')
                    exit(-1)
                dependents += depend.children

    def check_resources_needed(self):
        """ Verify that all given tasks are able to fit on a resource """
        for name, task in self.tasks.items():
            if self.find_free_compute_resource(task) is None:
                print('\nERROR: Task "{0}" requires {1} cores, no resoure can handle that requirement\n'.format(task.name, task.cores_required))
                exit(-1)

    def find_schedule(self):
        """ Simulator for finding the optimum task schedule """

        # Check for circular dependencies and tasks that are oversized for our
        # resources, erroring our and exiting if so
        self.check_circular_dependencies()
        self.check_resources_needed()

        current_ticks = 0 
        prioritized_tasks = self.prioritize_tasks()
        print('PRIORITIZED_TASKS: ' + str(prioritized_tasks) + '\n')
        while prioritized_tasks:
            tasks_processed = False
            # For each prioritized task try and assign 
            # it to a free compute resource
            for t in prioritized_tasks:
                task = self.tasks[t]
                if task.is_ready():
                    res = self.find_free_compute_resource(task)
                    # If we found a free resource let's add the task to it
                    if res:
                        # Uncomment line for testing to see at what ticks tasks
                        # were added
                        #print('-'*40 + str(current_ticks) + '-'*40)
                        if res.add_task(task):
                            prioritized_tasks.remove(task.name)
                            tasks_processed = True
                        else:
                            print("Failed to add task to resource")
            for resource in self.resources:
                resource.iterate()
            current_ticks += 1

        # Strictly for determining what the makespan for the schedule is
        while not all(map(lambda x: x.task_count()==0, self.resources)):
            current_ticks += 1
            for resource in self.resources:
                resource.iterate()
        # If we care to double check the makespan, uncomment and run
        print("\nSCHEDULE MAKESPAN: {0}".format(current_ticks))
        

if __name__ == '__main__':
    check_python3()
    import argparse

    argparser = argparse.ArgumentParser(
        description='Generates task schedule based on a task list' +
                    ' and compute resources available')

    argparser.add_argument('task_yaml', type=str, help='task description yaml file')
    argparser.add_argument('resource_yaml', type=str, help='resource description yaml file')

    args = argparser.parse_args()

    ts = TaskScheduler()
    ts.load_yaml(args.resource_yaml, args.task_yaml)
    ts.find_schedule()
