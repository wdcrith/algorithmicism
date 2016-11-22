import yaml
import copy

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
            self.parents_done = False

        def add_parent(self, parent):
            if parent:
                self.parents.append(parent)

        def add_child(self, child):
            if child:
                self.children.append(child)

        def get_full_execution_time(self, exclude_parents=[]):
            full_execution_time = self.execution_time
            # We want to ignore parents that overlap between dependent tasks
            for p in list(set(self.parents) - set(exclude_parents)):
                full_execution_time += p.get_full_execution_time(exclude_parents+self.parents)
            return full_execution_time

        def is_done(self):
            return self.execution_time <= 0

        def is_parents_done(self):
            if not self.parents_done:
                if len(self.parents):
                    self.parents_done = all(map(lambda p: p.is_done(), self.parents))
                else:
                    self.parents_done = True
            return self.parents_done

        def is_ready(self):
            return not self.is_done() and self.is_parents_done()

        def iterate(self):
            if self.is_ready():
                self.execution_time -= 1


    class ComputeResource:
        def __init__(self, name, cores_total):
            self.name = name
            self.cores_total = cores_total
            self.cores_in_use = 0
            self.tasks_in_progress = []

        def task_count(self):
            return len(self.tasks_in_progress)

        def add_task(self, task):
            if task.cores_required <= self.cores_available():
                self.tasks_in_progress.append(task)
                print(task.name + ': ' + self.name)
                self.cores_in_use += task.cores_required
                return True
            return False

        def remove_task(self, task):
            self.tasks_in_progress.remove(task)
            self.cores_in_use -= task.cores_required

        def cores_available(self):
            return self.cores_total - self.cores_in_use

        def iterate(self):
            task_completed = False
            for task in self.tasks_in_progress:
                task.iterate()
                if task.is_done():
                    self.remove_task(task)
                    task_completed = True
            return task_completed


    def prioritize_tasks(self):
        def get_priority_metric(name):
            # Our priority metric is the full execution time of a task
            # (execution time of the task plus all ancestor tasks execution times
            # Then we multiply that by cores required so that long tasks get priority
            # and core heavy tasks will get an extra advantage
            pm = self.tasks[name].cores_required * self.tasks[name].get_full_execution_time()
            return pm
        prioritized_tasks = sorted(self.tasks, key=get_priority_metric, reverse=True)
        return prioritized_tasks

    def find_free_compute_resource(self, task, resources):
        for resource in sorted(resources, key=lambda r: r.cores_available()):
            if resource.cores_available() >= task.cores_required:
                return resource
        return None

    def load_yaml(self, resources_path, tasks_path):
        resource_dict = yaml.safe_load(open(resources_path, 'r'))
        task_dict = yaml.safe_load(open(tasks_path, 'r'))

        # Initialize task objects from dict
        for name, value in task_dict.items():
            self.tasks[name] = self.Task(name, value['cores_required'],
                value['execution_time'])

        # Link parents<->children
        for name, value in task_dict.items():
            for pname in [p.strip() for p in value.get('parent_tasks', '').split(',')]:
                if pname:
                    parent = self.tasks.get(pname, None)
                    if parent is not None:
                        self.tasks[name].add_parent(parent)
                        parent.add_child(self.tasks[name])
                    else:
                        print('ERROR: {0}\'s parent task: "{1}", does not exist in task list.'.format(name, pname))
                        exit(-1)

        # Initialize sorted (by name) compute resources list
        for name, cores in sorted(resource_dict.items(), key=lambda r: r[0]):
            self.resources.append(self.ComputeResource(name, cores))

    def has_circular_dependencies(self):
        def traverse_children(taskname, children, entry=False):
            for child in children:
                if taskname == child.name:
                    return True
                return traverse_children(taskname, child.children)
            return False

        for name, task in self.tasks.items():
            if traverse_children(task.name, task.children, entry=True):
                return True

    def find_schedule(self):
        if self.has_circular_dependencies():
            print("ERROR: Circular dependencies detected between tasks")
            exit(-1)
        current_ticks = 0 
        prioritized_tasks = self.prioritize_tasks()
        print(prioritized_tasks)
        while prioritized_tasks:
            tasks_processed = False
            # For each prioritized task try and assign it to a free compute resource
            for t in prioritized_tasks:
                task = self.tasks[t]
                if task.is_ready():
                    res = self.find_free_compute_resource(task, self.resources)
                    #if we
                    if res is None:
                        continue
                    print('-'*40 + str(current_ticks) + '-'*40)
                    if res.add_task(task):
                        prioritized_tasks.remove(task.name)
                        tasks_processed = True
                    else:
                        print("failed to add task to resource")
            for resource in self.resources:
                resource.iterate()
            current_ticks += 1

        # Strictly for determining what the makespan for the schedule is
        while not all(map(lambda x: x.task_count()==0, self.resources)):
            current_ticks += 1
            for resource in self.resources:
                resource.iterate()
        print("MAKESPAN: {0}".format(current_ticks))
        

if __name__ == '__main__':
    ts = TaskScheduler()
    ts.load_yaml('resources.yaml', 'tasks.yaml')
    ts.find_schedule()
