import os
import sys
import unittest

filepath = os.path.join(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, os.path.join(filepath, '..'))
import task_scheduler

class TestTaskScheduler(unittest.TestCase):

    def test_initializtion(self):
        ts = task_scheduler.TaskScheduler()
        self.assertEqual({}, ts.tasks)
        self.assertEqual([], ts.resources)

    def test_get_priority_metric(self):
        ts = task_scheduler.TaskScheduler()
        t = task_scheduler.TaskScheduler.Task('task', 2, 100)
        ts.tasks[t.name] = t

        # Verify that our priority metric is a tasks cores * full execution time
        self.assertEqual(200, ts.get_priority_metric(t.name))

    def test_prioritize_tasks(self):
        ts = task_scheduler.TaskScheduler()
        t1 = task_scheduler.TaskScheduler.Task('task1', 1, 100)
        t2 = task_scheduler.TaskScheduler.Task('task2', 3, 200)
        t3 = task_scheduler.TaskScheduler.Task('task3', 2, 200)
        ts.tasks[t1.name] = t1
        ts.tasks[t2.name] = t2
        ts.tasks[t3.name] = t3

        # Verify that prioritize_tasks returns a list of tasks in descending
        # order based on their priority metrics
        self.assertEqual([t2.name, t3.name, t1.name], ts.prioritize_tasks())

    def test_find_free_compute_resource(self):
        ts = task_scheduler.TaskScheduler()
        r1 = task_scheduler.TaskScheduler.ComputeResource('resource1', 1)
        r2 = task_scheduler.TaskScheduler.ComputeResource('resource2', 2)

        t1 = task_scheduler.TaskScheduler.Task('task1', 1, 100)
        t2 = task_scheduler.TaskScheduler.Task('task2', 2, 200)

        # If no resources available find_free_compute_resource returns None
        self.assertEqual(None, ts.find_free_compute_resource(t1))

        ts.resources = [r1]
        # With a resource that has enough free cores to handle a task
        # find_free_compute_resource should return that resource
        self.assertEqual(r1, ts.find_free_compute_resource(t1))
        # If a task is too large for a resource, find_free_compute_resource
        # return None
        self.assertEqual(None, ts.find_free_compute_resource(t2))

        ts.resources = [r1, r2]
        # With multiple resources a find_free_compute_resource will return
        # the resource with the lowest cores available >= tasks cores_required
        self.assertEqual(r1, ts.find_free_compute_resource(t1))
        self.assertEqual(r2, ts.find_free_compute_resource(t2))        

    # TODO test load_yaml, check_circular_dependencies, check_resources_needed
    # and find_schedule

class TestTask(unittest.TestCase):

    def test_initialization(self):
        """ Initialize task, verify all member variables are as expected  """
        t = task_scheduler.TaskScheduler.Task('test', 5, 100)
        self.assertEqual('test', t.name)
        self.assertEqual(5, t.cores_required)
        self.assertEqual(100, t.execution_time)
        self.assertEqual([], t.parents)
        self.assertEqual([], t.children)

    def test_add_parent(self):
        t = task_scheduler.TaskScheduler.Task('test', 1, 100)

        # Test that invalid parent tasks don't get added to parent list
        t.add_parent(None)
        self.assertEqual([], t.parents)

        # Verify that valid parent tasks get added without issue
        p = task_scheduler.TaskScheduler.Task('parent', 2, 200)
        t.add_parent(p)
        self.assertEqual([p], t.parents)

    def test_add_child(self):
        t = task_scheduler.TaskScheduler.Task('test', 1, 100)

        # Test that invalid child tasks don't get added to children
        t.add_child(None)
        self.assertEqual([], t.children)

        # Test that valid child tasks get added fine
        c = task_scheduler.TaskScheduler.Task('child', 2, 200)
        t.add_child(c)
        self.assertEqual([c], t.children)

    def test_get_full_execution_time(self):
        t = task_scheduler.TaskScheduler.Task('test', 1, 100)
        self.assertEqual(100, t.get_full_execution_time())

        # Add a parent and verify that it affects our full execution time
        p = task_scheduler.TaskScheduler.Task('parent', 2, 200)
        t.add_parent(p)
        self.assertEqual(300, t.get_full_execution_time())
        # Test that our exclution list gets utilized properly
        self.assertEqual(100, t.get_full_execution_time(exclude_parents=[p]))

        # Add a grandparent and verify that that increases our full exec time
        gp = task_scheduler.TaskScheduler.Task('grandparent', 3, 300)
        p.add_parent(gp)
        self.assertEqual(600, t.get_full_execution_time())

        # If a task's grandparent is also it's parent the grandparents time
        # should only be counted once
        t.add_parent(gp)
        self.assertEqual(600, t.get_full_execution_time())

    def test_is_done(self):
        t = task_scheduler.TaskScheduler.Task('test', 1, 100)
        self.assertFalse(t.is_done())

        t.execution_time = 0
        self.assertTrue(t.is_done())

    def test_is_parents_done(self):
        t = task_scheduler.TaskScheduler.Task('test', 1, 100)
        # No parents so they should be done
        self.assertTrue(t.is_parents_done())

        p = task_scheduler.TaskScheduler.Task('parent', 2, 200)
        t.add_parent(p)
        # With a parent added with non-zero execution time, the parents are not
        # done
        self.assertFalse(t.is_parents_done())

        # Set parent execution time to zero, is_parents_done should return true
        p.execution_time = 0
        self.assertTrue(t.is_parents_done())

    def test_is_ready(self):
        t = task_scheduler.TaskScheduler.Task('test', 1, 100)
        # With a non-zero execution time and no parents the task is ready
        self.assertTrue(t.is_ready())

        p = task_scheduler.TaskScheduler.Task('parent', 2, 200)
        t.add_parent(p)
        # With a parent who is not done a task is not ready
        self.assertFalse(t.is_ready())

        # If the parent is done and the task is not then task is ready
        p.execution_time = 0
        self.assertTrue(t.is_ready())

        # If the task is done (execution time <= 0) then we are not ready to run
        t.execution_time = -1
        self.assertFalse(t.is_ready())

    def test_iterate(self):
        t = task_scheduler.TaskScheduler.Task('test', 1, 100)
        p = task_scheduler.TaskScheduler.Task('parent', 2, 200)
        # With a not done parent iteration should not affect remaining
        # execution time
        t.add_parent(p)
        t.iterate()
        self.assertEqual(100, t.execution_time)

        # With a done parent iterating should reduce execution time by one
        # each iteration
        p.execution_time = 0
        for i in range(100):
            t.iterate()
        self.assertEqual(0, t.execution_time)

        # Once the task is done iterating will not affect the exec time further
        t.iterate()
        self.assertEqual(0, t.execution_time)


class TestComputeResource(unittest.TestCase):

    def test_initialization(self):
        """ Initialize resource, verify that all member vars are as expected """
        r = task_scheduler.TaskScheduler.ComputeResource('test', 5)
        self.assertEqual('test', r.name)
        self.assertEqual(5, r.cores_total)
        self.assertEqual(0, r.cores_used)
        self.assertEqual([], r.tasks_in_progress)

    def test_task_count(self):
        r = task_scheduler.TaskScheduler.ComputeResource('test', 5)
        self.assertEqual(0, r.task_count())
        r.tasks_in_progress = [None]
        self.assertEqual(1, r.task_count())

    def test_cores_available(self):
        r = task_scheduler.TaskScheduler.ComputeResource('test', 5)
        self.assertEqual(5, r.cores_available())

        r.cores_used = 1
        self.assertEqual(4, r.cores_available())

    def test_add_task(self):
        r = task_scheduler.TaskScheduler.ComputeResource('test', 5)
        t = task_scheduler.TaskScheduler.Task('task', 6, 100)

        # Tasks with more cores required than our resource available cores
        # will not be added to the resource
        r.add_task(t)
        self.assertEqual([], r.tasks_in_progress)

        t.cores_required = 5
        r.add_task(t)
        self.assertEqual([t], r.tasks_in_progress)
        self.assertEqual(5, r.cores_used)
        self.assertEqual(0, r.cores_available())

    def test_remove_task(self):
        r = task_scheduler.TaskScheduler.ComputeResource('test', 5)
        t1 = task_scheduler.TaskScheduler.Task('task1', 1, 100)
        t2 = task_scheduler.TaskScheduler.Task('task2', 2, 200)
        
        # Tasks not in progress on the resource will not affect the resources
        # task list or core count when removal is attempted
        r.add_task(t1)
        r.remove_task(t2)
        self.assertEqual([t1], r.tasks_in_progress)
        self.assertEqual(t1.cores_required, r.cores_used)

        # Removing a task on the resource will affect the task list and
        # cores_used var
        r.remove_task(t1)
        self.assertEqual([], r.tasks_in_progress)
        self.assertEqual(0, r.cores_used)

    def test_iterate(self):
        r = task_scheduler.TaskScheduler.ComputeResource('test', 5)
        t = task_scheduler.TaskScheduler.Task('task', 1, 50)

        r.add_task(t)
        for i in range(t.execution_time-1):
            self.assertFalse(r.iterate())
        # ComputeResource.iterate() will return True when a task is completed
        self.assertTrue(r.iterate())
        # If a task is done it will be removed from tasks_in_progress
        self.assertTrue(t.is_done())
        self.assertEqual([], r.tasks_in_progress)
        # and any cores that were claimed by the task will be freed up
        self.assertEqual(r.cores_total, r.cores_available())


if __name__ == '__main__':
    unittest.main()
