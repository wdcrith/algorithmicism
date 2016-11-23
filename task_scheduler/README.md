# TaskScheduler project

So, after having put some time into this exercise I figure I should give a brief on what I
learned, decisions I made and things that need to be done going forward (hypothetically).

First off the problem at hand (scheduling tasks onto a heterogeneous collection of compute
resources) originally struck me as a traveling salesman problem with a bit of extra complexity,
so I had that as a base for my understanding. But since I have never dealt with this sort of problem
I did some googling about the interwebs. Came up with what looked to be a pretty good
description for the problem called ["Job Shop Scheduling"](https://en.wikipedia.org/wiki/Job_shop_scheduling)

A wikipedia page and a few whitepages on cloud computing with
heterogeneous computing resources (too theoretical for the task at hand) later.
I came upon the algorithm which I would leverage to solve the problem at hand, [List Scheduling](https://en.wikipedia.org/wiki/List_scheduling)
Not the most efficient algorithm but simple, easy to understand and quick to be brought up.

So with list scheduling decided upon I had to figure out a priority measure for prioritizing tasks to compute resources.

Since we have two main task variables (compute time and compute cores required) it seemed safe to use both in a priority measure
So I multiplied the two together so both long running tasks and core intensive tasks would be prioritized since
we don't want long running tasks to start at the end of the schedule. Doing so would just add that time onto our makespan,
instead long running tasks should start as early as possible so other short running tasks could be running in parallel resulting in
a smaller makespan. Second by prioritizing core intensive tasks we would be sure to not have a task that requires more cores
waiting around for them to open up.

Coding ensued.

Some time later...

I had the basic code up as seen in the initial commit of the github repo. Tested it against several task and resource inputs
and from my figuring it should find optimum schedules for any input.

Relatively happy with the code and not worrying too much about efficiency of the algorithm
(since scale is unknown at this stage and why optimize if simple methods are proficient)
I started to look at some of the error cases which could mess up the scheduling some of the easiest to test
were things like broken yaml files, yaml files with incorrect key names, tasks with core requirements that exceeded
any of our resources. As well as circular dependency between tasks, to check for this I wrote a naive check which traverses
through the descendants of a task. If the number of descendants seen exceeds our task count we know for certain that
a cycle is in our graph. This check could be made more efficient by using a smarter cycle detection algorithm but for our
purposes the naive approach will do (for now).

With those cases handled I wrote a base of unit tests for most of the methods (skipped some of the more complex due to time).
though for future improvements all classes and methods should be tested.

And that ends the excercise, going forward if scale is any issue the underlying list scheduling algorithm could be
swapped out for a more modern and effiecient solution. We could rewrite the whole thing in a compiled language (C++)
for a perforcmance increase. More unit tests should be written for the segments of code that weren't covered (modularizing
methods as necessary). And more automated end to end testing of the scheduler is always a good idea.
