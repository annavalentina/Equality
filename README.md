# Equality
﻿The aim of this project is to optimize streaming analytics on Geo-distributed, heterogeneous and edge computing environments. The objective is to find the trade-off between latency and data-quality check on the data by finding an optimal task placement of devices and a DQ check fraction. The custom Storm scheduler (compatible with Storm 2.1.0) enforces the offline algorithms placement decisions. The Storm topologies also enforce configurations like selectivity of the operators and tuple distribution to tasks. 


Equality Scheduler Set-up:

    · The scheduler jar must be placed in lib folder of Nimbus.
    · Nimbus's storm.yaml file must contain the following line:
         storm.scheduler: "com.equality.equalityscheduler.equalityScheduler"
    · Supervisors' storm.yaml files must contain the following:
         supervisor.scheduler.meta:
              group-id: "1"
       where group-id is a serial id number, starting from 1.

How to run the topologies:

In order to run the topologies 6 main arguments and 1 optional are needed:

    1. Main class name (SequentialTopology,DiamondTopology or TwoSourcesTopology)
    2. Topology name for Storm
    3. Experiment mode (opt or equal or default)   
    4. Path to configuration files
    5. Number of experiment (matches the configuration files)
    6. Number of tuples
    7. Number of supervisors (not necessary at default mode)

The first two modes run with the Equality scheduler whereas the default runs with the default Storm scheduler. To enforce the latter we comment out the "storm.scheduler: "com.equality.equalityscheduler.equalityScheduler" line and restart Nimbus.


Examples:

    · /storm/bin/storm jar sequentialDAG-1.0.jar SequentialTopology topologyName opt /ConfigurationExamples/ 1 100000 10
    · /storm/bin/storm jar diamondDAG-1.0.jar DiamondTopology topologyName equal /ConfigurationExamples/ 2 100000 10
    · /storm/bin/storm jar twoSourcesDAG-1.0.jar TwoSourcesTopology topologyName default /ConfigurationExamples/ 3 100000 . 
 
