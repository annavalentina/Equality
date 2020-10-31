# Equality
The aim of this project is to optimize streaming analytics on Geo-distributed, heterogeneous and edge computing environments. The objective is to find the trade-off between latency and data-quality check on the data by finding an optimal task placement of devices and a DQ check fraction. The custom Storm scheduler (compatible with Storm 2.1.0) enforces the offline algorithms placement decisions. The Storm topologies also enforce configurations like selectivity of the operators and tuple distribution to tasks. 


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
    
Python simulations:
The python implementation starts by finding an initial task placement solution using LP and an initial one by equally distributing tasks. Then it applies two heuristic-based algorithms (latOpt and qualOpt) that optimize the best of the two previous solutions. Also it implements and optimizes a Spring Relaxation initial placement. The output is the total latency, the fraction at which DQ check will be performed and an F value that combines latency and DQ fraction as follows: 

    F=latency/(1+beta*DQfraction)
where beta is user defined and represents the importance of DQ check (when beta is 0 DQ check is ignored).

The main file is Equality.py and takes 5 arguments:
 
    1. Number of operators - tested values: 5, 10, 15
	2. Beta for F formula - tested values: 0.5, 1, 1.5, 2, 3 
	3. Alpha penalty for enabling many communication links (the bigger the value the smaller the penalty) - tested values: 5, 10
	4. Threshold for qualOpt - tested values: 075, 0.8, 0.85, 0.9, 0.95
	5. Number of iterations
The output is 3 files containing the total latency, DQ fraction and F value respectively.
 
 
