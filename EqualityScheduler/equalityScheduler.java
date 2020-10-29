package com.equality.equalityscheduler;
import java.lang.reflect.Array;
import java.util.*;
import java.util.Map.Entry;
import java.util.logging.Logger;
import com.google.common.collect.Sets;
import jdk.nashorn.internal.runtime.regexp.joni.exception.SyntaxException;
import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.scheduler.*;
import org.apache.storm.utils.Utils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class equalityScheduler implements IScheduler {


    public void prepare(Map<String, Object> conf) {
        //noop
    }


    public void schedule(Topologies topologies, Cluster cluster) {

        Collection<TopologyDetails> topologyDetails = topologies.getTopologies();//Get all topologies
        Collection<SupervisorDetails> supervisorDetails = cluster.getSupervisors().values();//Get all supervisors

	//Create HashMap <supervisor id, supervisor details>
        Map<Integer, SupervisorDetails> supervisors = new HashMap<Integer, SupervisorDetails>();
        for(SupervisorDetails s : supervisorDetails){
            Map<String, Object> metadata = (Map<String, Object>)s.getSchedulerMeta();
            if(metadata.get("group-id") != null){
                supervisors.put(Integer.parseInt((String)metadata.get("group-id")), s);
            }
        }


        for(TopologyDetails t : topologyDetails) {
            HashMap<SupervisorDetails,  List<ExecutorDetails>> executorsToSupervisors = new HashMap<SupervisorDetails,  List<ExecutorDetails>>();
            if (!cluster.needsScheduling(t)) continue;  
	    //If the topology needs scheduling
            StormTopology topology = t.getTopology();
	    Map<String, SpoutSpec> spouts = topology.get_spouts();
            Map<String, Bolt> bolts = topology.get_bolts();


            JSONParser parser = new JSONParser();
            try {
 		for (String name : spouts.keySet()) {//For each spout
                    //Assign them based on the tags
                    SpoutSpec spout = spouts.get(name);
                    JSONObject conf = (JSONObject)parser.parse(spout.get_common().get_json_conf());
                    Integer gid = Integer.parseInt((String)conf.get("group-id"));//Get group id of the spout
                    if(conf.get("group-id") != null && supervisors.get(gid) != null){
                        SupervisorDetails supervisor = supervisors.get(gid);//Get the correct supervisor
                        List<WorkerSlot> availableSlots = cluster.getAvailableSlots(supervisor);//Get slots of the supervisor
                        List<ExecutorDetails> executors = cluster.getNeedsSchedulingComponentToExecutors(t).get(name);//Get executors of the spout
                        if(!availableSlots.isEmpty() && executors != null){
                            if(executorsToSupervisors.containsKey(supervisor)){//If the HasMap already contains this supervisor add the executor to its list
                                executorsToSupervisors.get(supervisor).addAll(executors);
                            }
                            else{
                                executorsToSupervisors.put(supervisor,executors);//Otherwise create entry for this supervisor
                            }
                        }
                    }
                }
                for (String name : bolts.keySet()) {//For each bolt
                    Bolt bolt = bolts.get(name);
                    List<ExecutorDetails> executors = cluster.getNeedsSchedulingComponentToExecutors(t).get(name);//Get all the executors of the bolt
                    if(executors!=null){
                        //Sort executors by task id
                   Collections.sort(executors, new Comparator<ExecutorDetails>() {
                            public int compare(ExecutorDetails o1, ExecutorDetails o2) {
                                Integer task1 =o1.getStartTask();
                                Integer task2 = o2.getStartTask();
                                return task1.compareTo(task2);
                            }
                        });

                    for(int i=0; i<Math.min(cluster.getSupervisors().size(),executors.size()) ;i++){//Map executors to supervisor one to one
                        SupervisorDetails supervisor = supervisors.get(i+1);//Get supervisor using group-id
                        if (supervisor==null){
                            continue;
                        }
                        List<WorkerSlot> availableSlots = cluster.getAvailableSlots(supervisor);//Get the available slots of the supervisor
                        if (!availableSlots.isEmpty() ) {
                            List<ExecutorDetails> execToAssign = new ArrayList<ExecutorDetails>();
                            execToAssign.add(executors.get(i));//Get the executor to assign
                            if(executorsToSupervisors.containsKey(supervisor)){//If the HasMap already contains this supervisor add the executor to its list
                                executorsToSupervisors.get(supervisor).addAll(execToAssign);
                            }
                            else{
                                executorsToSupervisors.put(supervisor,execToAssign);//Otherwise create entry for this supervisor
                            }
                        }
                    }
                    }

                }

                for (String c:cluster.getNeedsSchedulingComponentToExecutors(t).keySet()){
                    if(c.startsWith("__")){//Ack tasks etc
                        if(supervisors.get(1) != null){
                            SupervisorDetails supervisor = supervisors.get(1);//Get the first supervisor
                            List<WorkerSlot> availableSlots = cluster.getAvailableSlots(supervisor);
                            List<ExecutorDetails> executors =cluster.getNeedsSchedulingComponentToExecutors(t).get(c);
                                if(!availableSlots.isEmpty()){
                                    if(executorsToSupervisors.containsKey(supervisor)){//If the HasMap already contains this supervisor add the executor to its list
                                        executorsToSupervisors.get(supervisor).addAll(executors);
                                    }
                                    else{
                                        executorsToSupervisors.put(supervisor,executors);//Otherwise create entry for this supervisor
                                    }
                                }
                        }
                    }
                }
                for (Map.Entry<SupervisorDetails,List<ExecutorDetails> > entry : executorsToSupervisors.entrySet()) {//Actual assignment to supervisors
                    SupervisorDetails supervisor = entry.getKey();
                    List<ExecutorDetails> executors = entry.getValue();
                    List<WorkerSlot> availableSlots = cluster.getAvailableSlots(supervisor);
                    List<ExecutorDetails> execToAssign=new ArrayList<>();
                    if(!executors.isEmpty() && !availableSlots.isEmpty()) {
                        cluster.assign(availableSlots.get(0), t.getId(), executors);
                    }
                }
            } catch (ParseException pe) {
                pe.printStackTrace();
            }
        }
    }


    public Map<String, Map<String, Double>> config() {
        return new HashMap();
    }

}
