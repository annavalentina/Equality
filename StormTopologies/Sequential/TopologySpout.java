import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.*;

public class TopologySpout extends BaseRichSpout {

    TreeMap<Double, Integer> fractions;
    String mode;
    Integer numberOfTuples;

    public TopologySpout(TreeMap<Double, Integer> fractions, String mode, Integer numberOfTuples) {
        this.fractions = fractions;
        this.mode = mode;
        this.numberOfTuples = numberOfTuples;
    }

    public TopologySpout(String mode, Integer numberOfTuples) {//For default storm
        this.mode = mode;
        this.numberOfTuples = numberOfTuples;
    }

    SpoutOutputCollector spoutOutputCollector;
    private Integer index = 0;
    List<Integer> tasksIds;
    TopologyContext topologyContext;

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        this.topologyContext = topologyContext;
        this.tasksIds = topologyContext.getComponentTasks("MultiplierBolt1");//Get the tasks of the next node
        Collections.sort(this.tasksIds);
    }

    public void nextTuple() {
        if (index < numberOfTuples) {
            Random random = new Random();
            double value = random.nextDouble() * 100.0;
            if (mode.equals("opt") || mode.equals("equal")) {
                this.spoutOutputCollector.emitDirect(this.tasksIds.get(fractions.higherEntry(value).getValue()), new Values(index, index), index);//Direct sending of tuples to tasks
                index++;
            } else {//For default storm
                this.spoutOutputCollector.emit(new Values(index, index), index);
                index++;
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("id", "num"));
    }
}
