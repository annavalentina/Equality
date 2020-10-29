import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;


public class MultiplierBolt1 extends BaseRichBolt {
    TreeMap<Double, Integer> fractions1;
    TreeMap<Double, Integer> fractions2;
    double selectivity;
    String mode;

    public MultiplierBolt1(TreeMap<Double, Integer> fractions1, TreeMap<Double, Integer> fractions2, double selectivity, String mode) {
        this.fractions1 = fractions1;
        this.fractions2 = fractions2;
        this.selectivity = selectivity;
        this.mode = mode;
    }

    public MultiplierBolt1(double selectivity, String mode) {//For default storm
        this.selectivity = selectivity;
        this.mode = mode;
    }

    List<Integer> tasksIds1;
    List<Integer> tasksIds2;
    TopologyContext topologyContext;
    OutputCollector _collector;

    public void prepare(Map stormConf, TopologyContext topologyContext, OutputCollector collector) {
        this.topologyContext = topologyContext;
        this.tasksIds1 = topologyContext.getComponentTasks("AddingBolt1");//Get the tasks of the next node
        this.tasksIds2 = topologyContext.getComponentTasks("AddingBolt2");//Get the tasks of the next node
        _collector = collector;
        Collections.sort(this.tasksIds1);
        Collections.sort(this.tasksIds2);
    }

    public void execute(Tuple tuple) {
        Integer id = tuple.getInteger(0);
        Integer num = tuple.getInteger(1);
        Random random = new Random();
        double value = random.nextDouble() * 100.0;

        if (mode.equals("equal") || mode.equals("opt")) {
            if (selectivity == 1) {//Emit 1 tuple for each input tuple
                _collector.emitDirect(this.tasksIds1.get(fractions1.higherEntry(value).getValue()), tuple, new Values(id, num * 2));
                _collector.emitDirect(this.tasksIds2.get(fractions2.higherEntry(value).getValue()), tuple, new Values(id, num * 2));
            } else if (selectivity == 2) {//Emit 2 tuples for each input tuple
                _collector.emitDirect(this.tasksIds1.get(fractions1.higherEntry(value).getValue()), tuple, new Values(id, num * 2));
                _collector.emitDirect(this.tasksIds1.get(fractions1.higherEntry(value).getValue()), tuple, new Values(id, num * 2));
                _collector.emitDirect(this.tasksIds2.get(fractions2.higherEntry(value).getValue()), tuple, new Values(id, num * 2));
                _collector.emitDirect(this.tasksIds2.get(fractions2.higherEntry(value).getValue()), tuple, new Values(id, num * 2));
            } else if (selectivity < 1) {//Emit 1 tuple with probability=selectivity for each input tuple
                Random r = new Random();
                Float prob = r.nextFloat() * (1 - 0) + 0;
                if (prob <= selectivity) {
                    _collector.emitDirect(this.tasksIds1.get(fractions1.higherEntry(value).getValue()), tuple, new Values(id, num * 2));
                    _collector.emitDirect(this.tasksIds2.get(fractions2.higherEntry(value).getValue()), tuple, new Values(id, num * 2));
                }
            } else if (selectivity > 1) {//Emit 1 tuple and 1 more with probability=selectivity for each input tuple
                _collector.emitDirect(this.tasksIds1.get(fractions1.higherEntry(value).getValue()), tuple, new Values(id, num * 2));
                _collector.emitDirect(this.tasksIds2.get(fractions2.higherEntry(value).getValue()), tuple, new Values(id, num * 2));
                Random r = new Random();
                Float prob = r.nextFloat() * (1 - 0) + 0;
                if (prob <= (selectivity - 1)) {
                    _collector.emitDirect(this.tasksIds1.get(fractions1.higherEntry(value).getValue()), tuple, new Values(id, num * 2));
                    _collector.emitDirect(this.tasksIds2.get(fractions2.higherEntry(value).getValue()), tuple, new Values(id, num * 2));
                }
            }
            _collector.ack(tuple);
        } else {//For default storm
            if (selectivity == 1) {//Emit 1 tuple for each input tuple
                _collector.emit(tuple, new Values(id, num * 2));
            } else if (selectivity == 2) {//Emit 2 tuples for each input tuple
                _collector.emit(tuple, new Values(id, num * 2));
                _collector.emit(tuple, new Values(id, num * 2));
            } else if (selectivity < 1) {//Emit 1 tuple with probability=selectivity for each input tuple
                Random r = new Random();
                Float prob = r.nextFloat() * (1 - 0) + 0;
                if (prob <= selectivity) {
                    _collector.emit(tuple, new Values(id, num * 2));
                }
            } else if (selectivity > 1) {//Emit 1 tuple and 1 more with probability=selectivity for each input tuple
                _collector.emit(tuple, new Values(id, num * 2));
                Random r = new Random();
                Float prob = r.nextFloat() * (1 - 0) + 0;
                if (prob <= (selectivity - 1)) {
                    _collector.emit(tuple, new Values(id, num * 2));
                }
            }
            _collector.ack(tuple);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("id", "num"));
    }
}

