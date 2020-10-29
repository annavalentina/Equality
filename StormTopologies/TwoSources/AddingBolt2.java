import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

public class AddingBolt2 extends BaseRichBolt {

    public AddingBolt2() {
    }

    OutputCollector _collector;

    public void prepare(Map stormConf, TopologyContext topologyContext, OutputCollector collector) {
        _collector = collector;
    }

    public void execute(Tuple tuple) {
        Integer id = tuple.getInteger(0);
        Integer num = tuple.getInteger(1);
        Random r = new Random();
        Integer randomInteger = r.nextInt((100 - 0) + 1) + 0;
        _collector.ack(tuple);
        _collector.emit(tuple, new Values(id, num + randomInteger));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("id", "num"));
    }
}
