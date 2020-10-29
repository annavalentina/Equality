import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;


public class TwoSourcesTopology {


    public static void main(String[] args) throws Exception {

        //Use treemaps to store fractions
        List<TreeMap<Double, Integer>> treemaps = new ArrayList<TreeMap<Double, Integer>>();
        treemaps.add(new TreeMap<Double, Integer>());
        treemaps.add(new TreeMap<Double, Integer>());
        treemaps.add(new TreeMap<Double, Integer>());
        treemaps.add(new TreeMap<Double, Integer>());
        int numberOfBolts = 4;
        String path = args[2] + "twoSources/";//Path to files
        String mode = args[1];
        String numberOfExperiment = args[3];
        Integer numberOfTuples = Integer.parseInt(args[4]);

        String selectivitiesFilename = path + "selectivities" + numberOfExperiment + ".txt";
        List<Double> selectivities = new ArrayList<Double>();
        BufferedReader br2 = new BufferedReader(new FileReader(selectivitiesFilename));
        String line2;
        while ((line2 = br2.readLine()) != null) {
            selectivities.add(Double.parseDouble(line2));
        }

        if (mode.equals("opt")) {//Optimized-Equality
            int ex = Integer.parseInt(args[5]);
            String fractionsFilename = path + "fractions" + numberOfExperiment + ".txt";
            BufferedReader br = new BufferedReader(new FileReader(fractionsFilename));
            String line;
            int treeId = 0;
            while ((line = br.readLine()) != null) {
                int id = 0;
                double sum = 0.0;
                for (String a : line.split(", ")) {
                    if (Float.parseFloat(a) != 0) {
                        sum += (int) (Float.parseFloat(a) * 100);
                        treemaps.get(treeId).put(sum, id);
                    }
                    id += 1;
                }
                treeId += 1;
            }
            TopologyBuilder builder = new TopologyBuilder();
            builder.setSpout("TopologySpout1", new TopologySpout1(treemaps.get(0), mode, numberOfTuples)).addConfiguration("group-id", "1");
            builder.setSpout("TopologySpout2", new TopologySpout2(treemaps.get(1), mode, numberOfTuples)).addConfiguration("group-id", "2");
            builder.setBolt("MultiplierBolt1", new MultiplierBolt(treemaps.get(2), selectivities.get(0), mode), ex).directGrouping("TopologySpout1").setNumTasks(ex);
            builder.setBolt("MultiplierBolt2", new MultiplierBolt(treemaps.get(2), selectivities.get(1), mode), ex).directGrouping("TopologySpout2").setNumTasks(ex);
            builder.setBolt("AddingBolt1", new AddingBolt1(treemaps.get(3), selectivities.get(2), mode), ex).directGrouping("MultiplierBolt1").directGrouping("MultiplierBolt2").setNumTasks(ex);
            builder.setBolt("AddingBolt2", new AddingBolt2(), ex).directGrouping("AddingBolt1").setNumTasks(ex);

            Config config = new Config();
            config.setMessageTimeoutSecs(300);
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        } else if (mode.equals("equal")) {//Equal distribution
            int ex = Integer.parseInt(args[5]);
            //Divide tasks
            for (int j = 0; j < numberOfBolts; j++) {
                double sum = 0.0;
                double divided = 100.0 / ex;
                for (int i = 0; i < ex; i++) {
                    sum += divided;
                    treemaps.get(j).put(sum, i);
                }
            }
            TopologyBuilder builder = new TopologyBuilder();
            builder.setSpout("TopologySpout1", new TopologySpout1(treemaps.get(0), mode, numberOfTuples)).addConfiguration("group-id", "1");
            builder.setSpout("TopologySpout2", new TopologySpout2(treemaps.get(1), mode, numberOfTuples)).addConfiguration("group-id", "2");
            builder.setBolt("MultiplierBolt1", new MultiplierBolt(treemaps.get(2), selectivities.get(0), mode), ex).directGrouping("TopologySpout1").setNumTasks(ex);
            builder.setBolt("MultiplierBolt2", new MultiplierBolt(treemaps.get(2), selectivities.get(1), mode), ex).directGrouping("TopologySpout2").setNumTasks(ex);
            builder.setBolt("AddingBolt1", new AddingBolt1(treemaps.get(3), selectivities.get(2), mode), ex).directGrouping("MultiplierBolt1").directGrouping("MultiplierBolt2").setNumTasks(ex);
            builder.setBolt("AddingBolt2", new AddingBolt2(), ex).directGrouping("AddingBolt1").setNumTasks(ex);
            Config config = new Config();
            config.setMessageTimeoutSecs(300);
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        } else if (mode.equals("default")) {//Default storm
            TopologyBuilder builder = new TopologyBuilder();
            builder.setSpout("TopologySpout1", new TopologySpout1(mode, numberOfTuples));
            builder.setSpout("TopologySpout2", new TopologySpout2(mode, numberOfTuples));
            builder.setBolt("MultiplierBolt1", new MultiplierBolt(selectivities.get(0), mode)).shuffleGrouping("TopologySpout1");
            builder.setBolt("MultiplierBolt2", new MultiplierBolt(selectivities.get(1), mode)).shuffleGrouping("TopologySpout2");
            builder.setBolt("AddingBolt1", new AddingBolt1(selectivities.get(2), mode)).shuffleGrouping("MultiplierBolt1").shuffleGrouping("MultiplierBolt2");
            builder.setBolt("AddingBolt2", new AddingBolt2()).shuffleGrouping("AddingBolt1");
            Config config = new Config();
            config.setMessageTimeoutSecs(300);
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        }
    }
}
