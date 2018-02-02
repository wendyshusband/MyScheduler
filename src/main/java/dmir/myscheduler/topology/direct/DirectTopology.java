package dmir.myscheduler.topology.direct;

import dmir.myscheduler.topology.testRAS.RASOutput;
import dmir.myscheduler.topology.testRAS.RASRandomSentenceSpout;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

import java.util.HashMap;

public class DirectTopology {

    public static void main(String[] args) throws Exception{
        int numOfParallel;
        TopologyBuilder builder;
        StormTopology stormTopology;
        Config config;

        HashMap<String, String> component2Slot;

        numOfParallel = 4;

        builder = new TopologyBuilder();

        String desSpout = "my_spout";
        String desBolt = "my_bolt";

        builder.setSpout(desSpout, new RASRandomSentenceSpout(), numOfParallel);

        builder.setBolt(desBolt, new RASOutput(), numOfParallel)
                .shuffleGrouping(desSpout);

        config = new Config();
        config.setNumWorkers(numOfParallel);
        config.setMaxSpoutPending(65536);
        config.put(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT, 40000);
        config.put(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT, 40000);

        //LocalCluster cluster = new LocalCluster();
        //config.setDebug(true);
        //cluster.submitTopology("test-monitor", config, builder.createTopology());

        component2Slot = new HashMap<>();

        component2Slot.put(desSpout, "dmir5:6700;dmir5:6701;dmir6:6705;dmir4:6702");
        component2Slot.put(desBolt, "dmir7:6700;dmir8:6703;dmir5:6700");

        config.put("assigned_flag", "1");
        config.put("design_map", component2Slot);

        StormSubmitter.submitTopology("test", config, builder.createTopology());
    }
}
