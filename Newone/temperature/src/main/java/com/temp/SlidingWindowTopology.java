package com.temp;

import java.util.List;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
//import com.temp.bolt.JoinBolt;
import com.temp.bolt.PrinterBolt;
import com.temp.bolt.SlidingWindowSumBolt;
import com.temp.spout.RandomIntegerSpout; 
import com.temp.spout.RandomIntegerSpout2;
import com.temp.spout.RandomIntegerSpout3;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.topology.base.BaseWindowedBolt.Count;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A sample topology that demonstrates the usage of {@link org.apache.storm.topology.IWindowedBolt}
 * to calculate sliding window sum.
 */
public class SlidingWindowTopology {

    private static final Logger LOG = LoggerFactory.getLogger(SlidingWindowTopology.class);

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("integer", new RandomIntegerSpout(), 1);
        builder.setSpout("integer2", new RandomIntegerSpout2(), 1);
        builder.setSpout("integer3", new RandomIntegerSpout3(), 1);

        /*

        JoinBolt jBolt = new JoinBolt("integer", "value")
                                            .join("integer2", "value", "integer")
                                            .join("integer3", "value", "integer2");

        //builder.setBolt("joiner", jBolt, 1).fieldsGrouping("integer", "stream1", new Fields("value"))
        //                                    .fieldsGrouping("integer2", "stream2", new Fields("value"))
        //                                    .fieldsGrouping("integer3", "stream3", new Fields("value"));
                                            

        */

        builder.setBolt("slidingsum", new SlidingWindowSumBolt().withWindow(Count.of(30), Count.of(10)), 1)
               .shuffleGrouping("integer","stream1").shuffleGrouping("integer2","stream2").shuffleGrouping("integer3","stream3");
        //builder.setBolt("slidingsum", new SlidingWindowSumBolt().withWindow(Count.of(30), Count.of(10)), 1).shuffleGrouping("joiner");
        
        builder.setBolt("tumblingavg", new TumblingWindowAvgBolt().withTumblingWindow(Count.of(3)), 1)
               .shuffleGrouping("slidingsum");
        builder.setBolt("printer", new PrinterBolt(), 1).shuffleGrouping("tumblingavg");
        Config conf = new Config();
        conf.setDebug(true);
        String topoName = "test";
        if (args != null && args.length > 0) {
            topoName = args[0];
        }
        conf.setNumWorkers(1);
        StormSubmitter.submitTopologyWithProgressBar(topoName, conf, builder.createTopology());
    }

    /*
     * Computes tumbling window average
     */
    private static class TumblingWindowAvgBolt extends BaseWindowedBolt {
        private OutputCollector collector;

        @Override
        public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(TupleWindow inputWindow) {
            int sum = 0;
            List<Tuple> tuplesInWindow = inputWindow.get();
            LOG.debug("Events in current window: " + tuplesInWindow.size());
            if (tuplesInWindow.size() > 0) {
                /*
                 * Since this is a tumbling window calculation,
                 * we use all the tuples in the window to compute the avg.
                 */
                for (Tuple tuple : tuplesInWindow) {
                    sum += (int) tuple.getValue(0);
                }
                collector.emit(new Values(sum / tuplesInWindow.size()));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("avg"));
        }
    }
}
