package com.temp.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

public class PrinterBolt extends BaseBasicBolt{
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        System.out.print(tuple.getSourceStreamId());
        System.out.print(" avg:");
        System.out.println(tuple.getValue(0));
        
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }
}
