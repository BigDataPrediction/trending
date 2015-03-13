/**
 * 
 */
package org.vj.tending.storm.topology;

import org.apache.log4j.Logger;
import org.vj.trending.storm.bolt.IntermediateRankingsBolt;
import org.vj.trending.storm.bolt.ListExtractorBolt;
import org.vj.trending.storm.bolt.MongoWriterBolt;
import org.vj.trending.storm.bolt.RollingCountBolt;
import org.vj.trending.storm.bolt.TotalRankingsBolt;
import org.vj.trending.storm.spout.MongoCappedCollectionSpout;
import org.vj.trending.storm.utils.ConfigUtility;
import org.vj.trending.storm.utils.RealtimeUtil;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * @author Vijay
 *
 */
public class TrendingTopology
{
    public static final String spoutId = "eventReader";
    public static final String listExtractorId="ListIdReader";
    public static final String counterId = "counter";
    public static final String intermediateRankerId = "IntermediateRanker";
    public static final String totalRankerId = "FinalRanker";
    public static final String totalsavetomongoId="TrendingInjester";
    public static final int TOP_N = 5;
    public static final int DEFAULT_RUNTIME_IN_SECONDS = 3600;
    public static final int runtimeInSeconds = DEFAULT_RUNTIME_IN_SECONDS;
    
    public static final String url = "mongodb://localhost:27017/cp";
    public static final String collectionName = "user";
    static Logger LOG = Logger.getLogger(TrendingTopology.class);
            
    public static void main(String[] args) throws Exception {
//        if (args.length != 2) {
//            throw new IllegalArgumentException("Need two arguments: topology name and config file path");
//        }
        LOG.info("############################### Topology Started ..........................");
        String topologyName = "TrendingNow";
        String configFilePath = "/home/vijay/trending/trending.properties";
        Config conf = RealtimeUtil.buildStormConfig(configFilePath);
        // Set the spout to read from MongoDB collections
        MongoCappedCollectionSpout mongoSpout=new MongoCappedCollectionSpout(ConfigUtility.getString(conf, "mongo.input.url"), ConfigUtility.getString(conf, "mongo.collection.input"));
        
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(spoutId, mongoSpout);
        
        builder.setBolt(listExtractorId,new ListExtractorBolt()).shuffleGrouping(spoutId);
        builder.setBolt(counterId, new RollingCountBolt(runtimeInSeconds, 10), 2).fieldsGrouping(listExtractorId, new Fields("lid"));
        builder.setBolt(intermediateRankerId, new IntermediateRankingsBolt(TOP_N), 2).fieldsGrouping(counterId,
                new Fields("obj"));
        builder.setBolt(totalRankerId, new TotalRankingsBolt(TOP_N)).globalGrouping(intermediateRankerId);
        builder.setBolt(totalsavetomongoId,new MongoWriterBolt(ConfigUtility.getString(conf, "mongo.output.url"), ConfigUtility.getString(conf, "mongo.db.output"), ConfigUtility.getString(conf, "mongo.colloection.output"))).shuffleGrouping(totalRankerId);
        RealtimeUtil.submitStormTopology(topologyName, conf,  builder);
    }
        

}
