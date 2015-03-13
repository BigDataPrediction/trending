/**
 * 
 */
package org.vj.trending.storm.bolt;

/**
 * @author Vijay
 *
 */

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import clojure.lang.IFn.LO;

import com.mongodb.DBObject;

import org.apache.log4j.Logger;
import org.vj.trending.storm.tools.TupleHelpers;

import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: edvorkin
 * Date: 5/3/13
 * Time: 1:28 PM
 * To change this template use File | Settings | File Templates.
 */
public class ListExtractorBolt extends BaseRichBolt {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    OutputCollector outputCollector;
    private static final Logger LOG = Logger.getLogger(MongoWriterBolt.class);

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector=outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
       LOG.info("1.Tuple:" + tuple);
        if (TupleHelpers.isTickTuple(tuple)) {
            LOG.info("2.Inside the Tick");
            outputCollector.emit(new Values(tuple));
            
        }  else {
       DBObject object=(DBObject) tuple.getValueByField("document");
       String listId=(String)object.get("lid");
       LOG.info("###############################Now Processing List Id:" + listId);
       outputCollector.emit(tuple, new Values(listId));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("lid"));
    }
}