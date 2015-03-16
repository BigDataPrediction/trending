/**
 * 
 */
package org.vj.trending.storm.bolt;

/**
 * @author Vijay
 *
 */

import java.util.Map;

import org.apache.log4j.Logger;
import org.vj.trending.storm.tools.TupleHelpers;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.mongodb.DBObject;

public class ListExtractorBolt extends BaseRichBolt
{
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    OutputCollector outputCollector;
    private static final Logger LOG = Logger.getLogger(MongoWriterBolt.class);

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector)
    {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple)
    {
        if (TupleHelpers.isTickTuple(tuple))
        {
            outputCollector.emit(new Values(tuple));

        }
        else
        {
            DBObject object = null;
            try
            {
                object = (DBObject) tuple.getValue(0);
            }
            catch (Exception e)
            {
            }
            String listId = object.get("lid").toString();
            outputCollector.emit(tuple, new Values(listId));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("lid"));
    }
}