/**
 * 
 */
package org.vj.trending.storm.bolt;

/**
 * @author Vijay
 *
 */
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.vj.trending.storm.tools.Rankable;
import org.vj.trending.storm.tools.Rankings;
import org.vj.trending.storm.tools.TupleHelpers;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoURI;

public class MongoWriterBolt extends BaseRichBolt
{
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    private final String collectionName;
    private final String dbName;
    private final String url;
    private Mongo mongo;
    private DB db;
    private DBCollection collection;
    private static final Logger LOG = Logger.getLogger(MongoWriterBolt.class);

    public MongoWriterBolt(String url, String dbName, String collectionName)
    {
        this.url = url;
        this.dbName = dbName;
        this.collectionName = collectionName;
    }

    Logger getLogger()
    {
        return LOG;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector)
    {
        MongoURI uri = new MongoURI(url);
        try
        {
            mongo = new Mongo(uri);
        }
        catch (UnknownHostException e)
        {
            e.printStackTrace();
        }
        // Get the db the user wants
        db = mongo.getDB(dbName == null ? uri.getDatabase() : dbName);
        collection = db.getCollection(this.collectionName);
    }

    @Override
    public void execute(Tuple tuple)
    {
        boolean istick = TupleHelpers.isTickTuple(tuple);
        if (!TupleHelpers.isTickTuple(tuple))
        {
            String s = tuple.getSourceComponent();
            List v = tuple.getValues();
            Map<String, Long> ranks = new HashMap<String, Long>();
            Rankings rankings = (Rankings) tuple.getValueByField("rankings");
            boolean save = false;
            for (int i = 0; i < rankings.getRankings().size(); i++)
            {
                Rankable rankable = rankings.getRankings().get(i);
                if (rankable.getObject().getClass().getName().equalsIgnoreCase("java.lang.String"))
                {
                    ranks.put((String) rankable.getObject(), rankable.getCount());
                    save = true;
                }

            }
            if (save)
            {
                String docId = "globalRanking";
                final BasicDBObject query = new BasicDBObject("_id", docId);

                BasicDBObject object = new BasicDBObject();
                // sort ranking
                TreeMap<String, Long> sorted = sort(ranks);

                object.put("ranking", sorted);
                object.put("_id", docId);
                collection.update(query, object, true, false);
            }
        }
    }

    private TreeMap<String, Long> sort(Map map)
    {
        ValueComparator bvc = new ValueComparator(map);
        TreeMap<String, Long> sorted_map = new TreeMap<String, Long>(bvc);
        sorted_map.putAll(map);
        return sorted_map;

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
    {
    }
}