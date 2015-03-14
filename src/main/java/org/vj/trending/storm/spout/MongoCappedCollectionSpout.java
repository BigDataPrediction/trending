/**
 * 
 */
package org.vj.trending.storm.spout;

/**
 * @author Vijay
 *
 */
import com.mongodb.DBObject;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.List;

public class MongoCappedCollectionSpout extends MongoSpoutBase implements Serializable
{

    private static final long serialVersionUID = 1221725440580018348L;

    static Logger LOG = Logger.getLogger(MongoCappedCollectionSpout.class);

    public MongoCappedCollectionSpout(String url, String collectionName)
    {
        super(url, null, new String[] { collectionName }, null, null);
    }

    @Override
    protected void processNextTuple()
    {
        DBObject object = this.queue.poll();
        if (object != null)
        {
            List<Object> tuples = this.mapper.map(object);
            // Emit the tuple collection
            this.collector.emit(tuples);
        }
    }
}