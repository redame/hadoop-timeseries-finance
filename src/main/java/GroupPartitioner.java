import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

/**
 * PartitionerKey
 * 
 * This class partitions the data output from the map phase 
 * before it is sent through the shuffle phase.
 * 
 *  The getPartition() method determines how we group the data into the reducer. 
 *  In this case partitioning is done using the GroupKey portion of the key. * half of the key, the Text group (key.getGroup().hashcode()).
 * 
 * In the case of financial data, this allows us to partition the data by the
 * symbol name, year, quarter, month, etc. 
 * 
 * This also implements the configurable interface to show how to get access to configuration options.
 * 
 */
public class GroupPartitioner extends Partitioner<TSCompositeKey, TSData> implements Configurable {

	static Configuration config = null;
	
	@Override
	public int getPartition(TSCompositeKey key, TSData value, int numPartitions) {
		return Math.abs(key.getGroupKey().hashCode() * 127) % numPartitions;
	}

	public void setConf(Configuration conf)
	{
		config = conf;
	}
	
	public Configuration getConf()
	{
		return config;
	}
}
