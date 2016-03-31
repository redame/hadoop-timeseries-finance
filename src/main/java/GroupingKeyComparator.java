import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 
 * GroupingKeyComparator. 
 * 
 * This class is used to group the data values by the group key before passing to the reducer(s).
 * it is used during Hadoop's shuffle phase. The Group Partitioner was used to guarantee that all of records
 * with the same group key goes to the same reducer. This is used to insure that output from a map is grouped
 * into a single input to the reducer.
 * 
 */
public class GroupingKeyComparator extends WritableComparator {

	protected GroupingKeyComparator() {
		super(TSCompositeKey.class, true);
	}

	// Default descending sort
		static int srt = -1;
		
		@Override
		public void setConf(Configuration conf)
		{
			super.setConf(conf);
			
			TSConfProperties tsConf = new TSConfProperties(conf);
			if (tsConf.strSort.equals(TSConfProperties.TIMESERIES_SORT_ASC))
			  srt = 1;
			else
		      srt = -1;
		
		}
		
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable o1, WritableComparable o2) {

		TSCompositeKey tsK1 = (TSCompositeKey) o1;
		TSCompositeKey tsK2 = (TSCompositeKey) o2;

		return (srt * tsK1.compareGroupKeyTo(tsK2));

	}
}
