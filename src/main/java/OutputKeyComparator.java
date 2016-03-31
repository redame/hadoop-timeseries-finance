import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.conf.Configuration;

/**
 * OutputKeyComparator
 * 
 * We want to partition and group the data to the reducer(s) by the group id. However, we
 * also want to make use of hadoop for sorting the data to the reducer by the composite key value.
 * 
 * The use of the OutputKeyComparator is to sort the data for each call to the reducer by the second 
 * part of the composite key. Her the "value" will either be the date/timestamp or the value we are
 * reducing. Which one is irrelevant to this routine as this detail has been set up in the mapper.
 * 
 */
public class OutputKeyComparator extends WritableComparator {

	protected OutputKeyComparator() {
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
	public int compare(WritableComparable w1, WritableComparable w2) {

		TSCompositeKey tsK1 = (TSCompositeKey) w1;
		TSCompositeKey tsK2 = (TSCompositeKey) w2;

		return (srt * tsK1.compareTo(tsK2));

	}

	
}
