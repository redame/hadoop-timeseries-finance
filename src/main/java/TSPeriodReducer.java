
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/*
 *  The goal is to take Yahoo (or similar) data and map and reduce it
 *  to the scale of daily, weekly, monthly, quarterly and yearly data in
 *  terms of high, low, close, volume, and adjusted close. 
 *  
 *  These kinds of reductions allow for viewing time series data on different "fractal" levels 
 *  in terms of time. This shows patterns not normally visible at only the 
 *  daily and intra-day levels.
 *  
 *  This class reduces all of the date/timestamp records within a period specified by the group key
 *  to one consolidated record and outputs it as a comma delimited value. The group key is not written
 *  unless specified in the timeseries.outcols configuration parameter.
 *  
 *   The record outputs the highest high, lowest low, volume and opening and closing values for the specified
 *   period. The period is specified in the composite grouping key as the PeriodKey. 
 *   
 *   A header line with column names will be output during setup if specified by the timeseries.outhdr
 *   parameter. This of course assumes one reducer. Another way to handle this is to create a group key as a
 *   placeholder for a header and sorted such that it will always be first. Then output the header when the placeholder key
 *   is input. 
 *   
 *   This reducer can consolidate data that is in two forms: 
 *   1) Data that is already in the date/timestamp, high, low, close, volume, and adjusted close format (or
 *   some variation thereof).
 *   2) Data that has just the date/timestamp, value format.
 *    
 *   The outcols parameter allows tight control over what colums are output from this process. For debugging purposes
 *   the group key and period keys can be output. Also instead of a standard date a timestamp in the
 *   form of a long can also be output. 
 *   
 *   Because the inputs and outputs can be controlled map reductions can be chained together to 
 *   provide consolidations over wider periods from a day to a year. 
 *   
 *   
 */

public class TSPeriodReducer
  extends Reducer<TSCompositeKey, TSData, NullWritable, Text> {
	// Filter our records that don't span an entire period.
	static enum Reduce_Counters { MAP_DATA };
	
	static long lcnt = 0;
	TSConfProperties tsConf = new TSConfProperties();
	
	private MultipleOutputs<NullWritable, Text> out = null;
   
    private static final Log logger = LogFactory.getLog(TSPeriodReducer.class);
    
  
@Override
public void setup(Context context) throws IllegalArgumentException, IOException {
	lcnt = 0;
	
	 
	// Setup reducer configuration
	
	
	tsConf.setConf(context.getConfiguration());
	
	
	//String strn = ((FileSplit) context.getInputSplit()).getPath().toString();
	
	logger.debug("TSPeriodReducer setup has been called " + 
            String.format("HDR= %s, CsvCols=%s, ColsOut=%s",  tsConf.strOutHdr, tsConf.strCsvCols, tsConf.strOutCols));
	
	
	out = new MultipleOutputs<NullWritable, Text>(context);
	
	if (tsConf.strOutHdr.equals(TSConfProperties.TIMESERIES_HDRS))
	{
	Text value = new Text();
	value.set(YahooData.format_hdr(tsConf.strOutCols));
	  
	try {
		//context.write(outkey, value); Instead write to specific file.
		out.write(NullWritable.get(), value, tsConf.getOutputFileName(""));
		
	} catch (Exception e) {
		e.printStackTrace();
	}
	
	}
	  
    
  }
	

  @Override
  public void reduce(TSCompositeKey key, Iterable<TSData> values,
      Context context)
      throws IOException, InterruptedException {
    
	  Text value = new Text();

      YahooData ydata = null;
            
      TSData tsopen = new TSData(); 
      TSData tsclose = new TSData();
      
      context.getCounter(Reduce_Counters.MAP_DATA).increment(1);
	  
	  if (logger.isDebugEnabled())
	    logger.debug("TSPeriodReducer called" + context.getJobName() + " - " + key.getGroupKey());
	  
	  for (TSData tsdata : values) {

	  	/* Reduce the list of Data points to one containing the High, Low, Open, Close and Adjusted Close for the entire
	  	 Period covered by the GroupKey. Each call to the reducer contains all of the data for the period.
		 
		 The output should contain the first open (lowest date/timestamp), Highest High, Lowest Low, total volume and
		 last close and adjusted close (last date/timestamp) for the period. 
		 */
		  
		  if (ydata == null)
		  {
			ydata = new YahooData();
			ydata.date = tsdata.lDateTime;
			ydata.open = tsdata.fOpen;	
			if (StringUtils.contains(key.getInFields(), YahooData.TIMESERIES_COL_HIGH))
			    ydata.high = tsdata.fHigh;
			else 
				ydata.high = tsdata.fAdj_Close;
			
			if (StringUtils.contains(key.getInFields(), YahooData.TIMESERIES_COL_LOW))
				ydata.low = tsdata.fLow;
			else 
			    ydata.low = tsdata.fAdj_Close;
			
			if (StringUtils.contains(key.getInFields(), YahooData.TIMESERIES_COL_CLOSE))
				ydata.close = tsdata.fClose;
			else 
			    ydata.close = tsdata.fAdj_Close;
	
			ydata.volume = tsdata.lVolume;
			ydata.adj_close = tsdata.fAdj_Close;
			tsopen.copy(tsdata);
			tsclose.copy(tsdata);
		  }
		  else {
			  // Find earliest open.
		    if (tsdata.compareTo(tsopen) < 0)
		      tsopen.copy(tsdata);
		    
		    //Find latest close
		    if (tsdata.compareTo(tsclose) > 0)
		       tsclose.copy(tsdata);
		    
		    // sum the volume for the period
		    ydata.volume += tsdata.lVolume;
		    
		    // Find highest high for the period.
		    // If there is no high value input then use the adjusted close value
		    if (StringUtils.contains(key.getInFields(), YahooData.TIMESERIES_COL_HIGH))
		    {
		    if (tsdata.fHigh > ydata.high)
		      ydata.high = tsdata.fHigh;
		    }
		    else {
		    	// no csv input high. Adjusted close may contain tick data.
		    	if (tsdata.fAdj_Close > ydata.high)
				      ydata.high = tsdata.fAdj_Close;
			}
		    
		   
		    // Find lowest low for the period.
		    if (StringUtils.contains(key.getInFields(), YahooData.TIMESERIES_COL_LOW))
		    {
		    	if (tsdata.fLow < ydata.low)
				      ydata.low = tsdata.fLow;
		    }
		    else {
		    	// no csv input low. Adjusted close may contain tick data.
		    	if (tsdata.fAdj_Close < ydata.low)
				      ydata.low = tsdata.fAdj_Close;
			}
		    
		    }
		  }
	    
	  // combine into one data point. Use the period ending date for the date.
	  if (ydata !=  null)
	  {
		  ydata.id = "period"; // data that is input to process.
		  ydata.idn = lcnt++;
		  ydata.exchange = key.getXKey();
		  ydata.symbol = key.getSKey();
		  ydata.period = key.getPeriodKey();
		  // if there is no open value then use Adj Close (with earliest date).
		  if (StringUtils.contains(key.getInFields(), YahooData.TIMESERIES_COL_OPEN))
		    ydata.open = tsopen.fOpen;
		  else 
			 ydata.open = tsopen.fAdj_Close;
		  // if there is no close value then use Adj Close (with last date).
		  if (StringUtils.contains(key.getInFields(), YahooData.TIMESERIES_COL_CLOSE))
			ydata.close = tsclose.fClose;
		  else
		    ydata.close = tsclose.fAdj_Close;
		  ydata.adj_close = tsclose.fAdj_Close;
		  ydata.date = tsclose.lDateTime;
		  
		  // Create output line of data
		  value.set(YahooData.format(ydata, tsConf.strOutCols));  
		  
		  //context.write(outkey, value); Instead of the standard form write to specific file.
		  out.write(NullWritable.get(), value, tsConf.getOutputFileName(""));
		 
	  }
  }
  
  @Override
  public void cleanup(Context context) throws IOException {
	  
  	try {
  		if (out != null)
  		  out.close();
  	} catch (InterruptedException e) {
  		e.printStackTrace();
  	}
}
  
}