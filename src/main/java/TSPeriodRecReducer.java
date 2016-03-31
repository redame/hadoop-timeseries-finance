
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/*
 *  The goal is to take Yahoo (or similar) data and map and apply various record filters
 *  like lowest volume for a period or highest close within a period. This class can apply
 *  a number of record filters simultaneously and both consolidate and split them off into
 *  their own files. This is all controlled via the configuration. 
 *  
 *  These kinds of reductions allow for viewing time series data on different "fractal" levels 
 *  in terms of time. This shows patterns not normally visible at only the 
 *  daily and intra-day levels.
 *  
 *  This reducer (TSPeriodRecReducer) reduces all data to a set of records 
 *  that match one or more of the filter requirements. Each record that matches
 *  a filter is selected in entirety. Hence, while the TSPeriodReducer reduces all dates
 *  to the closing (last) date for the period, the TSPeriodRecReducer outputs the specific date
 *  (and other data columns) of the record within the period that matches the filter. 
 *  Essentially, this reducer finds the dates that certain events occurred as specified by the filter(s)..
 *  
 *  One record is produced for each value requested/filtered. Each record has a unique date within the
 *  period. 
 *  
 *  The behaviour is data driven by the configuration properties.
 *  
 *  The reducer uses the timeseries.split property (TSConfProperties) to determine whether to combine these
 *  records to individual dates (removing duplicates), split them out by filter into their individual file, or
 *  regressing the data back into a date, value pair stored in the adj close field. This results in creating both 
 *  individual and combined data sets.
 *
 *  The timeseries.outcols parameter is used to specify the output format with things like the date, 
 *  high, low, close, volume, and adj close.
 *  
 *  The data is output in a comma delimited value. The group key is not written
 *  unless specified in the timeseries.outcols configuration parameter.
 *   
 *   The data is ordered by the timeseries.sort property.
 *  
 *   A nice feature of the map-reduce framework is that it does not take a lot of code to 
 *   carry out fairly complex operations.
 *   
 */

    //class TSRecData implements java.lang.Comparable<TSRecData>, java.util.Comparator<TSRecData> {
	class TSRecData implements java.lang.Comparable<TSRecData> {
		
	public static int srt = 1;
	public String strFilterName = "";
	public int iFilterId = 0;
	public TSData tsdata = new TSData();
	public boolean bUniqueDate = true;
	public boolean bUniqueData = true;
	
	public void copy(TSRecData tsrec)
	{
		strFilterName = tsrec.strFilterName;
	    iFilterId = tsrec.iFilterId;
	    bUniqueDate = tsrec.bUniqueDate;
	    bUniqueData = tsrec.bUniqueData;
	    tsdata = new TSData();
	    tsdata.copy(tsrec.tsdata);
	}
	
	@Override
	public int compareTo(TSRecData ts) {
		
		return srt * tsdata.compareTo(ts.tsdata);
	}
}

	class TSFilteredRecs {
		
		public TSRecData [] recs = new TSRecData[0];  
		
		public static TSFilteredRecs Copy(TSFilteredRecs tsRecs)
		{
			TSFilteredRecs tsr = new TSFilteredRecs();	
            tsr.recs = new TSRecData[tsRecs.recs.length];
			
			for (int i = 0; i < tsr.recs.length; i++ )
			{
				TSRecData ts = new TSRecData();
				ts.copy(tsRecs.recs[i]);
			    tsr.recs[i] = ts;
			}
			
			return tsr;
		}
		
		void reset()
		{
		  for (int i = 0; i < recs.length; i++ )
		  {
			recs[i].bUniqueData = true;
			recs[i].bUniqueDate = true;
		  }
		}
		
		public void Setup(String strFilterList, String strSort)
		{
			if (strSort.equals(TSConfProperties.TIMESERIES_SORT_ASC))
				  TSRecData.srt = 1;
			else
				  TSRecData.srt = -1;
			
			String [] strFilters = strFilterList.split(",");
			
			recs = new TSRecData[strFilters.length];
			
			for (int i = 0; i < recs.length; i++ )
			{
				TSRecData ts = new TSRecData();
				recs[i] = ts;
			    ts.strFilterName = strFilters[i];
			    ts.iFilterId = i;
			}
		}
		
		
		public void sort()
		{
		Arrays.sort(recs);
		}
		
		public void markDupeDate()
		{
		
		// mark duplicate dates.
		
		for(int i = 0; i < recs.length; i++)
		  {
		  TSRecData ts1 = recs[i];
		  if (ts1.bUniqueDate)
		    {
		    for (int j = i+1; j < recs.length; j++)
		      {
			  TSRecData ts2 = recs[j];
			  if (ts2.bUniqueDate)
			    {
			    if (ts2.tsdata.lDateTime == ts1.tsdata.lDateTime)
			       ts2.bUniqueDate = false;
			    }
		      }
		    }
		  }
		}
		
		public void regressData()
		{
			for(int i = 0; i < recs.length; i++)
			  {
			  TSRecData ts = recs[i];
			  switch (ts.strFilterName)
				{
				case TSConfProperties.TIMESERIES_REC_HOPEN:
				case TSConfProperties.TIMESERIES_REC_LOPEN:
				case TSConfProperties.TIMESERIES_REC_OPEN:
					ts.tsdata.fAdj_Close = ts.tsdata.fOpen;
					break;			
					
				case TSConfProperties.TIMESERIES_REC_HIGH:
				case TSConfProperties.TIMESERIES_REC_LHIGH:
					ts.tsdata.fAdj_Close = ts.tsdata.fHigh;
					break;
				
				case TSConfProperties.TIMESERIES_REC_LOW:
				case TSConfProperties.TIMESERIES_REC_HLOW:
					ts.tsdata.fAdj_Close = ts.tsdata.fLow;
					break;
					
				case TSConfProperties.TIMESERIES_REC_HCLOSE:
				case TSConfProperties.TIMESERIES_REC_LCLOSE:
				case TSConfProperties.TIMESERIES_REC_CLOSE:
					ts.tsdata.fAdj_Close = ts.tsdata.fClose;
					break;
					
				case TSConfProperties.TIMESERIES_REC_HVOL:
				case TSConfProperties.TIMESERIES_REC_LVOL:
					// do nothing.
					break;
				
				case TSConfProperties.TIMESERIES_REC_HADJCLOSE:
				case TSConfProperties.TIMESERIES_REC_LADJCLOSE:
				case TSConfProperties.TIMESERIES_REC_ADJCLOSE:
					// Do nothing
					break;
					
				default:
				break;
				}
			  }
		}
		
		public void markDupeData()
		{
		
		// mark duplicate dates,adjusted close pairs.
		
		for(int i = 0; i < recs.length; i++)
		  {
		  TSRecData ts1 = recs[i];
		  if (ts1.bUniqueData)
		    {
		    for (int j = i+1; j < recs.length; j++)
		      {
			  TSRecData ts2 = recs[j];
			  if (ts2.bUniqueData)
			    {
			    if ((ts2.tsdata.lDateTime == ts1.tsdata.lDateTime) &&
			    	(ts2.tsdata.fAdj_Close == ts1.tsdata.fAdj_Close))
			       ts2.bUniqueData = false;
			    }
		      }
		    }
		  }
		}
		
		
	}


public class TSPeriodRecReducer
  extends Reducer<TSCompositeKey, TSData, NullWritable, Text> {
	
	static enum Reduce_Counters { MAP_DATA };
	public static boolean CHK_ALL_RECS = false;
	static int icnt = 0;
	TSConfProperties tsConf = new TSConfProperties();
	TSFilteredRecs tsRecs = null; 
	
	private MultipleOutputs<NullWritable, Text> out = null;
   
	private static final Log logger = LogFactory.getLog(TSPeriodRecReducer.class);
    
	
@Override
public void setup(Context context) throws IllegalArgumentException, IOException {
	icnt = 0;
	 
	out = new MultipleOutputs<NullWritable, Text>(context);
	
	// Setup reducer configuration
	tsConf.setConf(context.getConfiguration());
	
		logger.debug("setup has been called " + 
            String.format("HDR= %s, CsvCols=%s, ColsOut=%s, Filter=%s",  
            		      tsConf.strOutHdr, tsConf.strCsvCols, tsConf.strOutCols, tsConf.strFilter));
	
	
	tsRecs = new TSFilteredRecs();
	tsRecs.Setup(tsConf.strFilter, tsConf.strSort);
	
	
	
	if (tsConf.strOutHdr.equals(TSConfProperties.TIMESERIES_HDRS))
	{
	// add headers
	Text value = new Text();
	value.set(YahooData.format_hdr(tsConf.strOutCols));
	  
	try {
		//instead of context.write(NullWritable.get(), value), write to specified file. 
		
		if (tsConf.strSplit.equals(TSConfProperties.TIMESERIES_COMBINE) ||
				tsConf.strSplit.equals(TSConfProperties.TIMESERIES_ALLRECS))
			  out.write(NullWritable.get(), value, tsConf.getOutputFileName(TSConfProperties.TIMESERIES_COMBINE_FNAME));

		// If duplicates are allowed mark file name as containing duplicate records.
		if (tsConf.strSplit.equals(TSConfProperties.TIMESERIES_SPLIT) ||
			tsConf.strSplit.equals(TSConfProperties.TIMESERIES_ALLRECS))
		  out.write(NullWritable.get(), value, tsConf.getOutputFileName(TSConfProperties.TIMESERIES_SPLIT_FNAME));
		
		if (tsConf.strSplit.equals(TSConfProperties.TIMESERIES_REGRESS) ||
		    tsConf.strSplit.equals(TSConfProperties.TIMESERIES_ALLRECS))
			 out.write(NullWritable.get(), value, tsConf.getOutputFileName(TSConfProperties.TIMESERIES_REGRESS_FNAME));
		  
		if (tsConf.strSplit.equals(TSConfProperties.TIMESERIES_SPLIT))
		{
		// write each outcol to their own separate file.
			for (int i = 0; i < tsRecs.recs.length; i++)
			{
				out.write(NullWritable.get(), value, tsConf.getOutputFileName(tsRecs.recs[i].strFilterName));
			}
		}
		
	} catch (Exception e) {
		e.printStackTrace();
	}
	
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

  @Override
  public void reduce(TSCompositeKey key, Iterable<TSData> values,
      Context context)
      throws IOException, InterruptedException {
    
	  int i = 0;
	  TSRecData ts = null;
	  TSData tsd = null;
	  Text value = new Text();
	  boolean bfirst = true;
	  YahooData ydata = new YahooData();
	  // Reset the record filters for a new set.
	  tsRecs.reset();
	  icnt++;
     
      context.getCounter(Reduce_Counters.MAP_DATA).increment(1);
	  
	  if (logger.isDebugEnabled())
	    logger.debug("TSPeriodRecReducer called" + context.getJobName() + " - " + key.getGroupKey());
	  
	  for (TSData tsdata : values) {

	  	/* Reduce the list of Data points to one containing the High, Low, Open, Close and Adjusted Close for the entire
	  	 Period covered by the GroupKey. Each call to the reducer contains all of the data for the period.
		 
		 The output should contain the first open (lowest date/timestamp), Highest High, Lowest Low, total volume and
		 last close and adjusted close (last date/timestamp) for the period. 
		 */
		  

		  
		   // Just used for testing to see if all of the records coming through with proper group keys.
		  if (CHK_ALL_RECS && ydata != null)
		  {	
		  ydata.exchange = key.getXKey();
		  ydata.symbol = key.getSKey();
		  ydata.period = key.getPeriodKey();
		  ydata.id = "all";
		  ydata.idn = icnt;
		  ydata.date = tsdata.lDateTime;
		  ydata.open = tsdata.fOpen;	
		  ydata.high = tsdata.fHigh;
		  ydata.low = tsdata.fLow;
		  ydata.close = tsdata.fClose;
		  ydata.volume = tsdata.lVolume;
		  ydata.adj_close = tsdata.fAdj_Close;
	  
          value.set(YahooData.format(ydata, tsConf.strOutCols));
	      out.write(NullWritable.get(), value, tsConf.getOutputFileName(TSConfProperties.TIMESERIES_ALL_FNAME));
		  }
		  
		  if (bfirst)
		  {
			  // first time set each filter item to first data record.
			  bfirst = false;
			  for (i = 0; i < tsRecs.recs.length; i++)
			  {
				tsd = tsRecs.recs[i].tsdata; 
				tsd.copy(tsdata);
			  }
			 
		  }
		  else 
		  {
		  // Now filter data by criteria.
		  for (i = 0; i < tsRecs.recs.length; i++)
		    {
			ts =  tsRecs.recs[i];
			tsd = ts.tsdata;
			
			switch (ts.strFilterName)
			{
			case TSConfProperties.TIMESERIES_REC_OPEN:
				 if (tsdata.lDateTime < tsd.lDateTime)
					 tsd.copy(tsdata);
				break;
				
			case TSConfProperties.TIMESERIES_REC_HOPEN:
				if (tsdata.fOpen > tsd.fOpen)
				      tsd.copy(tsdata);
				break;
				
			case TSConfProperties.TIMESERIES_REC_LOPEN:
				if (tsdata.fOpen < tsd.fOpen)
				      tsd.copy(tsdata);
				break;
				
			case TSConfProperties.TIMESERIES_REC_HIGH:
				if (tsdata.fHigh > tsd.fHigh)
				      tsd.copy(tsdata);
				break;
				
			case TSConfProperties.TIMESERIES_REC_LHIGH:
				if (tsdata.fHigh < tsd.fHigh)
				      tsd.copy(tsdata);
				break;
				
			case TSConfProperties.TIMESERIES_REC_LOW:
				if (tsdata.fLow < tsd.fLow)
				      tsd.copy(tsdata);
				break;
				
			case TSConfProperties.TIMESERIES_REC_HLOW:
				if (tsdata.fLow > tsd.fLow)
				      tsd.copy(tsdata);
				break;
				
			case TSConfProperties.TIMESERIES_REC_CLOSE:
				if (tsdata.lDateTime > tsd.lDateTime)
					tsd.copy(tsdata);
				break;
				
			case TSConfProperties.TIMESERIES_REC_HCLOSE:
				if (tsdata.fClose > tsd.fClose)
				      tsd.copy(tsdata);
				break;
				
			case TSConfProperties.TIMESERIES_REC_LCLOSE:
				if (tsdata.fClose < tsd.fClose)
				      tsd.copy(tsdata);
				break;
				
			case TSConfProperties.TIMESERIES_REC_HVOL:
				if (tsdata.lVolume > tsd.lVolume)
				      tsd.copy(tsdata);
				break;
				
			case TSConfProperties.TIMESERIES_REC_LVOL:
				// use lowest non zero volume.
				if (tsd.lVolume == 0)
				{
				if (tsdata.lVolume > tsd.lVolume)
				  tsd.copy(tsdata);	
				}
				else {
					if (tsdata.lVolume != 0 && tsdata.lVolume < tsd.lVolume)
					      tsd.copy(tsdata);
				}
				
				break;
				
			case TSConfProperties.TIMESERIES_REC_ADJCLOSE:
				if (tsdata.lDateTime > tsd.lDateTime)
					tsd.copy(tsdata);
				break;
				
			case TSConfProperties.TIMESERIES_REC_HADJCLOSE:
				if (tsdata.fAdj_Close > tsd.fAdj_Close)
				      tsd.copy(tsdata);
				break;
				
			case TSConfProperties.TIMESERIES_REC_LADJCLOSE:
				if (tsdata.fAdj_Close < tsd.fAdj_Close)
				      tsd.copy(tsdata);
				break;
				
			default:
			break;
			}
			}  
		  }
	  }
		  
		
		  if (ydata != null)
		  {		  
			  
		// sort by date
		  tsRecs.sort(); 
			  
		// mark any possible duplicate dates if the data will be combined at all.
		  if (tsConf.strSplit.equals(TSConfProperties.TIMESERIES_COMBINE)||
			  tsConf.strSplit.equals(TSConfProperties.TIMESERIES_ALLRECS))
		  {
		  tsRecs.markDupeDate();
		  }
		
		  ydata.exchange = key.getXKey();
		  ydata.symbol = key.getSKey();
		  ydata.period = key.getPeriodKey(); 
		  
		  // output the data. One line per record.
		  
		  for (i = 0; i < tsRecs.recs.length; i++)
		  {
			  ts = tsRecs.recs[i];
			  
			  // Only unique records required? If so skip record.
			  if (tsConf.strSplit.equals(TSConfProperties.TIMESERIES_COMBINE) && ts.bUniqueDate == false)
				  continue;
			 
			  ydata.id = ts.strFilterName;
			  ydata.idn = ts.iFilterId;
			  tsd =  ts.tsdata;
			  ydata.date = tsd.lDateTime;
			  ydata.open = tsd.fOpen;	
			  ydata.high = tsd.fHigh;
			  ydata.low = tsd.fLow;
			  ydata.close = tsd.fClose;
			  ydata.volume = tsd.lVolume;
			  ydata.adj_close = tsd.fAdj_Close;
		  
              value.set(YahooData.format(ydata, tsConf.strOutCols));
		  
		      //instead of context.write(NullWritable.get(), value), write to specified file. 
              
              // Output unique records (by date) across all the record filters.
              if (ts.bUniqueDate &&
            	 (tsConf.strSplit.equals(TSConfProperties.TIMESERIES_COMBINE) ||
            	  tsConf.strSplit.equals(TSConfProperties.TIMESERIES_ALLRECS)))
            	 out.write(NullWritable.get(), value, tsConf.getOutputFileName(TSConfProperties.TIMESERIES_COMBINE_FNAME));
             
           // If the records are split by a filter then output all records into another file that
           // can contain duplicate records.
              if (tsConf.strSplit.equals(TSConfProperties.TIMESERIES_SPLIT) ||
                  tsConf.strSplit.equals(TSConfProperties.TIMESERIES_ALLRECS))
                out.write(NullWritable.get(), value, tsConf.getOutputFileName(TSConfProperties.TIMESERIES_SPLIT_FNAME));
              
		      // Also write the records out to their own individual files. The file is
              // identified by the Filter Name portion.
		      if (tsConf.strSplit.equals(TSConfProperties.TIMESERIES_SPLIT) ||
	              tsConf.strSplit.equals(TSConfProperties.TIMESERIES_ALLRECS))
		        out.write(NullWritable.get(), value, tsConf.getOutputFileName(ts.strFilterName));	
		      
		    }
			        
		  if (tsConf.strSplit.equals(TSConfProperties.TIMESERIES_REGRESS) ||
		          tsConf.strSplit.equals(TSConfProperties.TIMESERIES_ALLRECS))
		      {
		    	  /* Regress times series back to date, value pairs with the data value stored in the adj close field/column.
		    	     All data except volume ends up in the Adjusted_Close value. Everything is put in one file.
		    	     for example if I want the open, highest high, lowest low, highest close, lowest close and the close for
		    	     a period as one data series then I would set the filter for these records. TIMESERIES_REGRESS will filter 
		    	     out these records and output the values in one "adj close" forming one data series in that column. This is a regression back to
		    	     a "date, value" type series. You can only do this to a wider time period. For example daily data that has a standard
		    	     input of open, high, low, close can output on a weekly level the open, high, low, and close with the exact day that 
		    	     these evens occurred as one series in "adj close".
		    	  */
			      tsRecs.regressData();
			      
			      // mark duplicate date, adj close value pairs in results
			      tsRecs.markDupeData();
		    	  
			      for (i = 0; i < tsRecs.recs.length; i++)
				  {
					  ts = tsRecs.recs[i];
					  
					  // Only unique data records required. If a duplicate date, value pair then skip record. 
					  // Also skip the record if the adj_close is zero. This will be from column where all values are zero.
					  
					  if (ts.bUniqueData && tsd.fAdj_Close > 0.0)
					  {	  
					 
					 // adj_close contains series but other data will likely be requested.
					  ydata.id = ts.strFilterName;
					  ydata.idn = ts.iFilterId;
					  tsd =  ts.tsdata;
					  ydata.date = tsd.lDateTime;
					  ydata.open = tsd.fOpen;	
					  ydata.high = tsd.fHigh;
					  ydata.low = tsd.fLow;
					  ydata.close = tsd.fClose;
					  ydata.volume = tsd.lVolume;
					  ydata.adj_close = tsd.fAdj_Close;
				  
		              value.set(YahooData.format(ydata, tsConf.strOutCols));
		    	      out.write(NullWritable.get(), value, tsConf.getOutputFileName(TSConfProperties.TIMESERIES_REGRESS_FNAME));
					  }
				  }
		          
		  }
      }
  }
}
