import java.io.IOException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TSDataMapper
  extends Mapper<LongWritable, Text, TSCompositeKey, TSData> {
	
	TSConfProperties tsConf = new TSConfProperties();
	static enum Parse_Counters { LINE_DATA, BAD_DATA, GOOD_DATA };
	static int icnt = 0;
	
	private final TSCompositeKey key = new TSCompositeKey();
	private TSData tsdata = new TSData();
	
	
	private static final Log logger = LogFactory.getLog(TSDataMapper.class);
    
	
	@Override
	public void setup(Context context) throws IllegalArgumentException, IOException {
		icnt = 0;
		// Setup mapper configuration
		tsConf.setConf(context.getConfiguration());
		
        logger.debug("setup has been called " + String.format("Period=%s", tsConf.strPeriod));
        
        
    }
  
  @Override
  public void map(LongWritable inkey, Text value, Context cntxt)
      throws IOException, InterruptedException {
	  
	  cntxt.getCounter(Parse_Counters.LINE_DATA).increment(1);
	  
	  if (logger.isDebugEnabled())
	    logger.debug("Map called" + cntxt.getJobName() + " - " + value);
	  
		String line = value.toString();
		String strPeriodKey = "";
		Integer iCols = 0;
		
		if (StringUtils.containsIgnoreCase(line, YahooData.TIMESERIES_DATE_HDR) ||
				StringUtils.containsIgnoreCase(line, YahooData.TIMESERIES_TS_HDR)	)
			{
			/* Must have date or timestamp column.
			   Ignore header because the header (if any) will be output from setup for the reducer. 
		       As an alternative a group key could be generated to act as a placeholder for a header 
		       because it could be set so that it would always be the first record in output no matter
		       how many reducers you have.
			 */
				if (logger.isDebugEnabled())
					logger.debug(cntxt.getJobName() + ": encountered header: " + value);
				return;
			}
		
		YahooData ydata = YahooData.parse_template(line, tsConf.strCsvCols);
		if (ydata == null) 
		{
			// If not the header record then try and reconfigure using a standard template. 
			// Good for testing various formats without reconfiguring templates.
			
			if (!(StringUtils.containsIgnoreCase(line, YahooData.TIMESERIES_DATE_HDR) ||
				StringUtils.containsIgnoreCase(line, YahooData.TIMESERIES_TS_HDR)	))
			ydata = YahooData.parse(line);
		}
		
		if (ydata != null) {
			
			 cntxt.getCounter(Parse_Counters.GOOD_DATA).increment(1);
			 
			// The number of columns should match the first successfully read record pattern or 
			// should be rejected.
			if (iCols == 0) {
				iCols = ydata.icsvcols;
			}
			else {
			
				if (iCols != ydata.icsvcols)
				{
					if (logger.isDebugEnabled())
					  logger.debug(cntxt.getJobName() + ": Map bad record columns Expected cols=" + 
							       String.format("%d",iCols) + String.format(" csv cols=%d",ydata.icsvcols));
					System.err.println("Possible bad input line: " + value);
					cntxt.setStatus("Detected bad input line, wrong number of columns.");
					cntxt.getCounter(Parse_Counters.BAD_DATA).increment(1);
					return;
				}
			}
			
			strPeriodKey = ydata.generatePeriodKey(tsConf.strPeriod);
			
			/* set the key
			 The group key is set depending upon the period and whether exchange and/or stock symbol data 
			 exists in the data.
			*/ 
			key.set(ydata.exchange, ydata.symbol, strPeriodKey, ydata.date, ydata.strInFields);	
			
			//Setup the data to be reduced. Get it all as most cases it will be used.
			tsdata.lDateTime = ydata.date;
			tsdata.fOpen = ydata.open;	
			tsdata.fHigh = ydata.high;
			tsdata.fLow = ydata.low;
			tsdata.fClose = ydata.close;
			tsdata.lVolume = ydata.volume;
			tsdata.fAdj_Close = ydata.adj_close;
		
			// now that its parsed, we send it through the shuffle for sort,
			cntxt.write(key, tsdata);

		} else {
			// was it the header?
			if (StringUtils.containsIgnoreCase(line, YahooData.TIMESERIES_DATE_HDR) ||
				StringUtils.containsIgnoreCase(line, YahooData.TIMESERIES_TS_HDR)	)
			{
			/* Must have date or timestamp column.
			   Ignore header because the header (if any) will be output from setup for the reducer. 
		       As an alternative a group key could be generated to act as a placeholder for a header 
		       because it could be set so that it would always be the first record in output no matter
		       how many reducers you have.
			 */
				if (logger.isDebugEnabled())
					logger.debug(cntxt.getJobName() + ": encountered header: " + value);
			}
			else
			{
			if (logger.isDebugEnabled())
			  logger.debug(cntxt.getJobName() + ": Map bad parse record: " + value);
			System.err.println("Possible bad input line: " + value);
			cntxt.setStatus("Detected bad input line.");
			cntxt.getCounter(Parse_Counters.BAD_DATA).increment(1);
			}

		}
		
	}
  }
