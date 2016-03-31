import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

// This class is a simple way to centralize all of the properties and constants used to perform the timeseries reductions.

// Most of the map-reduce for the time series is "parameratized" (data driven) in terms of controlling input and output.
// These parameters are set via .xml conf files, resources, or command line options. ultimately the parameters end up as
// a relatively small number of properties in the programs configuration.

// The properties specify the input columns in the csv, what columns will be output, what is mapped and reduced, whether
// to download data from yahoo finance, etc. 

public class TSConfProperties {
	// Arguments on the command line
	public static final String TIMESERIES_ARGS_CSV = "-csvcols";
	public static final String TIMESERIES_ARGS_OUTCOLS = "-outcols";
	public static final String TIMESERIES_ARGS_PERIOD = "-period";
	public static final String TIMESERIES_ARGS_HDR = "-outhdr";
	public static final String TIMESERIES_ARGS_FILTER = "-filter";
	public static final String TIMESERIES_ARGS_SORT = "-sort";
	public static final String TIMESERIES_ARGS_SPLIT = "-split";
	public static final String TIMESERIES_ARGS_LCLSRC = "-lclsrc";
	public static final String TIMESERIES_ARGS_LCLDEST = "-lcldest";
	public static final String TIMESERIES_ARGS_DOWNLOAD = "-d";
	public static final String TIMESERIES_ARGS_YHOO_DOWNLOAD = "-yahooxml";
	
	// Properties in configuration
	public static final String TIMESERIES_LCLSRC  = "timeseries.lclsrc";
	public static final String TIMESERIES_LCLSRC_DEFAULT = "";
	public static final String TIMESERIES_LCLDEST  = "timeseries.lcldest";
	public static final String TIMESERIES_LCLDEST_DEFAULT = "";
			
	public static final String TIMESERIES_FNAME  = "timeseries.fname";
	public static final String TIMESERIES_DEFAULT_FNAME  = "out";
	
	// Columns in input file.
	public static final String TIMESERIES_CSVCOLS = "timeseries.csvcols";
	public static final String TIMESERIES_DEFAULT_CSVCOLS = "d,o,h,l,c,v,a";
	
	// Columns in output file.
	public static final String TIMESERIES_OUTCOLS = "timeseries.outcols";
	public static final String TIMESERIES_DEFAULT_OUTCOLS = "d,o,h,l,c,v,a";
	
	// Period of time to reduce on.
	public static final String TIMESERIES_PERIOD = "timeseries.period";
	public static final String TIMESERIES_DEFAULT_PERIOD = "m";
	
	// Output the column header in the result file.
	public static final String TIMESERIES_OUTHDR = "timeseries.outhdr";
	public static final String TIMESERIES_HDRS = "t";
	public static final String TIMESERIES_DEFAULT_HDRS = TIMESERIES_HDRS;
	
	// Sort the result by ascending or descending date
	public static final String TIMESERIES_SORT = "timeseries.sort";
	public static final String TIMESERIES_SORT_ASC = "asc";
	public static final String TIMESERIES_SORT_DESC = "desc";
	public static final String TIMESERIES_DEFAULT_SORT = TIMESERIES_SORT_DESC;
	
	// Filter records that meet criteria.
	public static final String TIMESERIES_FILTER = "timeseries.filter";
	public static final String TIMESERIES_DEFAULT_FILTER = "o,ho,lo,h,lh,l,hl,hv,lv,a,ha,la";
	
	// File path to yahoo finance xml configuration file.
	public static final String TIMESERIES_YHOO_DOWNLOAD = "timeseries.yahoo_download_xml";
	/* Whether to download Yahoo finance data, even if a path is specified. Data will be downloaded
	 only if timeseries.download is set to "t" and a xml file/path is specified in timeseries.yahoo_download
	 default behaviour is to use the xml file if specified and download fresh data.
	*/
	public static final String TIMESERIES_DOWNLOAD = "timeseries.download";
	public static final String TIMESERIES_DEFAULT_DOWNLOAD = "t";
	
	/* 
	 *  
	 *  The record's columns to be output are specified by the timeseries.filter property.
	 *  Individual records to filter for:
	 *  
	 *   filter - filter records for:				column in output (outcols)
	 *   o  -  Opening value for period.				o - Open
	 *   ho -  highest open in period.					o - Open
	 *   lo -  Lowest open in period.					o - Open
	 *   h  -  Highest high in period					h - high
	 *   lh -  Lowest high in period.                   h - High
	 *   l  -  Lowest low in period						l - Low
	 *   hl -  Highest Low in period.                   l - Low
	 *   c  -  closing value for period.				c - Close
	 *   hc -  Highest close in period.				    c - Close
	 *   lc -  Lowest close in period.				    c - Close
	 *   hv -  highest volume in period.				v - Volume
	 *   lv -  Lowest volume in period.					v - Volume
	 *   a  -  adj close value for period				a - adj close
	 *   ha -  Highest adj close in period.				a - Adj Close
	 *   la -  Lowest adj close in period.				a - Adj Close
	 */
	
	public static final String TIMESERIES_REC_OPEN = "o";
    public static final String TIMESERIES_REC_HOPEN = "ho";
    public static final String TIMESERIES_REC_LOPEN = "lo";
	public static final String TIMESERIES_REC_HIGH = "h";
	public static final String TIMESERIES_REC_LHIGH = "lh";
	public static final String TIMESERIES_REC_LOW = "l";
	public static final String TIMESERIES_REC_HLOW = "hl";
	public static final String TIMESERIES_REC_CLOSE = "c";
	public static final String TIMESERIES_REC_HCLOSE = "hc";
	public static final String TIMESERIES_REC_LCLOSE = "lc";
	public static final String TIMESERIES_REC_HVOL = "hv";
	public static final String TIMESERIES_REC_LVOL = "lv";
	public static final String TIMESERIES_REC_ADJCLOSE = "a";
	public static final String TIMESERIES_REC_HADJCLOSE = "ha";
	public static final String TIMESERIES_REC_LADJCLOSE = "la";
	
	
	// Split amd/or combine filtered records into their own file. When combined duplicate records (by date) are removed.
	public static final String TIMESERIES_SPLIT_RECS = "timeseries.split";
	public static final String TIMESERIES_SPLIT = "s";
	public static final String TIMESERIES_COMBINE = "c";
	public static final String TIMESERIES_REGRESS = "r";
	public static final String TIMESERIES_ALLRECS = "a";
	// default is to output everything.
	public static final String TIMESERIES_DEFAULT_SPLIT = TIMESERIES_ALLRECS;
	
	public static final String TIMESERIES_SPLIT_FNAME = "dup";
	public static final String TIMESERIES_COMBINE_FNAME = "com";
	public static final String TIMESERIES_REGRESS_FNAME = "reg";
	public static final String TIMESERIES_ALL_FNAME = "all";
	
	
	
	
	// Variables available to programs for configuration settings. Read from conf file and defaults (if any) assigned.
	// Error checked and cached into local variable for map reduce methods quick use.
	
	public String strPeriod = "";
	public String strCsvCols = "";
	public String strOutCols = "";
	public String strOutHdr = "";
	public String strFilter = "";
	public String strSort = "";
	public String strSplit = "";
	public String strOutName = "";
	
	// configuration file.
	public Configuration conf = null;
	
	public TSConfProperties()
	{
	
	}
	
	public TSConfProperties(Configuration config)
	{
		setConf(config);
	}
	
	public void AddResourceConf(String strResFile)
	{
		try {
		conf.addResource(new File(strResFile).toURI().toURL());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public String getProp(String name)
	{
		// Get data and clean it up in case of errors or missing.
		String strValue = conf.get(name);
		if (strValue == null)
			strValue = "";
		else
		{
	        strValue = strValue.trim();
	        // All non file parameters are case insensitive.
	        if (!(name.equals(TIMESERIES_FNAME) || 
	        	  name.equals(TIMESERIES_LCLSRC) ||
	        	  name.equals(TIMESERIES_LCLDEST) || 
	        	  name.equals(TIMESERIES_YHOO_DOWNLOAD)))
	          strValue = strValue.toLowerCase();
		}
	    return strValue;
	}
	
	public void setProperty(String name, String strValue)
	{
		if (name.equals(TIMESERIES_FNAME))
		   strValue = getFileName(strValue);
		
		conf.set(name, strValue);
	
		// update any local program configuration variables.
		switch (name)
		{
		case TIMESERIES_FNAME:
			strOutName = getProperty(name);
		break;
		
		case TIMESERIES_PERIOD:
			strPeriod = getProperty(name);
		break;
		
		case TIMESERIES_CSVCOLS:
			strCsvCols = getProperty(name);
		break;
		
		case TIMESERIES_OUTCOLS:
			strOutCols = getProperty(name);
		break;
		
		case TIMESERIES_OUTHDR:
			strOutHdr = getProperty(name);
		break;
		
		case TIMESERIES_FILTER:
			strFilter = getProperty(name);
		break;
		
		case TIMESERIES_SORT:
			strSort = getProperty(name);
		break;
		
		case TIMESERIES_SPLIT_RECS:
			strSplit = getProperty(name);
		break;
		}
	}
	
	public String getDefaultPropValue(String name)
	{
		String strValue = "";
		
		switch (name)
		{
		case TIMESERIES_FNAME:
			strValue = TIMESERIES_DEFAULT_FNAME;
		break;
		
		case TIMESERIES_PERIOD:
			strValue = TIMESERIES_DEFAULT_PERIOD;
		break;
		
		case TIMESERIES_CSVCOLS:
			strValue = TIMESERIES_DEFAULT_CSVCOLS;
		break;
		
		case TIMESERIES_OUTCOLS:
			strValue = getProperty(TIMESERIES_CSVCOLS);
		break;
		
		case TIMESERIES_OUTHDR:
			strValue = TIMESERIES_DEFAULT_HDRS;
		break;
		
		case TIMESERIES_FILTER:
			strValue = TIMESERIES_DEFAULT_FILTER;
		break;
		
		case TIMESERIES_SORT:
			strValue = TIMESERIES_DEFAULT_SORT;
		break;
		
		case TIMESERIES_SPLIT_RECS:
			strValue = TIMESERIES_DEFAULT_SPLIT;
		break;
		
		case TIMESERIES_DOWNLOAD:
			strValue = TIMESERIES_DEFAULT_DOWNLOAD;
		break;
		
		default:
			strValue = "";
		break;
		}
		
		return strValue;
	}
	
	public String getProperty(String name)
	{
		String strValue = getProp(name);
		if (strValue.equals(""))
		  strValue = getDefaultPropValue(name);
		
	    return strValue;
	}
	
	// Read in all the configuration properties.
	public void getConfProperties()
	{
		// Get properties and set defaults.
		
		strOutName = getProperty(TIMESERIES_FNAME);
		
		strPeriod = getProperty(TIMESERIES_PERIOD);
 		
		strCsvCols = getProperty(TIMESERIES_CSVCOLS);
		
		strOutCols = getProperty(TIMESERIES_OUTCOLS);
		
		strOutHdr = getProperty(TIMESERIES_OUTHDR);
		
		strFilter = getProperty(TIMESERIES_FILTER);
		
		strSort = getProperty(TIMESERIES_SORT);
		
		strSplit = getProperty(TIMESERIES_SPLIT_RECS);
		
	}
	
	public void setConf(Configuration config)
	{
		conf = config;
		
		getConfProperties();
	}
	
	
	public String getOutputFileName(String strFileId) {
		// Set file name adding any id if specified.
		String sname = strOutName;
		if (strPeriod.equals("") == false)
	       sname += "-" + strPeriod;
	    if (strFileId.equals("") == false)
	    	sname+= "-" + strFileId;
	    return sname;
	}
	
	// Get file name to use based upon input file and conf properties.
	public String getFileName(String filePath) {
		if (filePath == null)
			return "";
		
        String[] filePathDir = filePath.split(FileSystemPath.separator);
        
        String fileName = filePathDir[filePathDir.length - 1];
        
        // Does file contain postfix file extention (i.e. csv);
        if (fileName.contains("."))
          fileName = fileName.substring(0, fileName.lastIndexOf("."));
          
        return fileName;
    }
	
	
	// Command line parser for supported configuration properties.
	// returns list of parameters not in the set of configuration properties.
	public List<String> parseArgs(String [] args)
	{
		
		List<String> io_args = new ArrayList<String>();
		for (int i = 0; i < args.length; ++i) {
			try {
				
				if (TIMESERIES_ARGS_CSV.equals(args[i])) {
					// Default same as Adj_Close
					//available o - Open, h - High, l - Low, c- Close, v - Volume, 
					//          j- Adj_Close
					setProperty(TIMESERIES_CSVCOLS, args[++i]);

				}else if (TIMESERIES_ARGS_OUTCOLS.equals(args[i])) {
					/* Default column or 'i' is the same as csvcol
					   available o - Open, h - High, l - Low, c- Close, v - Volume, 
					          j- Adj_Close, i - input csvcol (default) or a - all
					 Can be list separated by comma. for example o,h,l,c,v,j
					 Date (and Time) are always output as first field 
					 */
					setProperty(TIMESERIES_OUTCOLS, args[++i]);

				} else if (TIMESERIES_ARGS_PERIOD.equals(args[i])) {
                    // y = Year, q = Quarter, m = Month, w = Week, d = Day  
					setProperty(TIMESERIES_PERIOD, args[++i]);

				}else if (TIMESERIES_ARGS_HDR.equals(args[i])) {
                    /* true or false. Whether to output the header as first line of output.
                     */
					setProperty(TIMESERIES_OUTHDR, args[++i]);

				}else if (TIMESERIES_ARGS_FILTER.equals(args[i])) {
                    /* What records to filter
                     */
					setProperty(TIMESERIES_FILTER, args[++i]);

				}  else if (TIMESERIES_ARGS_SORT.equals(args[i])) {
                    /* sorting
                     */
					setProperty(TIMESERIES_SORT, args[++i]);

				}  else if (TIMESERIES_ARGS_SPLIT.equals(args[i])) {
                    /* sorting
                     */
					setProperty(TIMESERIES_SPLIT_RECS, args[++i]);

				}    else if (TIMESERIES_ARGS_LCLSRC.equals(args[i])) {
                    /* sorting
                     */
					setProperty(TIMESERIES_LCLSRC, args[++i]);

				}    else if (TIMESERIES_ARGS_LCLDEST.equals(args[i])) {
                    /* sorting
                     */
					setProperty(TIMESERIES_LCLDEST, args[++i]);

				}   else if (TIMESERIES_ARGS_YHOO_DOWNLOAD.equals(args[i])) {
                    /* sorting
                     */
					setProperty(TIMESERIES_YHOO_DOWNLOAD, args[++i]);

				} else if (TIMESERIES_ARGS_DOWNLOAD.equals(args[i])) {
                    /* Default is to always download yahoo data if a xml file is specified. This can
                     * be used to suppress this by setting to something other than "t";
                     */
					setProperty(TIMESERIES_DOWNLOAD, args[++i]); 
				} else { 
					
					io_args.add(args[i]);

				}
			} catch (NumberFormatException e) {
				System.out.println("ERROR: Integer expected instead of " + args[i]);
				e.printStackTrace();
			} catch (ArrayIndexOutOfBoundsException e) {
				System.out.println("ERROR: Required parameter missing from " + args[i - 1]);
				e.printStackTrace();
			}
		}
		
		if (io_args.size() >= 1)
		{
			setProperty(TIMESERIES_FNAME, io_args.get(0));
		}
		
		return io_args;
	}
}
