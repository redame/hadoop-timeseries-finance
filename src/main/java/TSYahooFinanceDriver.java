import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
/*
 * Summary Description:
 * This application is an example of using map reduce against a set of time series data. It illustrates how to use various features of the hadoop
 * framework for command line processing, configuration, partitioning, use of comparator keys, use of the file system, etc. Yahoo finance is
 * used for the timeseries data because it is readily available and also integrates retrieving data from external sources as input into the
 * hadoop framework.
 * 
 * In the simplest case the program takes a yahoo finance historical data download .csv and reduces the daily date into more discrete
 * time periods and also will select for various attributes. So the daily date will be reduced to weekly, monthly, quarterly and yearly data.
 * Also a series can be created with thing like the lowest low, highest high, lowest close, etc. 
 * The number of supported options allows varying the number of input/output fields, sorting, the source of input and output files, etc.
 * 
 * Further Details:
 *  This driver is used to map reduce a set of timeseries input files. The time series data is map reduced for 
 *  all periods and the results are organized hierarchically in the output directory by input csv file, type of 
 *  reduction and time period. 
 *  
 *  Command line options and properties are centralized in the TSConfProperties class. 
 *  The various options and properties are used to explore and test out the capabilities of the map-reduce framework, the
 *  hadoop file system, and the configuration capabilities for data driven behavior.
 *  
 * Optionally data is copied from a "local" file system and output back in a csv format to a destination directory.
 * The word "local" is put in quotation marks because it is really left to the file system api to handle the transfers and
 * the files can be any type. Although local files are the most logical use.
 * This is controlled by the lclsrc and lcldest arguments and properties.  
 * 
 * Updated data can be specified to be downloaded from Yahoo Finance via the timeseries.download and 
 * timeseries.yahoo_download_xml properties. The timeseries.download specifies whether to do a download and
 * the timeseries.yahoo_download_xml property specifies the location of the XML File that configures the download. 
 * An example of this file format can be seen in tracking.xml as well as indexes.xml. This specifies to download
 * the historical data for the Dow Jones index and the 30 stocks that make up the index. The dow30.xml can be modified 
 * to download any data available in Yahoo Finance.
 * 
 * If a local source directory is specified then it will override any destination specified in the xml file. This way
 * any downloaded data will be fed directly into the map reduce process.
 * 
 *  The command line arguments, properties and xml files offer a number of ways to configure and control the 
 *  files fed into and out of the map-reduce process. Nevertheless at heart it remains a fairly simple process in that
 *  a number of time series files in csv format are fed into one map and two reductions. The first reduction reduces the
 *  records by summarizing data over larger time periods. The second filters the data out by various criteria applied to
 *  the fields. One does a period reduction and the other picks out various records based upon characteristics of the data. 
 *  
 *  Usage:
 *  TSYahooFinanceDriver [optional hadoop arguments] [-yahooxml <xml download file>] [-d t|f] [-csvcols d,o,h,l,c,v,a ] [-period y|q|m|w|d ] [-outcols d,o,h,l,c,v,a ]"
	            [-outhdr t|f] [-sort asc|desc ] [-filter o,ho,lo,h,lh,l,hl,c,hv,lv,a,ha,la ] [-lclsrc <local source directory>] [-lcldest <local csv directory> ]
		        <hadoop input path> <hadoop output path>
 *  Example :TSYahooFinanceDriver -csvcols d,o,h,l,c,v,a -outcols d,o,h,l,c,v,a -filter o,ho,lo,h,lh,l,hl,c,hv,lv,a,ha,la 
 *  -lcldest file:///csvfiles file:///input file:///output
 *  
 *  This was tested against hadoop versions 2.5 thru 2.7.1. Testing was performed on Windows and Centos 6.5 and ubuntu 14.04
 */

public class TSYahooFinanceDriver extends Configured implements Tool {
	
	// private static final Log logger = LogFactory.getLog(TSYahooFinanceDriver.class);
	private static boolean CONTINUE_ON_FAILURE = true; // One may or may not want to keep going on a failure of a map reduce and check the logs later.
	public static final String TIMESERIES_REC_NAME  = "rec";
	public static final String TIMESERIES_PERIOD_NAME  = "period";
	
	
	// Job configuration object.
	static Configuration conf = new Configuration();
	static TSConfProperties tsConf = new TSConfProperties(conf);
	
	public void ConfLocal()
	{
		conf.set("fs.defaultFS", FileSystemPath.FS_LOCAL);
		conf.set("mapreduce.jobtracker.address", "local");
		conf.set("mapreduce.job.tracker", "local");
		conf.set("mapreduce.framework.name", "local");
	    //conf.setInt("mapreduce.task.io.sort.mb", 1000);
		
	}
	
	public void ConfPseudoDist()
	{
		conf.set("fs.defaultFS", "hdfs://localhost:9000");
		conf.set("mapred.job.tracker", "localhost:9001");
		//conf.set("fs.default.name", "hdfs://localhost:9000");
		conf.set("dfs.replication", "1");
	}
	
	public void ConfnCluster()
	{
		// Change myhost to a real host.
			conf.set("mapred.job.tracker", "<myhost>:9001");
			conf.set("fs.default.name", "hdfs://<myhost>:9000");
			conf.set("dfs.replication", "1");
		
	}
	
	
	// Copy the output files from hadoop stored in srcDir to dstDir. Relative paths and the directory structure of srcDir is maintained.
	// A .csv extension is added to the output.
	public boolean CopyOutFiles(String srcDir, String dstDir)  throws FileNotFoundException, IOException {
		
		  boolean bFileCopied = true;
		  
		  try{
		  FileSystemPath fsp = new FileSystemPath(srcDir, tsConf.conf);
		  FileSystemPath fsp2 = new FileSystemPath(dstDir,tsConf.conf);
		  List<String> lstFilePath = fsp.getAllRelativeFilePath();
	
		  for (int i = 0; i < lstFilePath.size() && bFileCopied; i++)
		  {
			String inFile = lstFilePath.get(i);
			if (inFile.contains("-r-00") == false)
			  continue;
			String outFile = inFile.substring(0, inFile.lastIndexOf("-r-00")) + ".csv";
			Path dstPath = fsp2.fs.makeQualified(new Path(fsp2.pathName.getName() + FileSystemPath.separator + outFile));
			Path srcPath = fsp.fs.makeQualified(new Path(fsp.pathName.getName() + FileSystemPath.separator + inFile));
		    bFileCopied = FileUtil.copy(fsp.fs, srcPath, fsp2.fs, dstPath, false, true, tsConf.conf);
		  }
		  
          }
		  catch (IOException e)
			{ 
			  e.printStackTrace();
			  bFileCopied = false;	
			}; 
    
			return bFileCopied;
	}
	
	
	public void AddClassConf()  throws IOException {
		// configuration can be set in resource file.
		tsConf.AddResourceConf("resources" + FileSystemPath.separator + "tsconfig.xml");
		//tsConf.AddResourceConf("tsconfig.xml");
		
	}
	
	
	static int printUsage() {
		System.out.println("TSAllDriver [optional hadoop arguments] [-yahooxml <xml download file>] [-d t|f] [-csvcols d,o,h,l,c,v,a ] [-period y|q|m|w|d ] [-outcols d,o,h,l,c,v,a ]" + 
	                       " [-outhdr t|f] [-sort asc|desc ] [-filter o,ho,lo,h,lh,l,hl,c,hv,lv,a,ha,la ] [-lclsrc <local source directory>] [-lcldest <local csv directory> ]" +
				           " <hadoop input path> <hadoop output path>");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
		}
	
	
	
	public int runPeriodRecReducer(FileSystem fsInput, String strInFile, FileSystemPath fspOutput)  throws Exception 
	{
		
        Job job = Job.getInstance(tsConf.conf, "Yahoo Timeseries m/r Period Record Reduction on file - " + tsConf.strOutName);
		
	    job.setOutputKeyClass(TSCompositeKey.class);

	    job.setOutputValueClass(TSData.class);

	    job.setMapperClass(TSDataMapper.class);
	    
	    job.setReducerClass(TSPeriodRecReducer.class);

	    job.setInputFormatClass(TextInputFormat.class);
	    
	    LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
	    
	    //job.setPartitionerClass(GroupPartitioner.class);
	
		job.setGroupingComparatorClass(GroupingKeyComparator.class);
		
		job.setSortComparatorClass(OutputKeyComparator.class);

	    job.setJarByClass(TSYahooFinanceDriver.class);
	
	    Path p1 = fsInput.makeQualified(new Path(strInFile));
		FileInputFormat.setInputPaths(job, p1);
		
		String strOutDir = fspOutput.getName() + FileSystemPath.separator + tsConf.strOutName + FileSystemPath.separator + TIMESERIES_REC_NAME + FileSystemPath.separator + tsConf.strPeriod;
		Path p2 = fspOutput.fs.makeQualified(new Path(strOutDir));
	    FileOutputFormat.setOutputPath(job, p2);
        
	    System.out.println("Executing Job " + job.getJobName() + " Input= " + strInFile + " Output =" + strOutDir);
		
	    boolean success = job.waitForCompletion(true);
	    return(success ? 0 : 1);	
		
	}
	
	public int runPeriodReducer(FileSystem fsInput, String strInFile, FileSystemPath fspOutput)  throws Exception 
	{
		
        Job job = Job.getInstance(tsConf.conf, "Yahoo Timeseries m/r Period Reduction on file - " + tsConf.strOutName);
        
		
	    job.setOutputKeyClass(TSCompositeKey.class);

	    job.setOutputValueClass(TSData.class);

	    job.setMapperClass(TSDataMapper.class);
	    
	    job.setReducerClass(TSPeriodReducer.class);

	    job.setInputFormatClass(TextInputFormat.class);
	    
	    LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
	    
	    //job.setPartitionerClass(GroupPartitioner.class);
	
		job.setGroupingComparatorClass(GroupingKeyComparator.class);
		
		job.setSortComparatorClass(OutputKeyComparator.class);

	    job.setJarByClass(TSYahooFinanceDriver.class);
	
		Path p1 = fsInput.makeQualified(new Path(strInFile));
		FileInputFormat.setInputPaths(job, p1);
		
		String strOutDir = fspOutput.getName() + FileSystemPath.separator + tsConf.strOutName + FileSystemPath.separator + tsConf.strPeriod;
		Path p2 = fspOutput.fs.makeQualified(new Path(strOutDir));
		FileOutputFormat.setOutputPath(job, p2);
		
		System.out.println("Executing Job " + job.getJobName() + " Input= " + strInFile + " Output =" + strOutDir);
		
	    boolean success = job.waitForCompletion(true);
	    return(success ? 0 : 1);	
		
	}
	
	@Override
	public int run(String[] args) throws Exception {
		System.out.println("\n\nTimeSeries\n");
		
		// add resource xml file
		this.AddClassConf();
		
		ConfLocal();
		//ConfPseudoDist();
	     
		// read command line arguments into configuration.
		List<String> io_args = tsConf.parseArgs(args);
		
		// Make sure there are exactly 2 parameters left. One for the file and the second for the base output directory.
		if (io_args.size() != 2) {
			System.out.println("ERROR: Wrong number of parameters: " + io_args.size() + " instead of 2.");
			return printUsage();
		}
		
        String [] strPeriods = {YahooData.TIMESERIES_PERIOD_YEAR, 
        		                YahooData.TIMESERIES_PERIOD_QUARTER, 
        		                YahooData.TIMESERIES_PERIOD_MONTH,
        		                YahooData.TIMESERIES_PERIOD_WEEK,
        		                YahooData.TIMESERIES_PERIOD_DAY};
		
		int ret = 0;
		
		
		String strInput = io_args.get(0);
		String strOutput = io_args.get(1);
		
		// Get all of the input files to process. There may be one or many. 
		// The input may be a directory or file name.
		
		// are the files to be Downloaded from Yahoo Finance.
		String strLCLSrc = tsConf.getProp(TSConfProperties.TIMESERIES_LCLSRC);
		String strYahooDownload = tsConf.getProperty(TSConfProperties.TIMESERIES_YHOO_DOWNLOAD);
		
		FileSystemPath fspSrc = new FileSystemPath();
		
		if (tsConf.getProperty(TSConfProperties.TIMESERIES_DOWNLOAD).equals("t"))
		{
			/* Data that is downloaded from YahooFinance can possibly go directly into the input directory or to 
			   a local source directory and then copied over. If a local source directory is provided then this will 
			   override any setting in the yahoo xml download file.
			*/
			if (strYahooDownload.equals("") == false)
			{
			YahooConnection YC = new YahooConnection(conf);
			ret = YC.GetXMLConfiguration("resources" + FileSystemPath.separator + strYahooDownload);
			if (ret == 0)
			  {
				if (strLCLSrc.equals("") == false)
				{
				  YC.setOutputDir(strLCLSrc);
				}
				else
				{
				strLCLSrc = YC.getOpOutputDir();
				}
			    
				fspSrc.SetFSPath(strLCLSrc, tsConf.conf);
				
				// empty directory prior to download.
				fspSrc.emptyPath();
				
				// Download data from Yahoo Finance
				ret = YC.DownloadData();	
				if (ret != 0)
				  System.out.println("ERROR: error downloading yahoo finance data ret= " + String.valueOf(ret));
			  }
			else
			  System.out.println("ERROR: Cant get data from yahoo xml file=" + strYahooDownload + " ret= " + String.valueOf(ret));
			}
		if (ret != 0)
			return ret;
		}
		
		FileSystemPath fspInput = new FileSystemPath(strInput, tsConf.conf);
		
		if (strLCLSrc.equals("") == false)
		{
			// Copying files from another path into the input directory.
			if (fspSrc.fs == null)
			  fspSrc.SetFSPath(strLCLSrc, tsConf.conf);
			
			// Delete any data in the input directory as all the data is coming from the src directory.
			// Make sure the download directory is not the same as the input.
			if (fspSrc.pathName.getName().equals(fspInput.pathName.getName()) == false)
			  fspInput.emptyPath();	
			
			// copy the files from the Src to the Input directory
		    fspSrc.CopyFilesTo(fspInput, false, true);	
		}
		
		// Get a list of all of the files in the input directory.
	    List<String> lstFilePath = fspInput.getAllFilePath();
	    
	    // Delete any old data
	    FileSystemPath fspOutput = new FileSystemPath(strOutput, tsConf.conf);
	    fspOutput.emptyPath();
	    
	    for (int i = 0; i < lstFilePath.size()  && ret == 0; i++)
		  {
			String inFile = lstFilePath.get(i);
		    tsConf.setProperty(TSConfProperties.TIMESERIES_FNAME, inFile);
	    	for (int j = 0; j < strPeriods.length && ret == 0; j++)
		    {
		    tsConf.setProperty(TSConfProperties.TIMESERIES_PERIOD, strPeriods[j]);
		
		    // Run map/reducer on files from input file system and sends results to output file system 
            ret = runPeriodReducer(fspInput.fs, inFile, fspOutput);
            
            if (CONTINUE_ON_FAILURE)
              ret = 0;
        		
            if (ret == 0)
        	  ret = runPeriodRecReducer(fspInput.fs, inFile, fspOutput);
            
            if (CONTINUE_ON_FAILURE)
                ret = 0;
		    }
		
	    }
	    
	    // sending results from map/reduce output to a different (local?) file system
	    
	    String strLCLDest = tsConf.getProp(TSConfProperties.TIMESERIES_LCLDEST);
	    if (strLCLDest.equals("") == false)
		{
	    CopyOutFiles(strOutput, strLCLDest);
	    }	    
	    
	    return ret;
	}



public static void main(String[] args)  throws Exception {
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    int res = ToolRunner.run(new TSYahooFinanceDriver(), otherArgs);
    
	System.exit(res);
}

}

