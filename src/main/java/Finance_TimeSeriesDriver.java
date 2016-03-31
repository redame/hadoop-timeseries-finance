import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/*
 * Driver for timeseries data. Input for the application is in the general form.
 * 
 * -csvcols <csv input fields/columns> -period <y|q|m|w|d> -outcols <output fields> -outhdr <t|f>  -sort <asc | desc> <input csv file> <output directory>
 * for example -csvcols x,s,d,o,h,l,c,v,a -period m -outcols k,d,o,h,l,c,v,a  input/spx.csv output
 * only the input csv file and output directory is mandatory. 
 * 
 * This driver has two main paths that determine which reducer is used:
 * 1) No filter is specified. The TSPeriodReducer is used to consolidate data over the specified period. 
 * 2) A record filter is specified in which case the TSPeriodRecReducer is used and records that match the filter are
 * selected within the specified period. 
 * 
 * The parameters that this drivers accepts are turned into configuration properties as specified in the TSConfProperties class. This is the central place used
 * to control the map-reduce.
 * 
 * The regular map reduce options are also available and a configuration file (.xml) can be specified. 
 * An example is located in the resource subdirectory and is also loaded by this driver. This example
 * code is set to test various features of the latest hadoop (2.7).
 * 
 * Most options have a default value. For Yahoo data files this is input in the form of 
 * date, open, high, low, close, volume and adj close. Dates are in the form of yyyy-MM-dd. 
 * Data is in descending order.
 * 
 * This sample project makes use of custom group keys, partitioner, and output key comparator.
 * It consolidates trade data into periods of days, weeks, months, quarters and years.
 * 
 * Most of this can be applied to other time series data. The output of one consolidation can be input to
 * another one. These can be pipelined to efficiently consolidate data. Tweaks can be added for hashing partitioners
 * and also a combiner. 
 * 
 * Although this starts consolidation at daily level the same general algorithm applies to smaller time intervals of
 * hours or minutes.
 * 
 * This program has many features used to explore various features of the hadoop m/r framework. Partitioners, 
 * composite key, grouping keys, sorting, multiple file outputs, configuration, etc.
 * 
 */

public class Finance_TimeSeriesDriver extends Configured implements Tool {
	
	// private static final Log logger = LogFactory.getLog(Finance_TimeSeriesDriver.class);

	// Job configuration object.
	static Configuration conf = new Configuration();
	static TSConfProperties tsConf = new TSConfProperties(conf);
	
	public static void AddDefaultConf()
	{
	    Configuration.addDefaultResource("hdfs-default.xml");
	    Configuration.addDefaultResource("hdfs-site.xml");
	    Configuration.addDefaultResource("yarn-default.xml");
	    Configuration.addDefaultResource("yarn-site.xml");
	    Configuration.addDefaultResource("mapred-default.xml");
	    Configuration.addDefaultResource("mapred-site.xml");
	  }
	
	public void PrintConf() throws Exception 
	{
		for (Entry<String, String> entry: conf) {
	      System.out.printf("%s=%s\n", entry.getKey(), entry.getValue());	
	    }
	}
	
	public void PrintJobConf(Configuration conf) throws Exception 
	{
	    for (Entry<String, String> entry: conf) {
	      System.out.printf("%s=%s\n", entry.getKey(), entry.getValue());	
	    }
	}
	
	public void AddClassConf()  throws IOException {
		// configuration can be set in resource file.
		tsConf.AddResourceConf("resources" + FileSystemPath.separator + "tsconfig.xml");
		
	}
	
	public void ConfLocal()
	{
		conf.set("fs.defaultFS", "file:///");
		conf.set("mapreduce.jobtracker.address", "local");
		conf.set("mapreduce.framework.name", "local");
	    //conf.setInt("mapreduce.task.io.sort.mb", 1000);
		
	}
	
	public void ConfPseudoDist()
	{
		conf.set("mapred.job.tracker", "localhost:9001");
		conf.set("fs.default.name", "hdfs://localhost:9000");
		conf.set("dfs.replication", "1");
	}
	
	public void ConfnCluster()
	{
		// Change myhost to a real host.
			conf.set("mapred.job.tracker", "<myhost>:9001");
			conf.set("fs.default.name", "hdfs://<myhost>:9000");
			conf.set("dfs.replication", "1");
		
	}
	
	static int printUsage() {
		System.out.println("TimeSeriesJob [optional hadoop arguments] [-csvcols d,o,h,l,c,v,a ] [-period y|q|m|w|d ] [-outcols d,o,h,l,c,v,a ]" + 
	                       " [-outhdr t|f] [-sort asc|desc ] [-filter o,ho,lo,h,lh,l,hl,c,hv,lv,a,ha,la ]" +
				           " <hadoop input path> <hadoop output path>");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
		}
	
	@Override
	public int run(String[] args) throws Exception {
		System.out.println("\n\nTimeSeries\n");
		
		
		this.AddClassConf();
		
		// set command line arguments into configuration.
		List<String> io_args = tsConf.parseArgs(args);
		
		// Make sure there are exactly 2 parameters left.
		if (io_args.size() != 2) {
			System.out.println("ERROR: Wrong number of parameters: " + io_args.size() + " instead of 2.");
			return printUsage();
		}
		
        Job job = Job.getInstance(tsConf.conf, "Yahoo Timeseries m/r");
		
		//this.PrintJobConf(conf);

	    job.setOutputKeyClass(TSCompositeKey.class);

	    job.setOutputValueClass(TSData.class);

	    job.setMapperClass(TSDataMapper.class);
	    
	    if (tsConf.getProp(TSConfProperties.TIMESERIES_FILTER).equals(""))
	      job.setReducerClass(TSPeriodReducer.class);
	    else 
	    	// o,ho,lo,h,lh,l,hl,c,hv,lv,a,ha,la
	      job.setReducerClass(TSPeriodRecReducer.class);

	    job.setInputFormatClass(TextInputFormat.class);
	    //job.setOutputFormatClass(TextOutputFormat.class);
	    
	    // Output to specified file. Do not want an empty default file created.
	    LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
	    
	    job.setPartitionerClass(GroupPartitioner.class);
	
		job.setGroupingComparatorClass(GroupingKeyComparator.class);
		job.setSortComparatorClass(OutputKeyComparator.class);

	    job.setJarByClass(Finance_TimeSeriesDriver.class);
	
		Path arg1 = new Path(io_args.get(0));
		Path arg2 = new Path(io_args.get(1) + FileSystemPath.separator + "Job1");
		FileInputFormat.setInputPaths(job, arg1);
	    FileOutputFormat.setOutputPath(job, arg2);

	    boolean success = job.waitForCompletion(true);
	    return(success ? 0 : 1);	
	}



public static void main(String[] args)  throws Exception {
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    int res = ToolRunner.run(new Finance_TimeSeriesDriver(), otherArgs);
    
	System.exit(res);
}

}

