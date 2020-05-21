/**
 * MapReduce job(s) to calculate Matrix Multiply
 * (c) 2020 CSCI339 Kang and Yu
 */

// Java IO Packages
import java.io.IOException;
import java.util.Iterator;
import java.io.FileWriter;

// Hadoop IO Configuration libraries
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// Hadoop Mapreduce Libraries 
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;

// JSON Simple 
//import org.json.simple.*;
//import org.json.simple.parser.*;
//import org.json.simple.JSONObject;

/**
 *  MapReduce job chain that 
 * (1) pipes impressions and click logs to produce intermediate ([ referrer, ad_id] , clicked/not clicked) key-val pairs
 * (2) pipes ([ referrer, ad_id] , clicked/not clicked) key-val pairs to produce cumulative [referrer,ad_id], click-through rate
 */
public class Dot {
    public static void main(String[] args) throws Exception {
	if (args.length < 2) {
	    System.err.println("Error: Wrong number of parameters");
	    System.err.println("Expected: [in] [out]");
	    System.exit(1);
	}

	Configuration conf = new Configuration();

	Job job = Job.getInstance(conf, "trivial job");
	job.setJarByClass(Dot.class);

	// set the Mapper and Reducer functions we want
	job.setMapperClass(Dot.IdentityMapper.class);
	job.setReducerClass(Dot.IdentityReducer.class);
	job.setMapOutputKeyClass(DoubleWritable.class);
	job.setMapOutputValueClass(DoubleWritable.class);
	job.setOutputKeyClass(DoubleWritable.class);
	job.setOutputValueClass(DoubleWritable.class);
	// input arguments tell us where to get/put things in HDFS
	KeyValueTextInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));

	// ternary operator - a compact conditional that just returns 0 or 1
	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * map: (LongWritable, Text) --> (LongWritable, Text)
     * NOTE: Keys must implement WritableComparable, values must implement Writable
     */
    public static class IdentityMapper extends Mapper < LongWritable, Text,
	DoubleWritable, DoubleWritable > {

	@Override
	public void map(LongWritable key, Text val, Context context)
	    throws IOException, InterruptedException {
	    // write (key, val) out to memory/disk
	    // uncomment for debugging
	    //System.out.println("key: "+key+" val: "+val);
	    String[] vals = val.toString().split("\t");
	    double product = Double.parseDouble(vals[0]) * Double.parseDouble(vals[1]);	       
	    //System.out.println(product);
	    //System.out.println(vals);
	    context.write(new DoubleWritable(1), new DoubleWritable(product));
	}

    }

    /**
     * reduce: (LongWritable, Text) --> (LongWritable, Text)
     */
    public static class IdentityReducer extends Reducer < DoubleWritable, DoubleWritable,
						DoubleWritable, DoubleWritable > {

	@Override
	public void reduce(DoubleWritable key, Iterable < DoubleWritable > values, Context context)
	    throws IOException, InterruptedException {
	    // write (key, val) for every value
	    double sum = 0;
	    for (DoubleWritable val : values) {
		// uncomment for debugging
		sum += val.get();
		//System.out.println("key: "+key+" val: "+val);
	    }
	    //System.out.println("sum: "+sum);
	    context.write(key, new DoubleWritable(sum));
	}

    }
}
