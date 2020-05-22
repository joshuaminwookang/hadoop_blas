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

/**
 *  MapReduce job chain that 
 * (1) pipes impressions and click logs to produce intermediate ([ referrer, ad_id] , clicked/not clicked) key-val pairs
 * (2) pipes ([ referrer, ad_id] , clicked/not clicked) key-val pairs to produce cumulative [referrer,ad_id], click-through rate
 */
public class MatrixMultiply {

    public static void main(String[] args) throws Exception {
	if (args.length < 2) {
	    System.err.println("Error: Wrong number of parameters");
	    System.err.println("Expected: [in] [out]");
	    System.exit(1);
	}
	Configuration conf = new Configuration();

	Job job = Job.getInstance(conf, "trivial job");
	job.setJarByClass(MatrixMultiply.class);

	// set the Mapper and Reducer functions we want
	job.setMapperClass(MatrixMultiply.MMMapper.class);
	job.setReducerClass(MatrixMultiply.MMReducer.class);
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
     * Reads both logs; for Impressions Log, produce (impression_id, [referrer, ad_id])
     * For Clicks log, produce (impression_id, click or not)
     */
    public static class MMMapper extends Mapper < LongWritable, Text,
					       DoubleWritable, DoubleWritable > {

	        @Override
		public void map(LongWritable key, Text val, Context context)
		    throws IOException, InterruptedException {
		    // write (key, val) out to memory/disk
		    // uncomment for debugging
		    //System.out.println("key: "+key+" val: "+val);
		    String[] vals = val.toString().split("\t");
		    double product = 0.0;
		    int len = vals.length/2;
		    for (int i = 0; i < len; i++) {
			product += Double.parseDouble(vals[i]) * Double.parseDouble(vals[i+len]);
		    }
		    //System.out.println(product);
		    //System.out.println(vals);
		    context.write(new DoubleWritable(key.get()), new DoubleWritable(product));
		}
    }

    
    /**
     * reduce: (Text, Text) --> (Text, Text)
     * Produce intermediate file; for each impression, create [referrer, adId] + 1 or 0 
     */
    public static class MMReducer extends Reducer < DoubleWritable, DoubleWritable,
						DoubleWritable, DoubleWritable > {

	        @Override
		public void reduce(DoubleWritable key, Iterable < DoubleWritable > values, Context context)
		    throws IOException, InterruptedException {
		    // write (key, val) for every value
		    for (DoubleWritable val : values) {
			context.write(key, val);
		    }
		}

    }

}
