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
public class MM {

    public static void main(String[] args) throws Exception {
	if (args.length < 2) {
	    System.err.println("Error: Wrong number of parameters");
	    System.err.println("Expected: [in] [out]");
	    System.exit(1);
	}

	JobControl jobControl = new JobControl("jobChain"); 

	// Set up Job 1
	Configuration conf1 = new Configuration();//getConf();
	Job job1 = Job.getInstance(conf1, "step 1");
	job1.setJarByClass(ClickRate.class);
	job1.setJobName("step1");
	
	// set the Mapper and Reducer functions we want
	job1.setMapperClass(ClickRate.FirstMapper.class);
	job1.setReducerClass(ClickRate.FirstReducer.class);
	job1.setMapOutputKeyClass(Text.class);
	job1.setOutputKeyClass(Text.class);
	
	// input arguments tell us where to get/put things in HDFS
	MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class);
	MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class);
	FileOutputFormat.setOutputPath(job1, new Path("intermediate"));

	// Add Job1 to controller
	ControlledJob controlledJob1 = new ControlledJob(conf1);
	controlledJob1.setJob(job1);
	jobControl.addJob(controlledJob1);
	
	// Set up Job2
	Configuration conf2 = new Configuration();
	Job job2 = Job.getInstance(conf2);
	job2.setJarByClass(ClickRate.class);
	job2.setJobName("step 2");

	// set the Mapper and Reducer functions we want
	job2.setMapperClass(ClickRate.SecondMapper.class);
	job2.setReducerClass(ClickRate.SecondReducer.class);
	job2.setMapOutputKeyClass(Text.class);
	job2.setOutputKeyClass(Text.class);

	// input arguments tell us where to get/put things in HDFS 
	KeyValueTextInputFormat.addInputPath(job2, new Path("intermediate/part-r-00000"));
	FileOutputFormat.setOutputPath(job2, new Path(args[2]));

	// Add Job2 to JobControl; chain the two Jobs
	ControlledJob controlledJob2 = new ControlledJob(conf2);
	controlledJob2.setJob(job2);
	controlledJob2.addDependingJob(controlledJob1);
	jobControl.addJob(controlledJob2);

	// Launch Job Control
	Thread jobControlThread = new Thread(jobControl);
	jobControlThread.start();
	while(!jobControl.allFinished()){}
	System.exit(job1.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * map: (LongWritable, Text) --> (Text, Text)
     * Reads both logs; for Impressions Log, produce (impression_id, [referrer, ad_id])
     * For Clicks log, produce (impression_id, click or not)
     */
    public static class FirstMapper extends Mapper < LongWritable, Text,
					       Text, Text > {
	@Override
	public void map(LongWritable key, Text val, Context context)
	    throws IOException, InterruptedException {
	    // Setup JSON Parser
	    JSONParser parser = new JSONParser();
	    JSONObject obj = new JSONObject();
	    try {
		obj = (JSONObject) parser.parse(val.toString());
	    } catch (org.json.simple.parser.ParseException e) {
		System.out.println(e);
	    }
	    // Intermediate key,val = impressionId, [referrer, adId] or clicked/xClicked
	    Text intermediateKey = new Text(obj.get("impressionId").toString());
	    Text intermediateVal;
	    if (obj.containsKey("referrer")) {
		intermediateVal = new Text(("["+obj.get("referrer")+", "+obj.get("adId")+"]"));
	    } else {
		intermediateVal = new Text("clicked");
	    }
	    // Write to disk
	    context.write(intermediateKey, intermediateVal);
	}

    }

    /**                                                                                                                                                             * map: (LongWritable, Text) --> (Text, Text)                                                                                                                   * Reads intermediate file; produce [referrer, adId], click/no click pairs
     */
    public static class SecondMapper extends Mapper < LongWritable, Text,
					       Text, Text > {
	        @Override
		public void map(LongWritable key, Text val, Context context)
		    throws IOException, InterruptedException {
		    String line = val.toString();
		    String intermediateKey = line.substring(0, line.length()-1); // referrer + adID
		    String intermediateVal = line.substring(line.length()-1); // click or not (0 or 1)
		    context.write(new Text(intermediateKey), new Text(intermediateVal));
		}
    }
    
    /**
     * reduce: (Text, Text) --> (Text, Text)
     * Produce intermediate file; for each impression, create [referrer, adId] + 1 or 0 
     */
    public static class FirstReducer extends Reducer < Text, Text, Text, Text > {
	@Override
	public void reduce(Text key, Iterable < Text > values, Context context)
	    throws IOException, InterruptedException {
	    String finalKey = "Default";
	    String finalVal = "0";
	    Iterator<Text> i = values.iterator();
	    while (i.hasNext()) {
		String val = i.next().toString();
	      	if (val.equals("clicked")) {
		    finalVal = "1";
		} else {
		    finalKey = val;
		}
	    }
	    context.write(new Text(finalKey), new Text(finalVal));
	}

    }
   /**                                                                                                                                                         
    * reduce: (Text, Text) --> (Text, Text)                                                                            
    * Produce final click through rates for each [referrer, adId]
    */
    public static class SecondReducer extends Reducer < Text, Text, Text, Text > {
	        @Override
		public void reduce(Text key, Iterable < Text > values, Context context)
		    throws IOException, InterruptedException {
		    double clicks = 0; // Impression that were clicked
		    double total = 0; //Total impressions
		    for (Text val : values) {
			if (val.toString().equals("1")) {
			    clicks++;
			}
			total++;
		    }
		    context.write(key, new Text(Double.toString((clicks/total))));
		}
    }
}
