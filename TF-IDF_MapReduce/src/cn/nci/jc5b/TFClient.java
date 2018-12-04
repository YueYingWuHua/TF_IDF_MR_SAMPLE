package cn.nci.jc5b;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import cn.nci.jc5b.mr.TFMapReduceCore;
import cn.nci.jc5b.utils.MapReducerUtils;

public class TFClient {

	    private static String inputPath = "./input";
	    private static String outputPath = "./output";   
	    
	    public static void main(String[] args) {
	        TFClient client = new TFClient();
	        if (args.length == 2) {
	            inputPath = args[0];
	            outputPath = args[1];
	        } 	        
	        try {        	
	        	client.runTFJob(inputPath, outputPath);
	        } catch (Exception e) {
	            System.err.println(e);
	        }
	    }
 
	    private int runTFJob(String inputPath, String outputPath) throws Exception {
	        Configuration configuration = new Configuration();
	        MapReducerUtils.removeOutputFolder(configuration, outputPath);
	        
	        Job job = Job.getInstance(configuration);
	        job.setJobName("TF");
	        job.setJarByClass(TFMapReduceCore.class);

	        job.setMapperClass(TFMapReduceCore.TFMapper.class);
	        job.setCombinerClass(TFMapReduceCore.TFCombiner.class);
	        job.setPartitionerClass(TFMapReduceCore.TFPartitioner.class);
	        job.setNumReduceTasks(getNumReduceTasks(configuration, inputPath));
	        job.setReducerClass(TFMapReduceCore.TFReducer.class);

	        job.setMapOutputKeyClass(Text.class);
	        job.setMapOutputValueClass(Text.class);
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(Text.class);

	        FileInputFormat.addInputPath(job, new Path(inputPath));
	        FileOutputFormat.setOutputPath(job, new Path(outputPath));
	        
	        return job.waitForCompletion(true) ? 0 : 1;
	    }
	    
	    private int getNumReduceTasks(Configuration configuration, String inputPath) throws Exception {
	        FileSystem hdfs = FileSystem.get(configuration);
	        FileStatus status[] = hdfs.listStatus(new Path(inputPath));
	        return status.length;
	    }
}

