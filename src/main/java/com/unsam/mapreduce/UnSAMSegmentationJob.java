package com.unsam.mapreduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
public class UnSAMSegmentationJob extends Configured implements Tool {
@Override
public int run(String[] args) throws Exception {
System.out.println("\n=== DEBUG: UnSAMSegmentationJob.run() ===");
System.out.println("Number of arguments: " + args.length);
for (int i = 0; i < args.length; i++) {
System.out.println(" args[" + i + "] = '" + args[i] + "'");
}
if (args.length != 3) {
System.err.println("ERROR: Expected 3 arguments, but got " + args.length);
System.err.println("Usage: UnSAMSegmentationJob <input_path> <output_path>
<model_type>");
System.err.println(" model_type: vit_b, vit_l, or vit_h");
return -1;
}
String inputPath = args[0];
String outputPath = args[1];
String modelType = args[2];
System.out.println("Input path: " + inputPath);
System.out.println("Output path: " + outputPath);
System.out.println("Model type: " + modelType);
System.out.println("========================================\n");
Configuration conf = getConf();
// Step 1: Run profiling to auto-tune configuration
System.out.println("Step 1: Profiling workload for automatic optimization...");
try {
WorkloadProfiler profiler = new WorkloadProfiler(conf);
ProfileResults profile = profiler.profileWorkload(inputPath, modelType);
// Step 2: Apply optimized configuration
System.out.println("Step 2: Applying optimized configuration...");
ConfigurationOptimizer optimizer = new ConfigurationOptimizer(profile);
conf = optimizer.optimizeConfiguration(conf);
// Print optimized parameters
printConfiguration(conf);
// Step 3: Set up MapReduce job
System.out.println("Step 3: Setting up MapReduce job...");
Job job = Job.getInstance(conf, "UnSAM-MR Image Segmentation");
job.setJarByClass(UnSAMSegmentationJob.class);
// Set mapper and reducer classes
job.setMapperClass(ImagePreprocessingMapper.class);
job.setReducerClass(SAMSegmentationReducer.class);
// Set output key/value types
job.setMapOutputKeyClass(Text.class);
job.setMapOutputValueClass(ImageDataWritable.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(Text.class);
// Set custom input/output formats
job.setInputFormatClass(MedicalImageInputFormat.class);
// Set input and output paths
FileInputFormat.addInputPath(job, new Path(inputPath));
FileOutputFormat.setOutputPath(job, new Path(outputPath));
// Add SAM model to distributed cache
// Enable compression for intermediate data
job.addCacheFile(new URI("/unsam/scripts/sam_wrapper.py#sam_wrapper.py"));
job.addCacheFile(new java.net.URI("/unsam/models/mobile_sam.pt#mobile_sam.pt"));
conf.setBoolean("mapreduce.map.output.compress", true);
conf.set("mapreduce.map.output.compress.codec",
"org.apache.hadoop.io.compress.SnappyCodec");
// Set number of reducers based on optimization
int numReducers = conf.getInt("unsam.optimal.reducers", 6);
job.setNumReduceTasks(numReducers);
// Enable speculative execution for stragglers
conf.setBoolean("mapreduce.map.speculative", true);
conf.setBoolean("mapreduce.reduce.speculative", true);
// Step 4: Submit job and monitor
System.out.println("Step 4: Submitting job for execution...");
System.out.println("Processing images from: " + inputPath);
System.out.println("Output will be written to: " + outputPath);
boolean success = job.waitForCompletion(true);
// Step 5: Print job statistics
if (success) {
printJobStatistics(job);
}
return success ? 0 : 1;
} catch (Exception e) {
System.err.println("ERROR in job execution: " + e.getMessage());
e.printStackTrace();
return 1;
}
}
private void printConfiguration(Configuration conf) {
System.out.println("\n=== Optimized Configuration ===");
System.out.println("Map Memory: " +
conf.getInt("mapreduce.map.memory.mb", 0) + " MB");
System.out.println("Reduce Memory: " +
conf.getInt("mapreduce.reduce.memory.mb", 0) + " MB");
System.out.println("Number of Reducers: " +
conf.getInt("unsam.optimal.reducers", 0));
System.out.println("================================\n");
}
private void printJobStatistics(Job job) throws Exception {
System.out.println("\n=== Job Statistics ===");
System.out.println("Job ID: " + job.getJobID());
}
public static void main(String[] args) throws Exception {
System.out.println("\n=== DEBUG: UnSAMSegmentationJob.main() ===");
System.out.println("Main method received " + args.length + " arguments");
for (int i = 0; i < args.length; i++) {
System.out.println(" main args[" + i + "] = '" + args[i] + "'");
}
Configuration conf = new Configuration();
// Set default configurations
conf.set("fs.defaultFS", "hdfs://master:9000");
conf.set("mapreduce.framework.name", "yarn");
conf.set("yarn.resourcemanager.address", "master:8032");
int exitCode = ToolRunner.run(conf, new UnSAMSegmentationJob(), args);
System.exit(exitCode);
}
}
