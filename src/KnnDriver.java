import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class KnnDriver
{
	
	public static void main(String[] args) throws Exception
	{
		// Create configuration
		Configuration conf = new Configuration();
		
		if (args.length != 2)
		{
			System.err.println("Usage: KnnPattern <in> <out> <parameter file>");
			System.exit(2);
		}
		
		// Create job
		Job job = Job.getInstance(conf, "Find K-Nearest Neighbour");
		job.setJarByClass(KnnDriver.class);
		
		
		// Setup MapReduce job
		job.setMapperClass(KnnMaper.class);
		job.setReducerClass(KnnReducer.class);
		job.setNumReduceTasks(1); // Only one reducer in this design

		// Specify key / value
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(DoubleString.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		Random r = new Random();
		// Input (the data file) and Output (the resulting classification)
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + r.nextFloat()));
		// Execute job and return status
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
}