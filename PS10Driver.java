package mapreduce.demo.task8;

import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.IntWritable;

public class Task8 {
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "DemoTask8");
		job.setJarByClass(PS10Driver.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(PS10Mapper.class);
		job.setReducerClassPS10Reducer.class);
		 
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0])); 
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		/*
		Path out=new Path(args[1]);
		out.getFileSystem(conf).delete(out);
		*/
		
		job.waitForCompletion(true);
	}
}
