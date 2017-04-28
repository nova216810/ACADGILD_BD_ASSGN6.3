package mapreduce.demo.task8;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

public class PS10Mapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
		
	IntWritable year;
	IntWritable temp;
	
	@Override
	public void setup(Context context) {
		year = new IntWritable();
		temp = new IntWritable();
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] lineArray = value.toString().split(",");
		
		year.set(Integer.parseInt(lineArray[0].split("-")[2]));
		temp.set(Integer.parseInt(lineArray[2]));
		
		context.write(year, temp);
	}
}
