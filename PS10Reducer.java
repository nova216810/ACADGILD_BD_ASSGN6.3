package mapreduce.demo.task8;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class PS10Reducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>
{	
	ArrayList<IntWritable> tempArray;
	
	@Override
	public void setup(Context context) {
		tempArray = new ArrayList<IntWritable> ();
	}
	
	@Override
	public void reduce(IntWritable key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
	{
		tempArray.clear();
				
		for (IntWritable value : values) {
			tempArray.add(new IntWritable(value.get()));
		}
		
		Collections.sort(tempArray, new Comparator<IntWritable>() {
	        @Override
	        public int compare(IntWritable val1, IntWritable val2)
	        {
	            return  (-1) * (val1.get() - val2.get());
	        }
		});

		int endIndex = 3;
		if (tempArray.size() < 3) {
			endIndex = tempArray.size();
		}
		
		for (int i = 0; i < endIndex; i++) {
			context.write(key, tempArray.get(i));
		}	
	}
}
