package mr.kv;

import java.io.IOException;

import org.apache.commons.math3.stat.descriptive.summary.Sum;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KVTextReducer extends Reducer<Text, IntWritable,Text, IntWritable>{
	
	IntWritable v = new IntWritable();
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub

		int sum = 0;
		// <banzhang,1>
		// 累加求和,values是一个一个的1
		for (IntWritable value: values) {
			sum += value.get();
		}
		
		v.set(sum);
		
		context.write(key, v);
	}
}
