package mr.wordcount;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import java.lang.Iterable;

public class WordcountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

	int sum;
	IntWritable v = new IntWritable();

	protected void reduce(Text key, Iterable<IntWritable> values, 
			Context context) throws java.io.IOException ,InterruptedException {
		
		//累加求和
		sum = 0;
		for(IntWritable value:values) {
			sum += value.get();
			
		}
		//写出到下一级
		
		v.set(sum);
		context.write(key, v);
	};
}
