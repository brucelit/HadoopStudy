package mr.inputformat;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SequenceFileMapper extends Mapper<Text, BytesWritable, Text, BytesWritable>{
	@Override
	protected void map(Text key, BytesWritable value, Mapper<Text, BytesWritable, Text, BytesWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		// 将所有txt往环形缓冲区发，然后统一由reduce将其读走
		context.write(key, value);
	}
}
