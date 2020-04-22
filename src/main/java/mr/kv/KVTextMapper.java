package mr.kv;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.sun.tools.classfile.StackMapTable_attribute.verification_type_info;
import com.sun.tools.javap.Context;

public class KVTextMapper extends Mapper<Text, Text, Text, IntWritable>{
	
	IntWritable v = new IntWritable(1);
	
	@Override
	protected void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		// 直接封装对象
		
		
		//	写出
		context.write(key,v);
	}
}
