package mr.flowcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.checkerframework.checker.units.qual.kg;

import com.sun.tools.classfile.StackMapTable_attribute.verification_type_info;


public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
	
	Text k = new Text();
	FlowBean v = new FlowBean();
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// ��ȡһ��
		String line = value.toString();
		
		
		
		// ����"\t"�и�
		String[] fields = line.split(" ");
		
		// ��װ����
		k.set(fields[0]); // ��װ�ֻ���
		
		long upFlow = Long.parseLong(fields[fields.length - 2]);
		long downFlow = Long.parseLong(fields[fields.length-1]);
		
		v.setUpFlow(upFlow);
		v.setDownFlow(downFlow);
		
		// д��
		context.write(k, v);
		
	}
}
