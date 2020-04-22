package mr.serialtest;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.sun.tools.classfile.StackMapTable_attribute.verification_type_info;

public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean>{
	
	FlowBean v = new FlowBean();
	
	@Override
	protected void reduce(Text key, Iterable<FlowBean> values, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		long sum_upFlow = 0;
		long sum_downFlow = 0;
		// �ۼ����
		for(FlowBean flowBean: values) {
			sum_upFlow += flowBean.getUpFlow();
			sum_downFlow += flowBean.getDownFlow();
		}
		
		//��װҪ����Ķ���
		v.set(sum_upFlow, sum_downFlow); 
		
		// д��
		context.write(key, v);
	}

}
