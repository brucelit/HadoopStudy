package mr.flowcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner extends Partitioner<Text, FlowBean>{

	@Override
	public int getPartition(Text key, FlowBean value, int numPartitions) {
		// TODO Auto-generated method stub
		//重写getpartition方法
		//key是手机号
		//flowbean是流量信息
		String prePhonenum = key.toString().substring(0,3);
		int partition = 5;
		if ("130".equals(prePhonenum))
			partition = 0;
		else if ("131".equals(prePhonenum))
			partition = 1;
		else if ("132".equals(prePhonenum))
			partition = 2;
		else if ("133".equals(prePhonenum))
			partition = 3;
		else
			partition = 4;
		return partition;
	}
	

	
}
