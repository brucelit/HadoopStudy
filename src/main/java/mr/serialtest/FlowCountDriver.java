package mr.serialtest;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class FlowCountDriver {
	
	public static void main(String[] args) throws IOException,ClassNotFoundException, InterruptedException{
		
		args = new String [] {"E:/Hadoop/input","E:/Hadoop/output"};
		// ��ȡjob����
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf );
		
		//����jar��·��
		job.setJarByClass(FlowCountDriver.class);
		
		// ����mapper��reducer
		job.setMapperClass(FlowCountMapper.class);
		job.setReducerClass(FlowCountReducer.class);
		
		//����mapper�����key��value����
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		
		//�������������key��value����
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		//�����������·��
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//�ύjob
		
		boolean result = job.waitForCompletion(true);
		System.exit(result?1:0);
	}
}
