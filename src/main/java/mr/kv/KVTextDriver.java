package mr.kv;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KVTextDriver {
	public static void main(String[] args) throws IOException,ClassNotFoundException, InterruptedException{
		
		
		args = new String[] {"E:/Hadoop/input","E:/Hadoop/output"};
		Configuration conf = new Configuration();
		
		//�����иʽ
		conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPARATOR, " ");
		
		//��ȡjob����
		Job job = Job.getInstance(conf);
		
		//����jar·��
		job.setJarByClass(KVTextDriver.class);
		
		//����mapper��reducer��
		job.setMapperClass(KVTextMapper.class);
		job.setReducerClass(KVTextReducer.class);
		
		//����mapper�����key��value����
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		
		//�������������key��value����
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//��Ĭ�ϵ�inputformat��ʽ����
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		
		//�����������·��
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//�ύjob
		boolean result = job.waitForCompletion(true);
		System.exit(result?1:0);
	}
}
