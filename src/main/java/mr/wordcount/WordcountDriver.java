package mr.wordcount;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordcountDriver {
	public static void main(String[] args) throws ClassNotFoundException, InterruptedException, IOException {
		
		args = new String[] {"E:/Hadoop/input","E:/Hadoop/output"};
		Configuration conf = new Configuration();
		//��ȡjob����
		Job job = Job.getInstance(conf);
		
		//����jar �洢·��
		job.setJarByClass(WordcountDriver.class);
		
		//����map��reduce��
		job.setMapperClass(WordcountMapper.class);
		job.setReducerClass(WordcountReducer.class);
		
		//����mapper�׶�������ݵ�key��value���ͣ��̶���·
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//�����������������key��value���ͣ��̶���·
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//��������·�������·��
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//�������´�����Խ���Ƭ����Ϊ�趨ֵ,������洢��Ƭ���ֵ����Ϊ4M = 4194304
		job.setInputFormatClass(CombineTextInputFormat.class);
		CombineTextInputFormat.setMaxInputSplitSize(job, 20971520);
		
		//�ύjob
		boolean result = job.waitForCompletion(true);
		System.exit(result?1:0);
	}
}
