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
		//获取job对象
		Job job = Job.getInstance(conf);
		
		//设置jar 存储路径
		job.setJarByClass(WordcountDriver.class);
		
		//关联map和reduce类
		job.setMapperClass(WordcountMapper.class);
		job.setReducerClass(WordcountReducer.class);
		
		//设置mapper阶段输出数据的key和value类型，固定套路
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//设置最终数据输出的key和value类型，固定套路
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//设置输入路径和输出路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//增加如下代码可以将切片数改为设定值,将虚拟存储切片最大值设置为4M = 4194304
//		job.setInputFormatClass(CombineTextInputFormat.class);
//		CombineTextInputFormat.setMaxInputSplitSize(job, 20971520);
		
		
		// 设置分区
		job.setNumReduceTasks(3);
		//提交job
		boolean result = job.waitForCompletion(true);
		System.exit(result?1:0);
	}
}
