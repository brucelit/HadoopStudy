package mr.wordcount;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//map阶段处理
//KEYIN是输入数据的key
//VALUEIN是输入数据的value
//KEYOUT是输出数据的类型
//VALUEOUT是输出的数据value类型
public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	//创建key和value对象
	Text k = new Text();
	IntWritable v = new IntWritable(1);
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
	throws IOException, InterruptedException{
		
		//获取一行
		String line = value.toString();
		//切割单词
		String [] words = line.split(" ");
		//循环写出
		for(String word: words) {
			k.set(word);
			//v.set(1);
			context.write(k, v);
		}
	}
}
