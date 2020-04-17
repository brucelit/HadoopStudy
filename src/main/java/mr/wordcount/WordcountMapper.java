package mr.wordcount;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//map�׶δ���
//KEYIN���������ݵ�key
//VALUEIN���������ݵ�value
//KEYOUT��������ݵ�����
//VALUEOUT�����������value����
public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	//����key��value����
	Text k = new Text();
	IntWritable v = new IntWritable(1);
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
	throws IOException, InterruptedException{
		
		//��ȡһ��
		String line = value.toString();
		//�и��
		String [] words = line.split(" ");
		//ѭ��д��
		for(String word: words) {
			k.set(word);
			//v.set(1);
			context.write(k, v);
		}
	}
}
