package mr.inputformat;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.hamcrest.core.Is;
import com.sun.tools.classfile.StackMapTable_attribute.verification_type_info;

// ��ÿ���ļ�����һ��
public class WholeRecordReader extends RecordReader<Text, BytesWritable>{

	// ��ȡ����С�ļ�
	FileSplit split;
	Configuration configuration;
	Text k = new Text();
	BytesWritable v = new BytesWritable();
	boolean isProgress = true;
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		// ��ʼ����ȡ��Ƭ
		this.split = (FileSplit)split;
		configuration = context.getConfiguration();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// ����ҵ���߼�
		
		if (isProgress) {
		// 1 ��ȡ�ļ�ϵͳfs���󣬰��ļ����Ʒ�װ��key��ļ����ݷ�װ��byte��
		Path path=split.getPath();
		FileSystem fs = path.getFileSystem(configuration);
		
		// 2 ��ȡ������
		FSDataInputStream fis = fs.open(path);
		
		// 3 ����һ�������������ļ����ݿ����������ļ�
		byte[] buf = new byte[(int)split.getLength()];
		IOUtils.readFully(fis, buf, 0, buf.length);
		
		// 4 ��װv,k
		v.set(buf,0,buf.length);
		k.set(path.toString());
		
		// 5 �ر���Դ
		IOUtils.closeStream(fis);
		isProgress = false;
		return true;
		}
		return false;
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		
		
		return k;
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException, InterruptedException {
	
		return v;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}
	
}
