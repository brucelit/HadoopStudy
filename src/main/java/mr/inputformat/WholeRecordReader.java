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

// 对每个文件调用一次
public class WholeRecordReader extends RecordReader<Text, BytesWritable>{

	// 读取三个小文件
	FileSplit split;
	Configuration configuration;
	Text k = new Text();
	BytesWritable v = new BytesWritable();
	boolean isProgress = true;
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		// 初始化获取切片
		this.split = (FileSplit)split;
		configuration = context.getConfiguration();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// 核心业务逻辑
		
		if (isProgress) {
		// 1 获取文件系统fs对象，把文件名称封装到key里，文件内容封装到byte里
		Path path=split.getPath();
		FileSystem fs = path.getFileSystem(configuration);
		
		// 2 获取输入流
		FSDataInputStream fis = fs.open(path);
		
		// 3 创建一个缓存区，将文件内容拷贝到缓存文件
		byte[] buf = new byte[(int)split.getLength()];
		IOUtils.readFully(fis, buf, 0, buf.length);
		
		// 4 封装v,k
		v.set(buf,0,buf.length);
		k.set(path.toString());
		
		// 5 关闭资源
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
