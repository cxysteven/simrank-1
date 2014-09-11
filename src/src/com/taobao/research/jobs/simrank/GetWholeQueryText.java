package com.taobao.research.jobs.simrank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import com.taobao.research.jobs.common.Common;


/**
 * 通过Join原始Query的编号文件和keyword的归一化词表来得到带有归一化的keyword的Query Text
 * 
 * @author yangxudong.pt
 * @create_time: 2011-9-19
 */
public class GetWholeQueryText
{
	private static class Key implements WritableComparable<Key>
	{
		public Text normKey;
		public byte order;
		
		public Key()
		{
			normKey = new Text();
		}
		
		@Override
		public void readFields(DataInput in) throws IOException
		{
			normKey.readFields(in);
			order = in.readByte();
		}
		@Override
		public void write(DataOutput out) throws IOException
		{
			normKey.write(out);
			out.writeByte(order);
		}
		@Override
		public int compareTo(Key o)
		{
			int cmp = normKey.compareTo(o.normKey);
			if (cmp != 0)
				return cmp;
			if (order != o.order)
				return order < o.order ? -1 : 1;
			return 0;
		}
		/** A Comparator that compares serialized CompositeKey. */
		@SuppressWarnings("unused")
		public static class Comparator extends WritableComparator
		{
			public Comparator()
			{
				super(Key.class);
			}

			public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)					
			{
				@SuppressWarnings("unchecked")
				RawComparator<Text> comparator = WritableComparator.get(Text.class);
				int len = Byte.SIZE / 8;
				int cmp = comparator.compare(b1, s1, l1 - len, b2, s2, l2 - len);
				if (cmp != 0)
					return cmp;
				
				byte o1 = b1[s1 + l1 - len];
				byte o2 = b2[s2 + l2 - len];
				if (o1 != o2)
					return o1 < o2 ? -1 : 1;
				return 0;
			}

			static
			{ // register this comparator
				WritableComparator.define(Key.class, new Comparator());
			}
		}
	}
	
	//按照组合键的第一个元素（原始键）分组，为了方便对用一个（原始）键对应的值排序
	private static class GroupComparator implements RawComparator<Key>
	{		
		@SuppressWarnings("unchecked")
		@Override
	    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
		{
			RawComparator<Text> comparator = WritableComparator.get(Text.class);
			int len = Byte.SIZE / 8;
			return comparator.compare(b1, s1, l1 - len, b2, s2, l2 - len);
	    }		

		@Override
		public int compare(Key k1, Key k2)
		{
			return k1.normKey.compareTo(k2.normKey);
		}
	}
	
	private static class JobPartitioner implements Partitioner<Key, Text>
	{
		public int getPartition(Key key, Text value, int numPartitions)
		{
			Text norm_key = key.normKey;
			return (norm_key.hashCode() & Integer.MAX_VALUE) % numPartitions;
		}

		@Override
		public void configure(JobConf arg0)
		{}
	}
	
	/**
	 * 为Reduce端的Join做准备
	 */
	public static class JoinMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Key, Text>
	{
		private boolean queryCode;
		private Key key = new Key();
		private Text normKeyword = key.normKey;
		private Text value = new Text();

		@Override
		public void configure(JobConf job)
		{
			String inputPath_qQ = job.get("map.input.file.qQ");
			String inputPath = job.get("map.input.file");

			queryCode = inputPath.indexOf(inputPath_qQ) >= 0;
		}

		@Override
		public void map(LongWritable lineOffset, Text txtLine,
				OutputCollector<Key, Text> output, Reporter reporter)
				throws IOException
		{
			if (queryCode)
			{
				// 输入格式：qQ ^A id ^A norm_key ^C f ^C p, 其中norm_key可能为空
				// 输出格式：norm_key --> qQ ^A id ^A f ^C p
				String line = txtLine.toString();				
				String[] elements = line.split(Common.CONTROL_A, -1);
				if (elements.length != 3)
					return;
				String[] items = elements[2].split(Common.CONTROL_C, -1);
				if (items.length != 3)
					return;
				normKeyword.set(items[0]);	//可能为空
				key.order = 2;
				String strOutput = elements[0] + Common.CONTROL_A + elements[1] + Common.CONTROL_A +
					items[1] + Common.CONTROL_C + items[2]; //qQ ^A id ^A f ^C p
				value.set(strOutput);
				
				output.collect(key, value);
			}
			else
			{
				// 输入格式：nq ^A norm_key ^A raw_key
				// 输出格式：norm_key --> nq ^A raw_key
				String line = txtLine.toString();
				String[] elements = line.split(Common.CONTROL_A);
				if (elements.length != 3)
					return;
				normKeyword.set(elements[1]);
				key.order = 1;
				value.set(elements[0] + Common.CONTROL_A + elements[2]);
				output.collect(key, value);
			}
		}
	}

	private static class JoinReducer extends MapReduceBase implements
			Reducer<Key, Text, IntWritable, Text>
	{
		private IntWritable key = new IntWritable();
		private Text value = new Text();
		private String raw_key;
		private String query;
		
//		private void printReduceInput(Text norm_key, String value)
//		{
//			System.out.print("###input: " + norm_key.toString() + "-->" + value);
//		}
//		
//		private void printReduceOutput(IntWritable key, String value)
//		{
//			System.out.print("###output: " + key.get() + "-->" + value);
//		}

		@Override
		public void reduce(Key norm_key, Iterator<Text> values,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException
		{
			if (norm_key.normKey.getLength() <= 0)
			{
				String[] elements;
				while (values.hasNext())
				{//value的格式：qQ ^A id ^A f ^C p
					elements = values.next().toString().split(Common.CONTROL_A);
					key.set(Integer.parseInt(elements[1]));
					value.set(Common.CONTROL_C + elements[2]);
					output.collect(key, value);
				}
				return;
			}
			
			raw_key = null;//清空上一次保存的值
			while (values.hasNext())
			{
				String v = values.next().toString();
				String[] elements = v.split(Common.CONTROL_A);
				if (elements[0].equals("nq"))
				{
					raw_key = elements[1];
				}
				else// if (v.startsWith("qQ"))
				{//value的格式：qQ ^A id ^A f ^C p
					int qid = Integer.parseInt(elements[1]);
					key.set(qid);
					if (null == raw_key || raw_key.isEmpty())
					{//没有对应的raw key
						query = norm_key.normKey.toString() + Common.CONTROL_C + elements[2];
						value.set(query);
						output.collect(key, value);
						//System.err.println("Error: raw_key is null or empty for " + norm_key.normKey.toString());
						continue;
					}
					query = norm_key.normKey.toString() + Common.CONTROL_C + raw_key + Common.CONTROL_C + elements[2];
					
					value.set(query);
					output.collect(key, value);
					//printReduceOutput(key, query);
				}
			}
		}
	}

	protected static void configJob(JobConf conf)
	{
		conf.setJobName("Get Whole Query Text");
		conf.setJarByClass(GetWholeQueryText.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		conf.setPartitionerClass(JobPartitioner.class);
		conf.setMapperClass(JoinMapper.class);
		conf.setReducerClass(JoinReducer.class);
		conf.setMapOutputKeyClass(Key.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
		conf.setOutputValueGroupingComparator(GroupComparator.class);
		conf.set("mapred.child.java.opts", "-Xmx3000m");
		conf.setInt("io.sort.mb", 1024);
		conf.setInt("io.sort.factor", 100);
		//对map任务输出进行压缩
		conf.setCompressMapOutput(true);
		conf.setMapOutputCompressorClass(GzipCodec.class);
		conf.setInt("mapred.reduce.parallel.copies", 10);//Reduce端从Map端并行拷贝数据的并行线程数
		conf.setInt("mapred.inmen.merge.threshold", 0);	//不通过阈值来控制是否启动Merge & Spill
		conf.set("mapred.job.shuffle.input.buffer.percent", "0.8");//map输出缓冲区占整个堆空间的百分比
		// 对输出进行压缩
//		conf.setBoolean("mapred.output.compress", true);
//		conf.setClass("mapred.output.compression.codec", GzipCodec.class, CompressionCodec.class);
//		SequenceFileOutputFormat.setOutputCompressionType(conf,	SequenceFile.CompressionType.BLOCK);
//		SequenceFileOutputFormat.setOutputCompressorClass(conf, GzipCodec.class);
	}

	public static boolean runJob(String inputPath_qQ, String inputPathNQ, String outputDirPath, int R) throws Exception
	{
		System.out.print("************************************************* <getting whole query text> ");
		System.out.println("*************************************************");

		if (inputPath_qQ == null || inputPath_qQ.isEmpty())
			throw new Exception("inputPath_qQ is null or empty");
		if (inputPathNQ == null || inputPathNQ.isEmpty())
			throw new Exception("inputPathNQ is null or empty");
		if (outputDirPath == null || outputDirPath.isEmpty())
			throw new Exception("outputDirPath is null or empty");
		
		System.out.println("### inputPath_qQ: " + inputPath_qQ);
		System.out.println("### inputPathNQ: " + inputPathNQ);
		System.out.println("### outputDirPath: " + outputDirPath);
		System.out.println("### Reduce num: " + R);
		
		JobConf job = new JobConf();
		FileSystem fs = FileSystem.get(job);
		inputPath_qQ = fs.makeQualified(new Path(inputPath_qQ)).toString();
		inputPathNQ = fs.makeQualified(new Path(inputPathNQ)).toString();
		outputDirPath = fs.makeQualified(new Path(outputDirPath)).toString();

		FileInputFormat.addInputPath(job, new Path(inputPath_qQ));
		FileInputFormat.addInputPath(job, new Path(inputPathNQ));
		FileOutputFormat.setOutputPath(job, new Path(outputDirPath));
		job.set("map.input.file.qQ", inputPath_qQ);
		job.setNumReduceTasks(R);
		fs.delete(new Path(outputDirPath), true);
		configJob(job);
		RunningJob runningJob = JobClient.runJob(job);
		return runningJob.isSuccessful();
	}

	public static void main(String[] args) throws Exception
	{
		String inputPath_qQ = "/group/tbalgo-dev/zhouxj/aqr/20110829/qas/qQpart-00000"; 
		String inputPathNQ = "/group/tbalgo-dev/zhouxj/aqr/20110829/NQ";
		String outputDirPath = "/group/tbalgo-dev/zhenghon/simrank/queryText";
//		String inputPath_qQ = "/group/tbalgo-dev/zhenghon/simrank/test/qQ"; 
//		String inputPathNQ = "/group/tbalgo-dev/zhenghon/simrank/test/nq.txt";
		runJob(inputPath_qQ, inputPathNQ, outputDirPath, 400);
	}
}
