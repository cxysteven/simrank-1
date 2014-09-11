package com.taobao.research.jobs.simrank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
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
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import com.taobao.research.jobs.common.Common;
import com.taobao.research.jobs.common.IndexPair;
import com.taobao.research.jobs.common.SizeCustomizeTextInputFormat;

/**
 * 权值矩阵的抽取任务
 * @author yangxudong.pt
 *
 */
public class WeightMatrixBuilder
{
	private static final boolean DEBUG = false;
	//点击次数的过滤阈值
	private static int threshold = 0;
	//public static JobID jobID = null;
	
	public static void setThreshold(int t)
	{
		threshold = t;
	}
	
	//计数器
	//enum Counter {MAX_QUERY_ID, MAX_AD_ID}
	
	public static class Key implements WritableComparable<Key>
	{
		/** type用于标识index1是广告的编号（0），还是Query的编号（1）
		 *  type = 0时，value表示normalized_weight(q,a)
		 *  type = 1时，value表示normalized_weight(a,q)
		 *  */
		public byte type;
		/** 表示广告或Query的编号 */
		public int index1;
		/** 表示广告或Query的编号，若index2 == -1, 表示对应的值为方差 */
		public int index2;
		
		
		@Override
		public void readFields(DataInput in) throws IOException
		{
			type = in.readByte();
			index1 = in.readInt();
			index2 = in.readInt();
		}

		@Override
		public void write(DataOutput out) throws IOException
		{
			out.writeByte(type);
			out.writeInt(index1);
			out.writeInt(index2);
		}

		@Override
		public int compareTo(Key o)
		{
			if (type != o.type)
				return type < o.type ? -1 : 1;
			if (index1 != o.index1)
				return index1 < o.index1 ? -1 : 1;
			if (index2 == o.index2)
				return 0;
			return index2 < o.index2 ? -1 : 1;
		}
		@Override
		public boolean equals(Object o)
		{
			if (this == o)
				return true;
			if (!(o instanceof Key))
				return false;
			Key k2 = (Key)o;
			if (this.index1 == k2.index1 && this.index2 == k2.index2 && this.type == k2.type)
				return true;
			return false;
		}
		
		/** A Comparator that compares serialized CompositeKey. */
		public static class Comparator extends WritableComparator
		{
			public Comparator()
			{
				super(Key.class);
			}

			public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)					
			{
				if (b1[s1] != b2[s2])
					return b1[s1] < b2[s2] ? -1 : 1;
				
				int offset = Byte.SIZE / 8;
				int index11 = readInt(b1, s1 + offset);
				int index12 = readInt(b2, s2 + offset);
				if (index11 != index12)
					return (index11 < index12) ? -1 : 1;
				
				offset += Integer.SIZE / 8;
				int index21 = readInt(b1, s1 + offset);
				int index22 = readInt(b2, s2 + offset);
				if (index21 != index22)
					return (index21 < index22) ? -1 : 1;
				
				return 0;
			}

			static
			{ // register this comparator
				WritableComparator.define(Key.class, new Comparator());
			}
		}
	}
	
	public static class Value implements Writable
	{
		/**
		 * index用于表示广告编号或Query编号
		 * 因为GroupComparator的使用，导致在Reducer中会丢失Key.index2的值，
		 * 把该值放在Value中，即index = Key.index2
		 */
		public int index;
		public double val;
		
		@Override
		public void readFields(DataInput in) throws IOException
		{
			index = in.readInt();
			val = in.readDouble();
		}
		@Override
		public void write(DataOutput out) throws IOException
		{
			out.writeInt(index);
			out.writeDouble(val);
		}
		@Override
		public boolean equals(Object o)
		{
			if (o instanceof Value)
			{
				Value v = (Value)o;
				if (v.index == this.index && v.val == this.index)
					return true;
			}
			return false;
		}
	}
	
	private static class JobPartitioner implements
			Partitioner<Key, Value>
	{
		public int getPartition(Key key, Value value,
				int numPartitions)
		{
			int offset = numPartitions / 2;
			if (key.type == 0)
			{
				int base = numPartitions - offset;
				return key.index1 % base + offset;
			}
			
			return key.index1 % offset;
			
		}

		@Override
		public void configure(JobConf arg0)
		{}
	}
	
	//按照前两个元素来分组
	private static class GroupComparator implements RawComparator<Key>
	{		
		@Override
	    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			int len = (Integer.SIZE + Byte.SIZE) / 8;
			int cmp = WritableComparator.compareBytes(b1, s1, len, b2, s2, len);
			return cmp;
	    }		

		@Override
		public int compare(Key k1, Key k2)
		{
			if (k1.type != k2.type)
				return (k1.type < k2.type) ? -1 : 1;
			if (k1.index1 == k2.index1)
				return 0;
			
			return k1.index1 < k2.index1 ? -1 : 1;
		}
	}
	
/*	public static class ReportOutFormat<K extends WritableComparable<IndexPair>, V extends Writable>
			extends MultipleOutputFormat<K, V>
	{

		private SequenceFileOutputFormat<K, V> theOutputFormat = null;

		@Override
		protected RecordWriter<K, V> getBaseRecordWriter(FileSystem fs,
				JobConf job, String name, Progressable arg3) throws IOException
		{
			if (theOutputFormat == null)
			{
				theOutputFormat = new SequenceFileOutputFormat<K, V>();
			}
			return theOutputFormat.getRecordWriter(fs, job, name, arg3);
		}

		@Override
		// 根据key
		protected String generateFileNameForKeyValue(K key, V value, String name)
		{
//			Key k = (Key)key;
//			if (k.type == 0)
//				return "W_Q2A" + name;
//			else
//				return "W_A2Q" + name;
		}
	}
*/
	
	private static class JobMapper extends MapReduceBase
	 	implements Mapper<LongWritable, Text, Key, Value>
	{
		private Key key = new Key();
		private Value value = new Value();		
		private long currMaxQueryId = 0;
		private long currMaxAdId = 0;
		private JobConf jobConf;	//用于在close函数中新建FileSystem对象，防止使用默认的hadoop用户
		//private Reporter task_reporter = null;
		//private RunningJob job;
		private String maxIdDirPath;
		
		//调式用的函数
//		private void printMapInput (LongWritable lineno, Text txt) {
//			System.out.println("##### Map input: (" + lineno.toString() + "-->" + 
//					txt.toString() + ")");
//		}
//		
//		private void printMapOutput (Key key, Value value) {
//			System.out.println("##### Map output: (" + key.type + "," + key.index1 + "," + 
//				key.index2 + ") (" + value.index + "," + value.val + ")");
//		}
		
		@Override
		public void close()
		{
			//统计所有的Query和广告的最大编号
//			Counters counters;
//			try
//			{
//				counters = job.getCounters();
//				long max_query_Id = counters.getCounter(WeightMatrixBuilder.Counter.MAX_QUERY_ID);			
//				if (max_query_Id < currMaxQueryId)
//					task_reporter.incrCounter(Counter.MAX_QUERY_ID, currMaxQueryId - max_query_Id);
//				long max_ad_Id = counters.getCounter(WeightMatrixBuilder.Counter.MAX_AD_ID);
//				if (max_ad_Id < currMaxAdId)
//					task_reporter.incrCounter(Counter.MAX_AD_ID, currMaxAdId - max_ad_Id);
//			}
//			catch (IOException e)
//			{
//				e.printStackTrace();
//			}
			if (currMaxQueryId == 0 && currMaxAdId == 0)
				return;
			
			String maxQureyIdDir = maxIdDirPath + "/query";
			String maxAdIdDir = maxIdDirPath + "/ad";
			Path maxQureyIdDirPath = new Path(maxQureyIdDir);
			Path maxAdIdDirPath = new Path(maxAdIdDir);
			try
			{
				FileSystem fs = FileSystem.get(URI.create(maxIdDirPath), jobConf);
				if (currMaxAdId != 0)
				{
					//用当前最大的AdId创建一个文件
					if (!fs.exists(maxAdIdDirPath))	//判断父目录是否存在
					{
						fs.create(fs.makeQualified(new Path(maxAdIdDir + "/" + currMaxAdId)));
					}
					else
					{
						boolean larger = true;
						FileStatus[] files = fs.listStatus(maxAdIdDirPath);
						for (FileStatus file : files)
						{
							int id = Integer.parseInt(file.getPath().getName());
							if (currMaxAdId < id)
							{
								larger = false;
								break;
							}
						}
						if (larger)
						{
							fs.create(fs.makeQualified(new Path(maxAdIdDir + "/" + currMaxAdId)));
						}
					}
				}
				
				if (currMaxQueryId == 0)
					return;
				//用当前最大的QueryId创建一个文件
				if (!fs.exists(maxQureyIdDirPath))	//判断父目录是否存在
				{
					fs.create(fs.makeQualified(new Path(maxQureyIdDir + "/" + currMaxQueryId)));
				}
				else
				{
					boolean larger = true;
					FileStatus[] files = fs.listStatus(maxQureyIdDirPath);
					for (FileStatus file : files)
					{
						int id = Integer.parseInt(file.getPath().getName());
						if (currMaxQueryId < id)
						{
							larger = false;
							break;
						}
					}
					if (larger)
					{
						fs.create(fs.makeQualified(new Path(maxQureyIdDir + "/" + currMaxQueryId)));
					}
				}
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
		}
		
		@Override
		public void configure(JobConf conf)
		{
//			String mapTaskId = conf.get("mapred.task.id");
//			String jobId = mapTaskId.substring(0, mapTaskId.indexOf("_m_"));
//			jobId = "job" + jobId.substring(jobId.indexOf('_'));
//			try
//			{
//				JobClient jobClient = new JobClient(conf);
//				job = jobClient.getJob(JobID.forName(jobId));
//				if (job == null)
//					throw new IOException("No job with ID " + jobId + " found.");
//			}
//			catch (IOException e)
//			{
//				System.err.println(e.getMessage());
//				e.printStackTrace();
//			}
			jobConf = conf;
			String inputPath = conf.get("map.input.file");
			String qasPath = conf.get("WeightMatrixBuilder.inputPathQas");
			maxIdDirPath = conf.get("WeightMatrixBuilder.maxIdDirPath");
			if (inputPath.indexOf(qasPath) >= 0)
				key.type = 0;
			else
				key.type = 1;
			
			if (DEBUG)
			{
				System.out.println(inputPath);
				System.out.println(key.type);
			}
		}
		
		/**
		 * 期待的输入数据的格式：
		 * 1. qas ^A query_id ^A {ad_id ^B click_num}
		 * 2. aqs ^A ad_id ^A {query_id ^B click_num}
		 * { }表示内部的内容可以重复1次或多次，但至少一次
		 */		
		@Override
		public void map(LongWritable lineno, Text val,
				OutputCollector<Key, Value> output, Reporter reporter)
				throws IOException
		{
//			if (task_reporter == null)
//				task_reporter = reporter;
			String line = val.toString();
//			if (DEBUG)
//				printMapInput(lineno, val);

			String[] elements = line.split(Common.CONTROL_A);
			if (elements.length < 3)
				return;
			int id = Integer.parseInt(elements[1]);
			
			//记录最大Query和广告编号
			if (key.type == 0)
			{
				if (currMaxQueryId < id)
					currMaxQueryId = id;
			}
			else if (key.type == 1)
			{
				if (currMaxAdId < id)
					currMaxAdId = id;
			}
			key.index2 = id;
			value.index = id;
			
			//求权值总和
			int valid_num = 0;
			long sum = 0, temp;
			int beginIndex = 0;
			for (int i = 2; i < elements.length; i++)
			{
				beginIndex = elements[i].indexOf(Common.CONTROL_B);
				if (beginIndex < 0)
				{
					throw new IOException("输入数据格式不对. In mapper.");
				}
				temp = Long.parseLong(elements[i].substring(beginIndex + 1));
				if (temp >= threshold)
				{
					sum += temp;
					valid_num++;
				}
			}
			if (valid_num <= 0)
				return;
			
			//求平均权值
			double average = (double)sum / valid_num;
			
			//求normalized_weight，以及实际值与期望值之差平方和
			double square = 0;
			String[] content;
			for (int i = 2; i < elements.length; i++)
			{
				content = elements[i].split(Common.CONTROL_B);				
				double weight = Double.valueOf(content[1]);
				if (weight < threshold)
					continue;
				
				value.val = weight / sum;
				key.index1 = Integer.parseInt(content[0]);
				output.collect(key, value);
//				if (DEBUG)
//					printMapOutput(key, value);
				
				square += (weight - average) * (weight - average);
			}
			
			//求方差
			double variance = square / valid_num;
			key.type = (byte) (1 - key.type);	//标识取反
			key.index1 = id;
			key.index2 = -1;
			value.index = -1;
			value.val = variance;
			output.collect(key, value);
//			if (DEBUG)
//				printMapOutput(key, value);
			key.type = (byte) (1 - key.type);	//还原
		}
	}
	
	private static class JobReducer extends MapReduceBase
 		implements Reducer<Key, Value, IndexPair, DoubleWritable>
	{
		private MultipleOutputs multipleOutputs;
		private IndexPair indexPair = new IndexPair();
		private DoubleWritable val = new DoubleWritable();
		//调式用的函数	
//		private void printReduceInputKey(Key key)
//		{
//			System.out.println("##### Reduce input: key = (" + key.type + "," + key.index1 + ","
//					+ key.index2 + ")");
//		}
//
//		private void printReduceInputValue(Value value)
//		{
//			System.out.println("##### Reduce input: value = (" + value.index
//					+ "," + value.val + ")");
//		}
//
//		private void printReduceOutput()
//		{
//			System.out.println("##### Reduce output: (" + indexPair.index1
//					+ "," + indexPair.index2 + ") " + val.get());
//		}
		
		@Override
		public void configure(JobConf conf)
		{
			multipleOutputs = new MultipleOutputs(conf);
		}
		
		@Override
		public void close() throws IOException
		{
			multipleOutputs.close();
		}
		/**
		 * 求W(q,a)和W(a,q). 公式见SimRank++论文p.10
		 * W(q,a)=spread(a) * normalized_weight(q, a)
		 * W(a,q)=spread(q) * normalized_weight(a, q)
		 * spread(i) = e ^ (-variance(i))
		 */
		@SuppressWarnings("unchecked")
		@Override
		public void reduce(Key key, Iterator<Value> values,
				OutputCollector<IndexPair, DoubleWritable> output, Reporter reporter)
				throws IOException
		{
//			if (DEBUG)
//				printReduceInputKey(key);
			
			OutputCollector<IndexPair, DoubleWritable> collector;
			if (key.type == 0)
				collector = multipleOutputs.getCollector("WQA", "q", reporter);
			else
				collector = multipleOutputs.getCollector("WAQ", "a", reporter);
			
			Value value = values.next();
//			if (DEBUG)
//				printReduceInputValue(value);
			
			if (value.index != -1)
			{
				throw new IOException("输入数据顺序不对. In reducer.");
			}
			double variance = value.val;
			
			double spread = Math.exp(-variance);
			
			while (values.hasNext())
			{
				value = values.next();
//				if (DEBUG)
//					printReduceInputValue(value);
				
				indexPair.index1 = key.index1;		//权值矩阵行坐标
				indexPair.index2 = value.index;		//权值矩阵纵坐标
				val.set(spread * value.val);
				collector.collect(indexPair, val);
//				if (DEBUG)
//					printReduceOutput();
			}
		}

	}
	
	protected static void configJob(JobConf conf)
	{
		conf.setJobName("Weight Matrix Builder");
		conf.setJarByClass(WeightMatrixBuilder.class);	
		conf.setInputFormat(SizeCustomizeTextInputFormat.class);
		conf.setMapperClass(JobMapper.class);
		conf.setReducerClass(JobReducer.class);
		conf.setPartitionerClass(JobPartitioner.class);		
		conf.setMapOutputKeyClass(Key.class);
		conf.setMapOutputValueClass(Value.class);
		conf.setOutputValueGroupingComparator(GroupComparator.class);
		conf.setOutputKeyClass(IndexPair.class);
		conf.setOutputValueClass(DoubleWritable.class);
		conf.setOutputFormat(NullOutputFormat.class);	//suppress empty part file
		MultipleOutputs.addMultiNamedOutput(conf, "WQA", SequenceFileOutputFormat.class, IndexPair.class, DoubleWritable.class);
		MultipleOutputs.addMultiNamedOutput(conf, "WAQ", SequenceFileOutputFormat.class, IndexPair.class, DoubleWritable.class);
		//conf.setOutputFormat(ReportOutFormat.class);
		conf.set("mapred.child.java.opts", "-Xmx3000m");
		//map-side优化
		conf.setLong("mapred.max.split.size", 16 * 1024 * 1024);//设置最大Split长度16M
		//conf.set("io.sort.record.percent", "0.06");
		conf.setInt("io.sort.mb", 1024);
		conf.setInt("io.sort.factor", 100);
		//对map任务输出进行压缩
		conf.setCompressMapOutput(true);
		conf.setMapOutputCompressorClass(GzipCodec.class);
		//reduce-side优化
		conf.setInt("mapred.reduce.parallel.copies", 10);//Reduce端从Map端并行拷贝数据的并行线程数
		conf.setInt("mapred.inmen.merge.threshold", 0);	//不通过阈值来控制是否启动Merge & Spill
		conf.set("mapred.job.shuffle.input.buffer.percent", "0.80");//map输出缓冲区占整个堆空间的百分比
		conf.set("mapred.job.shuffle.merge.percent", "0.7");//map输出缓冲区中的数据到达指定百分比时开始启动Merge & Spill
		//对输出进行压缩, BZip2Codec的结果是splitable的
//		conf.setBoolean("mapred.output.compress", true);
//		conf.setClass("mapred.output.compression.codec", BZip2Codec.class, CompressionCodec.class);
//		SequenceFileOutputFormat.setOutputCompressionType(conf, SequenceFile.CompressionType.BLOCK);
//		SequenceFileOutputFormat.setOutputCompressorClass(conf, BZip2Codec.class);
	}
	
	public static boolean runJob(String inputPathQas, String inputPathAqs, String outputDirPath, String maxIdDirPath, int R) throws Exception
	{
		System.out.print("************************************************* <building weight matrix> ");
		System.out.println("*************************************************");
		if (inputPathQas == null || inputPathQas.length() == 0)
			throw new Exception("inputPathQas is null or empty");
		if (inputPathAqs == null || inputPathAqs.length() == 0)
			throw new Exception("inputPathAqs is null or empty");
		if (outputDirPath == null || outputDirPath.length() == 0)
			throw new Exception("outputDirPath is null or empty");		
		
		JobConf job = new JobConf();
		FileSystem fs = FileSystem.get(job);
		inputPathQas = fs.makeQualified(new Path(inputPathQas)).toString();
		inputPathAqs = fs.makeQualified(new Path(inputPathAqs)).toString();
		outputDirPath = fs.makeQualified(new Path(outputDirPath)).toString();
		job.set("WeightMatrixBuilder.inputPathQas", inputPathQas);
		job.set("WeightMatrixBuilder.maxIdDirPath", maxIdDirPath);
		FileInputFormat.addInputPath(job, new Path(inputPathQas));
		FileInputFormat.addInputPath(job, new Path(inputPathAqs));
		FileOutputFormat.setOutputPath(job,	new Path(outputDirPath));
		job.setNumReduceTasks(R);
		
		fs.delete(new Path(outputDirPath), true);
		fs.delete(new Path(maxIdDirPath), true);
		
		try
		{
			configJob(job);
			RunningJob runningJob = JobClient.runJob(job);
			//jobID = runningJob.getID();
			return runningJob.isSuccessful();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		return false;
	}
}
