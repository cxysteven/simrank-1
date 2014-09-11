package com.taobao.research.jobs.simrank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;

import com.taobao.research.jobs.common.Common;

/**
 * 求有共同点击广告的两两Query
 * @author yangxudong.pt
 * @create_time 2011-9-15
 * @deprecated
 */
public class GetQueryPair
{
	public static class QueryPairWritable implements WritableComparable<QueryPairWritable>
	{
		public int q1;
		public int q2;
		@Override
		public void readFields(DataInput in) throws IOException
		{
			q1 = in.readInt();
			q2 = in.readInt();
		}
		@Override
		public void write(DataOutput out) throws IOException
		{
			out.writeInt(q1);
			out.writeInt(q2);
		}
		@Override
		public int compareTo(QueryPairWritable o)
		{
			if (q1 != o.q1)
				return q1 < o.q1 ? -1 : 1;
			if (q2 != o.q2)
				return q2 < o.q2 ? -1 : 1;
			return 0;
		}
		
		/** A Comparator that compares serialized CompositeKey. */
		public static class Comparator extends WritableComparator
		{
			public Comparator()
			{
				super(QueryPairWritable.class);
			}

			public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)					
			{
				return compareBytes(b1, s1, l1, b2, s2, l2);
			}

			static
			{ // register this comparator
				WritableComparator.define(QueryPairWritable.class, new Comparator());
			}
		}
	}
	
	//按照组合键的第一个元素（原始键）分组，为了方便对用一个（原始）键对应的值排序
	private static class GroupComparator implements RawComparator<QueryPairWritable>
	{		
		@Override
	    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			int len = Integer.SIZE / 8;
			return WritableComparator.compareBytes(b1, s1, len, b2, s2, len);
	    }		

		@Override
		public int compare(QueryPairWritable p1, QueryPairWritable p2)
		{
			int k1 = p1.q1;
			int k2 = p2.q1;
			if (k1 == k2)
				return 0;
			return k1 < k2 ? -1 : 1;
		}
	}
	
	/**
	 * 求有共同点击广告的两两Query
	 * 实现辅助排序，以便Reduce端的去重
	 * @author yangxudong.pt
	 *
	 */
	private static class QueryPairMapper extends MapReduceBase
 		implements Mapper<LongWritable, Text, QueryPairWritable, IntWritable>
	{
		private Set<Integer> queries = new TreeSet<Integer>();
		private QueryPairWritable key = new QueryPairWritable();
		private IntWritable value = new IntWritable();
		
		/**
		 * 输入格式：aqs ^A aid ^A {qid ^B click_num}
		 * 输出内容：qid1,qid2 (二进制）
		 */
		@Override
		public void map(LongWritable lineOffset, Text txtLine,
				OutputCollector<QueryPairWritable, IntWritable> output, Reporter reporter)
				throws IOException
		{
			String strLine = txtLine.toString();
			String[] elements = strLine.split(Common.CONTROL_A);

			if (elements.length <= 2)
				return;
			queries.clear();
			
			String temp;
			for (int i = 2; i < elements.length; i++)
			{
				temp = elements[i].substring(0, elements[i].indexOf(Common.CONTROL_B));
				queries.add(Integer.valueOf(temp));
			}
			
			Integer[] qs = queries.toArray(new Integer[0]);
			int length = queries.size();
			for (int i = 0; i < length - 1; ++i)
			{
				key.q1 = qs[i];
				for (int j = i + 1; j < length; ++j)
				{
					key.q2 = qs[j];
					value.set(qs[j]);
					output.collect(key, value);
				}
			}
		}	
	}

	private static class QueryPairReducer extends MapReduceBase implements
			Reducer<QueryPairWritable, IntWritable, IntWritable, IntWritable>
	{
		private IntWritable lastValue = new IntWritable(-1);
		private IntWritable key = new IntWritable();

		@Override
		public void reduce(QueryPairWritable pair,
				Iterator<IntWritable> values,
				OutputCollector<IntWritable, IntWritable> output, Reporter arg3)
				throws IOException
		{
			key.set(pair.q1);
			IntWritable currentValue;
			while (values.hasNext())
			{
				currentValue = values.next();
				if (!currentValue.equals(lastValue))
				{
					output.collect(key, currentValue);
					lastValue.set(currentValue.get());
				}
			}
		}
	}
	
	private static class QueryPairCombiner extends MapReduceBase implements
	Reducer<QueryPairWritable, IntWritable, QueryPairWritable, IntWritable>
	{
		private IntWritable lastValue = new IntWritable(-1);
		private QueryPairWritable pair = new QueryPairWritable();
		
		@Override
		public void reduce(QueryPairWritable inPair,
				Iterator<IntWritable> values,
				OutputCollector<QueryPairWritable, IntWritable> output, Reporter arg3)
				throws IOException
		{
			pair.q1 = inPair.q1;
			IntWritable currentValue;
			while (values.hasNext())
			{
				currentValue = values.next();
				if (!currentValue.equals(lastValue))
				{
					pair.q2 = currentValue.get();
					output.collect(pair, currentValue);
					lastValue.set(currentValue.get());
				}
			}
		}
	}
	
	protected static void configJob(JobConf conf)
	{
		conf.setJobName("Get Query Pair");
		conf.setJarByClass(GetQueryPair.class);	
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		conf.setMapperClass(QueryPairMapper.class);
		conf.setReducerClass(QueryPairReducer.class);
		conf.setCombinerClass(QueryPairCombiner.class);
		conf.setMapOutputKeyClass(QueryPairWritable.class);
		conf.setMapOutputValueClass(IntWritable.class);
		conf.setOutputValueGroupingComparator(GroupComparator.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.set("mapred.child.java.opts", "-Xmx3072m");
		conf.setInt("io.sort.mb", 1024);
		conf.setInt("io.sort.factor", 100);
		//对输出进行压缩
		conf.setBoolean("mapred.output.compress", true);
		conf.setClass("mapred.output.compression.codec", GzipCodec.class, CompressionCodec.class);
		SequenceFileOutputFormat.setOutputCompressionType(conf, SequenceFile.CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputCompressorClass(conf, GzipCodec.class);
	}
	
	public static boolean runJob(String inputPathAqs, String outputDirPath, int R) throws Exception
	{
		System.out.print("************************************************* <building query pairs> ");
		System.out.println("*************************************************");

		if (inputPathAqs == null || inputPathAqs.length() == 0)
			throw new Exception("inputPathAqs is null or empty");
		if (outputDirPath == null || outputDirPath.length() == 0)
			throw new Exception("outputDirPath is null or empty");		
		
		JobConf job = new JobConf();
		FileSystem fs = FileSystem.get(job);
		inputPathAqs = fs.makeQualified(new Path(inputPathAqs)).toString();
		outputDirPath = fs.makeQualified(new Path(outputDirPath)).toString();

		FileInputFormat.addInputPath(job, new Path(inputPathAqs));
		FileOutputFormat.setOutputPath(job,	new Path(outputDirPath));
		
		job.setNumReduceTasks(R);
		fs.delete(new Path(outputDirPath), true);	
		configJob(job);
		RunningJob runningJob = JobClient.runJob(job);
		return runningJob.isSuccessful();
	}
}
