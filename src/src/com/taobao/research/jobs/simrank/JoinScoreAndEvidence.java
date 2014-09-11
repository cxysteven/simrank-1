package com.taobao.research.jobs.simrank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
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
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import com.taobao.research.jobs.common.IndexPair;
import com.taobao.research.jobs.common.IndexValueWritable;

/**
 * 计算最终的SimRank相似性分数
 * @author yangxudong.pt
 */
public class JoinScoreAndEvidence
{
	private static double defEvalue = 0.48;	//默认的evidence值
	
	public static void setDefaultEvidenceValue(double defVal)
	{
		defEvalue = defVal;
	}
	
	private static class Key implements WritableComparable<Key>
	{
		public IndexPair pair;
		public byte order;
		
		public Key()
		{
			pair = new IndexPair();
		}
		
		@Override
		public void readFields(DataInput in) throws IOException
		{
			pair.readFields(in);
			order = in.readByte();
		}
		@Override
		public void write(DataOutput out) throws IOException
		{
			pair.write(out);
			out.writeByte(order);
		}
		@Override
		public int compareTo(Key o)
		{		
			int cmp = pair.compareTo(o.pair);
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
				RawComparator<IndexPair> comparator = WritableComparator.get(IndexPair.class);
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
		@Override
	    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			int len = Byte.SIZE / 8;
			return WritableComparator.compareBytes(b1, s1, l1 - len, b2, s2, l2 - len);
	    }		

		@Override
		public int compare(Key k1, Key k2)
		{
			return k1.pair.compareTo(k2.pair);
		}
	}
	
	private static class JobPartitioner implements Partitioner<Key, DoubleWritable>
	{
		public int getPartition(Key key, DoubleWritable value, int numPartitions)
		{
			IndexPair pair = key.pair;
			return (pair.hashCode() & Integer.MAX_VALUE) % numPartitions;
		}

		@Override
		public void configure(JobConf arg0)
		{}
	}
	
	/**
	 * 为Reduce端的Join做准备
	 */
	public static class JoinMapper extends MapReduceBase implements
			Mapper<IndexPair, DoubleWritable, Key, DoubleWritable>
	{
		private boolean score;
		private Key key = new Key();
		
		@Override
		public void configure(JobConf job)
		{
			String qqscorePath = job.get("qqScorePath");
			String inputPath = job.get("map.input.file");
			score = inputPath.indexOf(qqscorePath) >= 0;
		}

		@Override
		public void map(IndexPair indexPair, DoubleWritable value,
				OutputCollector<Key, DoubleWritable> output, Reporter reporter)
				throws IOException
		{
			//对角线元素始终为1，不需要输出
			if (indexPair.index1 == indexPair.index2)
				return;
			
			key.pair = indexPair;
			key.order = (byte) (score ? 0 : 1);
			output.collect(key, value);
		}		
	}
	
	public static class JoinReducer extends MapReduceBase implements
		Reducer<Key, DoubleWritable, IntWritable, IndexValueWritable>
	{
		private IndexValueWritable colValue = new IndexValueWritable();
		private IntWritable rowIndex = new IntWritable();
		
		@Override
		public void reduce(Key key, Iterator<DoubleWritable> values,
				OutputCollector<IntWritable, IndexValueWritable> output,
				Reporter reporter) throws IOException
		{
			//相似性分数排在前面
			double raw_score = values.next().get();
			if (values.hasNext())
			{
				colValue.score = raw_score * values.next().get();
			}
			else
			{//没有evidence分数用默认值代替
				colValue.score = raw_score * defEvalue;
			}

			rowIndex.set(key.pair.index1);
			colValue.index = key.pair.index2;
			output.collect(rowIndex, colValue);
		}	
	}
	
//	public static class JoinReducer extends MapReduceBase implements
//		Reducer<Key, DoubleWritable, IndexPair, DoubleWritable>
//	{
//		private DoubleWritable score = new DoubleWritable();
//		
//		@Override
//		public void reduce(Key key, Iterator<DoubleWritable> values,
//				OutputCollector<IndexPair, DoubleWritable> output,
//				Reporter reporter) throws IOException
//		{
//			//相似性分数排在前面
//			double raw_score = values.next().get();
//			if (values.hasNext())
//			{
//				score.set(raw_score * values.next().get());
//			}
//			else
//			{//没有evidence分数用默认值代替
//				score.set(raw_score * defEvalue);
//			}
//			
//			output.collect(key.pair, score);
//		}	
//	}
	
	protected static void configJob(JobConf conf)
	{
		conf.setJobName("Get Final Score");
		conf.setJarByClass(JoinScoreAndEvidence.class);
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		conf.setPartitionerClass(JobPartitioner.class);
		conf.setMapperClass(JoinMapper.class);
		conf.setReducerClass(JoinReducer.class);
		conf.setMapOutputKeyClass(Key.class);
		conf.setMapOutputValueClass(DoubleWritable.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(IndexValueWritable.class);
		conf.setOutputValueGroupingComparator(GroupComparator.class);
		conf.set("mapred.child.java.opts", "-Xmx3000m");
		conf.setInt("io.sort.mb", 1024);
		conf.setInt("io.sort.factor", 100);
	}
	
	public static boolean runJob(String evidencePath, String qqScorePath, String outputPath, String workDirPath, int R) throws Exception
	{
		System.out.print("************************************************* <computing final similarity scores> ");
		System.out.println("**********************************************");

		if (evidencePath == null || evidencePath.isEmpty())
			throw new Exception("evidencePath is null or empty");
		if (qqScorePath == null || qqScorePath.isEmpty())
			throw new Exception("qqScorePath is null or empty");
		if (outputPath == null || outputPath.isEmpty())
			throw new Exception("outputPath is null or empty");
		
		System.out.println("###### evidencePath: " + evidencePath);
		System.out.println("###### qqScorePath: " + qqScorePath);
		System.out.println("###### outputPath: " + outputPath);
		System.out.println("###### reduce num: " + R);
		
		JobConf job = new JobConf();
		FileSystem fs = FileSystem.get(job);
		outputPath = fs.makeQualified(new Path(outputPath)).toString();

		FileInputFormat.addInputPath(job, new Path(evidencePath));
		FileInputFormat.addInputPath(job, new Path(qqScorePath));
		FileOutputFormat.setOutputPath(job,	new Path(outputPath));
		job.set("qqScorePath", qqScorePath);
		job.setNumReduceTasks(R);
		fs.delete(new Path(outputPath), true);
		configJob(job);
		RunningJob runningJob = JobClient.runJob(job);
		return runningJob.isSuccessful();
	}
	
	public static void main(String[] args) throws Exception
	{
		String WORK_DIR = "/group/tbalgo-dev/zhenghon/simrank";
		String INPUT_PATH_EVIDENCE = WORK_DIR + "/evidence";
		String INPUT_PATH_SCORE = WORK_DIR + "/S_Q2Q";
		String OUTPUT_PATH_DIR = WORK_DIR + "/FinalScore";
		
		runJob(INPUT_PATH_EVIDENCE, INPUT_PATH_SCORE, OUTPUT_PATH_DIR, WORK_DIR, 400);
	}
}
