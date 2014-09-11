package com.taobao.research.jobs.simrank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.mapred.join.TupleWritable;

import com.taobao.research.jobs.common.IndexValueWritable;
import com.taobao.research.jobs.common.IndexPair;
import com.taobao.research.jobs.common.Join;

/**
 * 计算最终的SimRank相似性分数
 * @author yangxudong.pt
 * @deprecated
 */
public class GetFinalScore
{
	private static double defEvalue = 0.5;	//默认的evidence值
	
	public static void setDefaultEvidenceValue(double defVal)
	{
		defEvalue = defVal;
	}
	
	public static boolean runJoin(String evidencePath, String qqScorePath, String outputPath, int R) throws Exception
	{
		if (evidencePath == null || evidencePath.isEmpty())
			throw new Exception("evidencePath is null or empty");
		if (qqScorePath == null || qqScorePath.isEmpty())
			throw new Exception("qqScorePath is null or empty");
		if (outputPath == null || outputPath.isEmpty())
			throw new Exception("outputPath is null or empty");
		
		FileSystem fs = FileSystem.get(new Configuration());
		evidencePath = fs.makeQualified(new Path(evidencePath)).toString();
		qqScorePath = fs.makeQualified(new Path(qqScorePath)).toString();
		outputPath = fs.makeQualified(new Path(outputPath)).toString();
		fs.delete(new Path(outputPath), true);
		
		///做外连接，把相似性分数和证据矩阵的分数join在一起
		System.out.print("------------------------------------------------- <joining data (3)> ");
		System.out.println("---------------------------------------------");
		String[] joinArgs = {"-r", String.valueOf(R), "-outKey", IndexPair.class.getName(),
				"-joinOp", "outer", qqScorePath, evidencePath, outputPath};
		Join.main(joinArgs);
		return true;
	}
	
	private static class RowValue implements WritableComparable<RowValue>
	{
		public int rowIndex;
		public double score;
		@Override
		public void readFields(DataInput in) throws IOException
		{
			rowIndex = in.readInt();
			score = in.readDouble();
		}
		@Override
		public void write(DataOutput out) throws IOException
		{
			out.writeInt(rowIndex);
			out.writeDouble(score);
		}
		
		@Override
		public int compareTo(RowValue o)
		{
			if (rowIndex != o.rowIndex)
				return rowIndex < o.rowIndex ? -1 : 1;
			if (score != o.score)
				return score < o.score ? 1 : -1;	//按分数降序排列
			return 0;
		}
		/** A Comparator that compares serialized CompositeKey. */
		public static class Comparator extends WritableComparator
		{
			public Comparator()
			{
				super(RowValue.class);
			}

			public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)					
			{
				int r1 = readInt(b1, s1);
				int r2 = readInt(b2, s2);
				if (r1 != r2)
					return r1 < r2 ? -1 : 1;
				
				int len = Integer.SIZE / 8;
				double sc1 = readDouble(b1, s1 + len);
				double sc2 = readDouble(b2, s2 + len);
				if (sc1 == sc2)
					return 0;
				
				return sc1 < sc2 ? 1 : -1;	//按分数降序排列
			}

			static
			{ // register this comparator
				WritableComparator.define(RowValue.class, new Comparator());
			}
		}
	}
	
	private static class JobPartitioner implements Partitioner<RowValue, IndexValueWritable>
	{
		public int getPartition(RowValue key, IndexValueWritable value, int numPartitions)
		{
			int rowIndex = key.rowIndex;
			return (rowIndex & Integer.MAX_VALUE) % numPartitions;
		}

		@Override
		public void configure(JobConf arg0)
		{
		}
	}
	
	//按照组合键的第一个元素（原始键）分组，为了方便对用一个（原始）键对应的值排序
	private static class GroupComparator implements RawComparator<RowValue>
	{		
		@Override
	    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			int len = Integer.SIZE / 8;
			return WritableComparator.compareBytes(b1, s1, len, b2, s2, len);
	    }		

		@Override
		public int compare(RowValue rv1, RowValue rv2)
		{
			if (rv1.rowIndex == rv2.rowIndex)
				return 0;
			return rv1.rowIndex < rv2.rowIndex ? -1 : 1;
		}
	}
	
	private static class GetScoreMapper extends MapReduceBase
		implements Mapper<IndexPair, TupleWritable, RowValue, IndexValueWritable>
	{
		private IndexValueWritable value = new IndexValueWritable();
		private RowValue key = new RowValue();
		
		@Override
		public void map(IndexPair inIndex, TupleWritable tuple,
				OutputCollector<RowValue, IndexValueWritable> output, Reporter reporter)
				throws IOException
		{
			if (!tuple.has(0))
			{
				System.err.println("Error: exist evidence score but no simrank score");
				return;
			}

			DoubleWritable score = (DoubleWritable) tuple.get(0);
			value.index = inIndex.index2;

			if (tuple.has(1))
				value.score = score.get() * ((DoubleWritable)tuple.get(1)).get();
			else
				value.score = score.get() * defEvalue;
			
			key.rowIndex = inIndex.index1;
			key.score = value.score;
			output.collect(key, value);
		}
		
	}
	
	private static class NormScoreReducer extends MapReduceBase implements
		Reducer<RowValue, IndexValueWritable, IntWritable, IndexValueWritable>
	{
		private IntWritable key = new IntWritable();	//行号
		private IndexValueWritable value = new IndexValueWritable();
		private double maxScore;
		private IndexValueWritable curValue;
		
		private void printReduceOut(IntWritable key, IndexValueWritable value)
		{
			System.out.println(key.get() + "," + value.index + "-->" + value.score);
		}
		
		/**
		 * 归一化同一Query的所有相似性Queries
		 * max score --> 1, other score --> other score / max score
		 * @param colValues: 已经倒序排列的相似Queries的scores
		 */
		@Override
		public void reduce(RowValue rowValue, Iterator<IndexValueWritable> colValues,
				OutputCollector<IntWritable, IndexValueWritable> output, Reporter reporter)
				throws IOException
		{
			if (colValues.hasNext())
			{
				key.set(rowValue.rowIndex);
				curValue = colValues.next();
				value.index = curValue.index;
				value.score = 1.0d;
				maxScore = curValue.score;
				output.collect(key, value);
				//printReduceOut(key, value);
			}
			
			while (colValues.hasNext())
			{
				curValue = colValues.next();
				value.index = curValue.index;
				value.score = curValue.score / maxScore;
				output.collect(key, value);
				//printReduceOut(key, value);
			}
		}	
	}
	
	protected static void configJob(JobConf conf)
	{
		conf.setJobName("Get Final Score");
		conf.setJarByClass(GetFinalScore.class);
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		conf.setPartitionerClass(JobPartitioner.class);
		conf.setMapperClass(GetScoreMapper.class);
		conf.setReducerClass(NormScoreReducer.class);
		conf.setMapOutputKeyClass(RowValue.class);
		conf.setMapOutputValueClass(IndexValueWritable.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(IndexValueWritable.class);
		conf.setOutputValueGroupingComparator(GroupComparator.class);
		conf.set("mapred.child.java.opts", "-Xmx3072m");
		conf.setInt("io.sort.mb", 1024);
		conf.setInt("io.sort.factor", 100);
		//对输出进行压缩
//		conf.setBoolean("mapred.output.compress", true);
//		conf.setClass("mapred.output.compression.codec", GzipCodec.class, CompressionCodec.class);
//		SequenceFileOutputFormat.setOutputCompressionType(conf, SequenceFile.CompressionType.BLOCK);
//		SequenceFileOutputFormat.setOutputCompressorClass(conf, GzipCodec.class);
	}
	
	public static boolean runJob(String evidencePath, String qqScorePath, String outputPath, String workDirPath, int R) throws Exception
	{
		System.out.print("************************************************* <computing final similarity scores> ");
		System.out.println("*************************************************");

		if (evidencePath == null || evidencePath.isEmpty())
			throw new Exception("evidencePath is null or empty");
		if (qqScorePath == null || qqScorePath.isEmpty())
			throw new Exception("qqScorePath is null or empty");
		if (outputPath == null || outputPath.isEmpty())
			throw new Exception("outputPath is null or empty");
		
		String joinedScores = workDirPath + "/joinedScores";
		if (!runJoin(evidencePath, qqScorePath, joinedScores, R))
			return false;
		
		JobConf job = new JobConf();
		FileSystem fs = FileSystem.get(job);
		outputPath = fs.makeQualified(new Path(outputPath)).toString();

		FileInputFormat.addInputPath(job, new Path(joinedScores));
		FileOutputFormat.setOutputPath(job,	new Path(outputPath));
		
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
		
		runJob(INPUT_PATH_EVIDENCE, INPUT_PATH_SCORE, OUTPUT_PATH_DIR, WORK_DIR, 100);
	}
}
