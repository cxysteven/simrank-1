package com.taobao.research.jobs.simrank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
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

import com.taobao.research.jobs.common.Common;
import com.taobao.research.jobs.common.IndexValueWritable;

/**
 * 聚合一个Query对应的所有Query，已经与这些Query的相似性分数
 * 输入：<qid, s> --> query text
 * 输出：qid --> query text1, s1; query text2, s2 ......
 * @author yangxudong.pt
 *
 */
public class AggregateScore
{
	private static class Value implements Writable
	{
		public Text query;
		public double score;
		
		public Value()
		{
			query = new Text();
		}
		
		@Override
		public void readFields(DataInput in) throws IOException
		{
			query.readFields(in);
			score = in.readDouble();
		}
		@Override
		public void write(DataOutput out) throws IOException
		{
			query.write(out);
			out.writeDouble(score);			
		}
	}
	
	private static class JobPartitioner implements Partitioner<IndexValueWritable, Value>
	{
		public int getPartition(IndexValueWritable key, Value value, int numPartitions)
		{
			int colIndex = key.index;
			return (colIndex & Integer.MAX_VALUE) % numPartitions;
		}

		@Override
		public void configure(JobConf arg0)
		{}
	}
	
	//按照组合键的第一个元素（原始键）分组，为了方便对用一个（原始）键对应的值排序
	private static class GroupComparator implements RawComparator<IndexValueWritable>
	{		
		@Override
	    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			int len = Integer.SIZE / 8;
			return WritableComparator.compareBytes(b1, s1, len, b2, s2, len);
	    }		

		@Override
		public int compare(IndexValueWritable cv1, IndexValueWritable cv2)
		{
			if (cv1.index == cv2.index)
				return 0;
			return cv1.index < cv2.index ? -1 : 1;
		}
	}
	
	public static class JobMapper extends MapReduceBase implements
		Mapper<IndexValueWritable, Text, IndexValueWritable, Value>
	{
		private Value value = new Value();
		
		@Override
		public void map(IndexValueWritable qs, Text qt,
				OutputCollector<IndexValueWritable, Value> output, Reporter reporter)
				throws IOException
		{
			value.query.set(qt);
			value.score = qs.score;
			output.collect(qs, value);
		}
	}
	
	/**
	 * 归一化同一Query的所有相似性Queries,并聚合为一行
	 * max score --> 1, other score --> other score / max score
	 * @param colValues: 已经倒序排列的相似Queries的scores
	 */
	public static class JobReducer extends MapReduceBase implements
		Reducer<IndexValueWritable, Value, IntWritable, Text>
	{
		private Text finalResult = new Text();
		private IntWritable qid = new IntWritable();
		private StringBuilder strRet = new StringBuilder(5120);
		
		@Override
		public void reduce(IndexValueWritable indexValue, Iterator<Value> values,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException
		{
			double maxScore = 1.0;
			Value curValue;
			qid.set(indexValue.index);
			strRet.delete(0, strRet.length());
			if (values.hasNext())
			{
				curValue = values.next();
				maxScore = curValue.score;
				strRet.append(Common.CONTROL_A).append(curValue.query.toString()).append(Common.CONTROL_B).append("1.0");
			}
			
			double curScore;
			while (values.hasNext())
			{
				curValue = values.next();
				curScore = curValue.score / maxScore;
				strRet.append(Common.CONTROL_A).append(curValue.query.toString()).append(Common.CONTROL_B).append(curScore);
			}
			
			finalResult.set(strRet.toString());
			output.collect(qid, finalResult);
		}
	}
	
	protected static void configJob(JobConf conf)
	{
		conf.setJobName("Aggregate and normlize scores");
		conf.setJarByClass(AggregateScore.class);
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		conf.setPartitionerClass(JobPartitioner.class);
		conf.setMapperClass(JobMapper.class);
		conf.setReducerClass(JobReducer.class);
		conf.setMapOutputKeyClass(IndexValueWritable.class);
		conf.setMapOutputValueClass(Value.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
		conf.setOutputValueGroupingComparator(GroupComparator.class);
		conf.set("mapred.child.java.opts", "-Xmx3000m");
		conf.setInt("io.sort.mb", 1024);
		conf.setInt("io.sort.factor", 100);
	}
	
	public static boolean runJob(String inputPath, String outputPath, int R) throws Exception
	{
		System.out.print("********************************************* <Aggregating and normlizing scores> ");
		System.out.println("**********************************************");

		if (inputPath == null || inputPath.isEmpty())
			throw new Exception("inputPath is null or empty");
		if (outputPath == null || outputPath.isEmpty())
			throw new Exception("outputPath is null or empty");
		System.out.println("###### inputPath: " + inputPath);
		System.out.println("###### outputPath: " + outputPath);
		System.out.println("###### reduce num: " + R);
		
		JobConf job = new JobConf();
		FileSystem fs = FileSystem.get(job);
		outputPath = fs.makeQualified(new Path(outputPath)).toString();

		FileInputFormat.addInputPath(job, new Path(inputPath));
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
		String INPUT_PATH_SCORE = WORK_DIR + "/QueryScoreText";
		String OUTPUT_PATH_DIR = WORK_DIR + "/AggregatedScore";
		
		runJob(INPUT_PATH_SCORE, OUTPUT_PATH_DIR, 400);
	}
}
