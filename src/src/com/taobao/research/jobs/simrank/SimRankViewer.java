package com.taobao.research.jobs.simrank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.join.TupleWritable;
import com.taobao.research.jobs.common.IndexValueWritable;
import com.taobao.research.jobs.common.Common;
import com.taobao.research.jobs.common.Join;

/**
 * 得到最终的结果，文本文件
 * @author yangxudong.pt
 * @deprecated
 */
public class SimRankViewer
{
	/**
	 * 把QueryId（key）对应的Query的相似性分数和其文本Join起来
	 * @param queryText：QueryId --> QueryText （Query编号文件的路径）
	 * @param finalScorePath：最终的Query相似性分数文件，qid1 --> qid2, score
	 * @param outputPath: 输出目录
	 * @param R：reduce num
	 * @return 是否Join成功
	 * @throws Exception
	 */
	public static boolean runJoin1(String queryText, String finalScorePath, String outputPath, int R) throws Exception
	{
		if (queryText == null || queryText.isEmpty())
			throw new Exception("queryText is null or empty");
		if (finalScorePath == null || finalScorePath.isEmpty())
			throw new Exception("finalScorePath is null or empty");
		if (outputPath == null || outputPath.isEmpty())
			throw new Exception("outputPath is null or empty");
		
		FileSystem fs = FileSystem.get(new Configuration());
		queryText = fs.makeQualified(new Path(queryText)).toString();
		finalScorePath = fs.makeQualified(new Path(finalScorePath)).toString();
		outputPath = fs.makeQualified(new Path(outputPath)).toString();
		fs.delete(new Path(outputPath), true);
		
		///做外连接，把相似性分数和query的Text join在一起
		System.out.print("-------------------------------------- <joining data (final score and query text)> ");
		System.out.println("---------------------------------------------");
		String[] joinArgs = {"-r", String.valueOf(R), "-outKey", IntWritable.class.getName(),
				"-joinOp", "inner", finalScorePath, queryText, outputPath};
		Join.main(joinArgs);
		return true;
	}
	
	public static boolean runJoin2(String queryText, String simQueryPath, String outputPath, int R) throws Exception
	{
		if (queryText == null || queryText.isEmpty())
			throw new Exception("queryText is null or empty");
		if (simQueryPath == null || simQueryPath.isEmpty())
			throw new Exception("simQueryPath is null or empty");
		if (outputPath == null || outputPath.isEmpty())
			throw new Exception("outputPath is null or empty");
		
		FileSystem fs = FileSystem.get(new Configuration());
		queryText = fs.makeQualified(new Path(queryText)).toString();
		simQueryPath = fs.makeQualified(new Path(simQueryPath)).toString();
		outputPath = fs.makeQualified(new Path(outputPath)).toString();
		fs.delete(new Path(outputPath), true);
		///做外连接，把相似性分数和证据矩阵的分数join在一起
		System.out.print("------------------------------------------------- <joining data (query text and its similar queries)> ");
		System.out.println("---------------------------------------------");
		String[] joinArgs = {"-r", String.valueOf(R), "-outKey", Text.class.getName(),
				"-outFormat", TextOutputFormat.class.getName(),
				"-outValue", Text.class.getName(), queryText, simQueryPath, outputPath};
		Join.main(joinArgs);
		return true;
	}
	
	public static void run(String queryTextPath, String finalScorePath, String outputPath, String workDirPath, int FS_R, int R) throws Exception
	{
		String tempPath = workDirPath + "/simQuery";
		String tempPath2 = workDirPath + "/simQuery2";
		runJoin1(queryTextPath, finalScorePath, tempPath, FS_R);
		runJob(tempPath, tempPath2, FS_R);//得到同一个Query对应的所有Query的文本和相应的相似性分数
		runJoin2(queryTextPath, tempPath2, outputPath, R);//对key（queryId)换成文本
		
		//FileSystem fs = FileSystem.get(new Configuration());
		//fs.delete(new Path(tempPath), true);
		//fs.delete(new Path(tempPath2), true);
	}
	
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
		{
		}
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
	
	/**
	 * 按列聚合Query文本和相似性分数，同一列的属于同一个分区，按相似性分数降序排列
	 * @author yangxudong.pt
	 */
	private static class JobMapper extends MapReduceBase
		implements Mapper<IntWritable, TupleWritable, IndexValueWritable, Value>
	{
		private Value value = new Value();
		
		@Override
		public void map(IntWritable qid, TupleWritable tuple,
				OutputCollector<IndexValueWritable, Value> output, Reporter reporter)
				throws IOException
		{
			IndexValueWritable colValue = (IndexValueWritable) tuple.get(0);
			value.score = colValue.score;
			value.query = (Text) tuple.get(1);
			output.collect(colValue, value);
		}
	}
	
	/**
	 * 把同一列的Query对应的Query文本和相似性分数合并到一行
	 * 输出格式：QueryId --> 与该Query相似的Query文本和对应的相似性分数（降序排序）
	 * @author yangxudong.pt
	 */
	private static class JobReducer extends MapReduceBase implements
		Reducer<IndexValueWritable, Value, IntWritable, Text>
	{
		private Text finalResult = new Text();
		private IntWritable qid = new IntWritable();
		private StringBuilder strRet = new StringBuilder(5120);
		
		@Override
		public void reduce(IndexValueWritable colValue, Iterator<Value> values,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException
		{
			qid.set(colValue.index);
			strRet.delete(0, strRet.length());
			while (values.hasNext())
			{
				Value v = values.next();
				strRet.append(Common.CONTROL_A).append(v.query).append(Common.CONTROL_B).append(v.score);
			}
			finalResult.set(strRet.toString());
			output.collect(qid, finalResult);
		}
	}
	
	protected static void configJob(JobConf conf)
	{
		conf.setJobName("Get final SimRank results");
		conf.setJarByClass(SimRankViewer.class);
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
		System.out.print("************************************************* <merging final results> ");
		System.out.println("*************************************************");

		if (inputPath == null || inputPath.isEmpty())
			throw new Exception("inputPath is null or empty");
		if (outputPath == null || outputPath.isEmpty())
			throw new Exception("outputPath is null or empty");

		
		JobConf job = new JobConf();
		FileSystem fs = FileSystem.get(job);
		inputPath = fs.makeQualified(new Path(inputPath)).toString();
		outputPath = fs.makeQualified(new Path(outputPath)).toString();
		
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job,	new Path(outputPath));
		
		job.setNumReduceTasks(R);
		fs.delete(new Path(outputPath), true);
		configJob(job);
		RunningJob runningJob = JobClient.runJob(job);
		return runningJob.isSuccessful();
	}
}
