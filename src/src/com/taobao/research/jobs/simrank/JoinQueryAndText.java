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
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
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
import org.apache.hadoop.mapred.lib.MultipleInputs;
import com.taobao.research.jobs.common.GenericObject;
import com.taobao.research.jobs.common.IndexValueWritable;

/**
 * 输入1：qid --> query text
 * 输入2：qid1 --> qid2, s(1,2)
 * 输出：<qid, s> --> query text
 * 通过Key进行Join
 * @author yangxudong.pt
 *
 */
public class JoinQueryAndText
{
	private static class QueryOrder implements WritableComparable<QueryOrder>
	{
		public int queryId;
		public byte order;
		
		@Override
		public void readFields(DataInput in) throws IOException
		{
			queryId = in.readInt();
			order = in.readByte();
		}
		@Override
		public void write(DataOutput out) throws IOException
		{
			out.writeInt(queryId);
			out.writeByte(order);			
		}
		@Override
		public int compareTo(QueryOrder o)
		{
			if (queryId != o.queryId)
				return queryId < o.queryId ? -1 : 1;
			if (order != o.order)
				return order < o.order ? -1 : 1;
			return 0;
		}
		
		@SuppressWarnings("unused")
		public static class Comparator extends WritableComparator
		{
			public Comparator()
			{
				super(QueryOrder.class);
			}

			public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)					
			{
				int q1 = readInt(b1, s1);
				int q2 = readInt(b2, s2);
				if (q1 != q2)
					return q1 < q2 ? -1 : 1;
				int len = Integer.SIZE / 8;
				byte o1 = b1[s1 + len];
				byte o2 = b2[s2 + len];
				if (o1 == o2)
					return 0;
				return o1 < o2 ? -1 : 1;
				//return compare(b1, s1, l1, b2, s2, l2);
			}

			static
			{ // register this comparator
				WritableComparator.define(QueryOrder.class, new Comparator());
			}
		}
	}
	
	//按照组合键的第一个元素（原始键）分组，为了方便对用一个（原始）键对应的值排序
	private static class GroupComparator implements RawComparator<QueryOrder>
	{		
		@Override
	    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			int len = Integer.SIZE / 8;
			return WritableComparator.compareBytes(b1, s1, len, b2, s2, len);
	    }		

		@Override
		public int compare(QueryOrder q1, QueryOrder q2)
		{
			if (q1.queryId == q2.queryId)
				return 0;
			return q1.queryId < q2.queryId ? -1 : 1;
		}
	}
	
	private static class JobPartitioner implements Partitioner<QueryOrder, GenericObject>
	{
		public int getPartition(QueryOrder key, GenericObject value, int numPartitions)
		{
			return (key.queryId & Integer.MAX_VALUE) % numPartitions;
		}

		@Override
		public void configure(JobConf arg0)
		{}
	}
	
	/**
	 * 为Reduce端的Join做准备,发送相似性分数
	 */
	public static class JoinScoreMapper extends MapReduceBase implements
			Mapper<IntWritable, IndexValueWritable, QueryOrder, GenericObject>
	{
		private QueryOrder queryQrder = new QueryOrder();
		private GenericObject value = new GenericObject();
		private IndexValueWritable query_score = new IndexValueWritable();
		@Override
		public void map(IntWritable qid, IndexValueWritable q_score,
				OutputCollector<QueryOrder, GenericObject> output,
				Reporter reporter) throws IOException
		{
			queryQrder.queryId = qid.get();
			//跳过对角线上的元素，自己和自己的相似性始终为1，不需要输出
			if (queryQrder.queryId == q_score.index)
				return;
			
			queryQrder.order = 2;	//同一个query有多个值，因此排后面
			value.set(q_score);
			output.collect(queryQrder, value);
			
			//emit对称矩阵的另一半（上三角阵）
			queryQrder.queryId = q_score.index;
			query_score.score = q_score.score;
			query_score.index = qid.get();
			value.set(query_score);
			output.collect(queryQrder, value);		
		}
	}
	
	/**
	 * 为Reduce端的Join做准备,发送Query Text
	 */
	public static class JoinTextMapper extends MapReduceBase implements
			Mapper<IntWritable, Text, QueryOrder, GenericObject>
	{
		private QueryOrder queryOrder = new QueryOrder();
		private GenericObject queryText = new GenericObject();
		
		@Override
		public void map(IntWritable qid, Text q_text,
				OutputCollector<QueryOrder, GenericObject> output,
				Reporter reporter) throws IOException
		{
			queryOrder.queryId = qid.get();
			queryOrder.order = 1;	//文本排前面
			queryText.set(q_text);
			output.collect(queryOrder, queryText);
		}
	}
	
//	public static class JoinRawTextMapper extends MapReduceBase implements
//		Mapper<LongWritable, Text, QueryOrder, GenericObject>
//	{
//		private QueryOrder queryOrder = new QueryOrder();
//		private GenericObject queryText = new GenericObject();
//		private Text q_text = new Text();
//		/**
//		 * 输入格式：qQ ^A qid ^A query text
//		 */
//		@Override
//		public void map(LongWritable lineOffset, Text txtLine,
//				OutputCollector<QueryOrder, GenericObject> output, Reporter reporter)
//				throws IOException
//		{
//			String strLine = txtLine.toString();
//			String[] elements = strLine.split(Common.CONTROL_A);
//			queryOrder.queryId = Integer.parseInt(elements[1]);
//			queryOrder.order = 0;
//			q_text.set(elements[2]);
//			queryText.set(q_text);
//			output.collect(queryOrder, queryText);
//		}	
//	}
	
	public static class JoinReducer extends MapReduceBase implements
		Reducer<QueryOrder, GenericObject, IndexValueWritable, Text>
	{
		private Text error = new Text("No query text exist.");
		@Override
		public void reduce(QueryOrder query, Iterator<GenericObject> values,
				OutputCollector<IndexValueWritable, Text> output, Reporter reporter)
				throws IOException
		{
			GenericObject object = values.next(); //query text排在前面
			Text queryText = error;
			if (object.get() instanceof Text)
				queryText = new Text((Text) object.get());
			while (values.hasNext())
			{
				output.collect((IndexValueWritable)values.next().get(), queryText);
			}
		}
	}
	
	protected static void configJob(JobConf conf)
	{
		conf.setJobName("Join query and text");
		conf.setJarByClass(JoinQueryAndText.class);
		conf.setReducerClass(JoinReducer.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		conf.setPartitionerClass(JobPartitioner.class);
		conf.setOutputValueGroupingComparator(GroupComparator.class);
		conf.setMapOutputKeyClass(QueryOrder.class);
		conf.setMapOutputValueClass(GenericObject.class);
		conf.setOutputKeyClass(IndexValueWritable.class);
		conf.setOutputValueClass(Text.class);
		conf.set("mapred.child.java.opts", "-Xmx3000m");
		conf.setInt("io.sort.mb", 1024);
		conf.setInt("io.sort.factor", 100);
	}
	
	public static boolean runJob(String finalScorePath, String queryText, String outputPath, int R) throws Exception
	{
		System.out.print("************************************************* <joining query and text> ");
		System.out.println("*********************************************");

		if (finalScorePath == null || finalScorePath.isEmpty())
			throw new Exception("finalScorePath is null or empty");
		if (queryText == null || queryText.isEmpty())
			throw new Exception("queryText is null or empty");
		if (outputPath == null || outputPath.isEmpty())
			throw new Exception("outputPath is null or empty");
		System.out.println("###### qqScorePath: " + finalScorePath);
		System.out.println("###### queryText: " + queryText);
		System.out.println("###### outputPath: " + outputPath);
		System.out.println("###### reduce num: " + R);
		
		JobConf job = new JobConf();
		FileSystem fs = FileSystem.get(job);
		Path qqsPath = new Path(finalScorePath);
		Path qtPath = new Path(queryText);
		Path outPath = new Path(outputPath);

		MultipleInputs.addInputPath(job, qqsPath, SequenceFileInputFormat.class, JoinScoreMapper.class);
		MultipleInputs.addInputPath(job, qtPath, SequenceFileInputFormat.class, JoinTextMapper.class);
		//MultipleInputs.addInputPath(job, qtPath, TextInputFormat.class, JoinRawTextMapper.class);
		FileOutputFormat.setOutputPath(job, outPath);
		job.setNumReduceTasks(R);
		fs.delete(outPath, true);
		configJob(job);
		RunningJob runningJob = JobClient.runJob(job);
		return runningJob.isSuccessful();
	}
	
	public static void main(String[] args) throws Exception
	{
		String WORK_DIR = "/group/tbalgo-dev/zhenghon/simrank";
		String INPUT_PATH_SCORE = WORK_DIR + "/FinalScore";
		String INPUT_PATH_QUERY_TEXT = WORK_DIR + "/queryText";
		//String INPUT_PATH_QUERY_TEXT = "/group/tbalgo-dev/zhouxj/aqr/20110829/qas/qQpart-00000";
		String OUTPUT_PATH_DIR = WORK_DIR + "/QueryScoreText";
		
		runJob(INPUT_PATH_SCORE, INPUT_PATH_QUERY_TEXT, OUTPUT_PATH_DIR, 400);
	}
}
