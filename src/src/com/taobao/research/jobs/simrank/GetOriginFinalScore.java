package com.taobao.research.jobs.simrank;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;

import com.taobao.research.jobs.common.Common;

/**
 * 得到最终的结果
 * @author yangxudong.pt
 *
 */
public class GetOriginFinalScore
{
//	public static class JobMapper extends MapReduceBase implements
//		Mapper<IntWritable, Text, IntWritable, Text>
//	{
//
//		/**
//		 * 输入格式：qQ ^A qid ^A query text
//		 */
//		@Override
//		public void map(IntWritable arg0, Text arg1,
//				OutputCollector<IntWritable, Text> arg2, Reporter arg3)
//				throws IOException
//		{
//			// TODO Auto-generated method stub
//			
//		}
//		
//	}

	public static class JobReducer extends MapReduceBase implements
		Reducer<IntWritable, Text, Text, NullWritable>
	{
		private Text finalResult = new Text();
		private StringBuilder str = new StringBuilder(5120);
		
		@Override
		public void reduce(IntWritable qid, Iterator<Text> values,
				OutputCollector<Text, NullWritable> output, Reporter reporter)
				throws IOException
		{
			str.delete(0, str.length());	//清空
			
			str.append(values.next().toString());
			if (values.hasNext()) //可能有些query和其他所有的query都不相关
			{
				if (str.toString().startsWith(Common.CONTROL_A))
					str.insert(0, values.next().toString());
				else
					str.append(values.next().toString());
				
				finalResult.set(str.toString());
				output.collect(finalResult, NullWritable.get());
			}
			
			if (values.hasNext())
			{
				System.err.println("Error: two many values");
			}
		}
	}
	
	protected static void configJob(JobConf conf)
	{
		conf.setJobName("get origin final scores");
		conf.setJarByClass(GetOriginFinalScore.class);
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setMapperClass(IdentityMapper.class);
		conf.setReducerClass(JobReducer.class);
		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(NullWritable.class);
		conf.set("mapred.child.java.opts", "-Xmx3000m");
		conf.setInt("io.sort.mb", 1024);
		conf.setInt("io.sort.factor", 100);
	}
	
	public static boolean runJob(String inputPath_QueryText, String inputPath_AggScore, String outputPath, int R) throws Exception
	{
		System.out.print("********************************************* <getting origin final scores> ");
		System.out.println("**********************************************");

		if (inputPath_QueryText == null || inputPath_QueryText.isEmpty())
			throw new Exception("inputPath_qQ is null or empty");
		if (inputPath_AggScore == null || inputPath_AggScore.isEmpty())
			throw new Exception("inputPath_AggScore is null or empty");
		if (outputPath == null || outputPath.isEmpty())
			throw new Exception("outputPath is null or empty");
		
		System.out.println("### inputPath_qQ: " + inputPath_QueryText);
		System.out.println("### inputPathNQ: " + inputPath_AggScore);
		System.out.println("### outputDirPath: " + outputPath);
		System.out.println("### Reduce num: " + R);
		
		JobConf job = new JobConf();
		FileSystem fs = FileSystem.get(job);
		outputPath = fs.makeQualified(new Path(outputPath)).toString();

		FileInputFormat.addInputPath(job, new Path(inputPath_QueryText));
		FileInputFormat.addInputPath(job, new Path(inputPath_AggScore));
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
		String INPUT_PATH_TEXT = WORK_DIR + "/queryText";
		String INPUT_PATH_SCORE = WORK_DIR + "/AggregatedScore";
		String OUTPUT_PATH_DIR = WORK_DIR + "/FinalScoreOrigin";
		
		runJob(INPUT_PATH_TEXT, INPUT_PATH_SCORE, OUTPUT_PATH_DIR, 400);
	}
	
}
