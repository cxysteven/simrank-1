package com.alimama.loganalyzer.common;

import java.util.Map;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.GzipCodec;


public abstract class AbstractProcessor implements Processor{
	public boolean run(String inputPath,String outputPath,int numMap,int numReduce,boolean isInputSequenceFile,Map<String,String> properties) throws Exception {
              JobConf conf = new JobConf(this.getClass());
              System.out.println(">>> "+this.getClass().getCanonicalName());

              conf.setJobName(this.getClass().getSimpleName());
              FileInputFormat.setInputPaths(conf, inputPath);
              FileOutputFormat.setOutputPath(conf,new Path(outputPath));
              if (isInputSequenceFile)
                conf.setInputFormat(SequenceFileInputFormat.class);
              else
                conf.setInputFormat(TextInputFormat.class);
              conf.setMapperClass(getMapper());
              conf.setNumMapTasks(numMap);
              conf.setReducerClass(getReducer());
              conf.setNumReduceTasks(numReduce);
              if ("true".equals(conf.get("map.out.compress"))) { 
                 //conf.setMapOutputCompressionType(SequenceFile.CompressionType.BLOCK);
            	 SequenceFileOutputFormat.setOutputCompressionType(conf, SequenceFile.CompressionType.BLOCK);
                 //conf.setMapOutputCompressorClass(GzipCodec.class);     
            	 SequenceFileOutputFormat.setOutputCompressorClass(conf, GzipCodec.class);
              }
              if (conf.get("mapred.max.tracker.failures")!=null) {
                  conf.setMaxTaskFailuresPerTracker(Integer.valueOf(conf.get("mapred.max.tracker.failures")));
              }

              for (String propertyKey : properties.keySet())
                   conf.set(propertyKey,properties.get(propertyKey));
              
              configJob(conf);
              JobClient c=new JobClient(conf);
              RunningJob job=c.runJob(conf);
              return job.isSuccessful();
       }
       protected abstract void configJob(JobConf conf);
       

}
