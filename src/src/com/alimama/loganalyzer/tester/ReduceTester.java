package com.alimama.loganalyzer.tester;


import org.jmock.*;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import java.util.*;
import org.apache.hadoop.mapred.JobConf;
public class ReduceTester<K1,V1,K2,V2>  {

	public  void test(Class reduceClass,K1 inputKey,V1[] inputValues,K2[] outputKeys,V2[] outputValues) throws Exception {
		test(reduceClass,new JobConf(),inputKey,inputValues,outputKeys,outputValues);
	}
 
	public  void test(Class reduceClass,JobConf jobConf,K1 inputKey,V1[] inputValues,K2[] outputKeys,V2[] outputValues) throws Exception {
		Mockery ctx=new Mockery();
		final K2[] finalOutputKeys=outputKeys;
		final V2[] finalOutputValues=outputValues;
		final OutputCollector<K2, V2> output=ctx.mock(OutputCollector.class);
		ctx.checking(new Expectations() {{
			for (int i=0;i<finalOutputKeys.length;i++) {
				try {
					one (output).collect(finalOutputKeys[i],finalOutputValues[i]);
				}
				catch (Exception e){
					e.printStackTrace();
					throw new RuntimeException(e);
				}
			}
		}});
		org.apache.hadoop.mapred.Reducer<K1,V1,K2,V2> reducer=(Reducer<K1,V1,K2,V2>)reduceClass.newInstance();
		reducer.configure(jobConf);
		reducer.reduce(inputKey,Arrays.asList(inputValues).iterator(),output,null);
        ctx.assertIsSatisfied();
	}
   
}