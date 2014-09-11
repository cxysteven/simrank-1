package com.alimama.loganalyzer.tester;

import org.jmock.*;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.JobConf;
public class MapTester<K1,V1,K2,V2>  {
	
	public  void test(Class mapClass,K1 inputKey,V1 inputValue,K2[] outputKeys,V2[] outputValues) throws Exception {
	    test(mapClass,new JobConf(),inputKey,inputValue,outputKeys,outputValues);
	}
	public  void test(Class mapClass,JobConf jobConf,K1 inputKey,V1 inputValue,K2[] outputKeys,V2[] outputValues) throws Exception {
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
					throw new RuntimeException(e);
				}
			}
		}});
		org.apache.hadoop.mapred.Mapper<K1,V1,K2,V2> mapper=(Mapper<K1,V1,K2,V2>)mapClass.newInstance();
        mapper.configure(jobConf);
		mapper.map(inputKey,inputValue,output,null);
        ctx.assertIsSatisfied();
	}

}
