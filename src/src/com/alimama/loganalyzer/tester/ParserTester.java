package com.alimama.loganalyzer.tester;
import java.lang.reflect.*;
import java.util.List;


public class ParserTester<T> {
	
	public void test(Class parserClass,String parseMethod,String line,String[] keys,Object[] values) throws Exception{
		Method method=parserClass.getMethod(parseMethod, new Class[]{String.class});
		T bean=(T)method.invoke(null,line);
		Field[] fields=bean.getClass().getFields();
		for (int i=0;i<keys.length;i++) {
			String key=keys[i];
			Object value=values[i];
			String methodName="get"+key.substring(0,1).toUpperCase()+key.substring(1);
			Method getMethod=bean.getClass().getMethod(methodName, new Class[]{});
			if (getMethod==null)
				throw new RuntimeException("No such method "+methodName+" in "+bean.getClass().getSimpleName());
			Object actualValue=getMethod.invoke(bean, new Object[0]);
			if ((value==null && actualValue!=null) 
					|| (value!=null && !value.equals(actualValue)))
				throw new RuntimeException("Unexpected result: expected = "+value+", actual = "+actualValue);
		}
			
	}
	
	public void testList(Class parserClass,String parseMethod,String line,String[] keys,Object[][] valuesArray) throws Exception{
		Method method=parserClass.getMethod(parseMethod, new Class[]{String.class});
		List<T> beans=(List<T>)method.invoke(null,line);
		for (int k=0;k<valuesArray.length;k++) {
		  T bean=beans.get(k);
		  Field[] fields=bean.getClass().getFields();
		  Object[] values=valuesArray[k];
		  for (int i=0;i<keys.length;i++) {
			String key=keys[i];  
			Object value=values[i];
			String methodName="get"+key.substring(0,1).toUpperCase()+key.substring(1);
			Method getMethod=bean.getClass().getMethod(methodName, new Class[]{});
			if (getMethod==null)
				throw new RuntimeException("No such method "+methodName+" in "+bean.getClass().getSimpleName());
			Object actualValue=getMethod.invoke(bean, new Object[0]);
			if ((value==null && actualValue!=null) 
					|| (value!=null && !value.equals(actualValue)))
				throw new RuntimeException("Unexpected result: list index "+k+", expected = "+value+", actual = "+actualValue);
		  }
		}	
	}

        public void testListToStr(Class parserClass,String parseMethod,String line,String[] expectedValues) throws Exception{
		Method method=parserClass.getMethod(parseMethod, new Class[]{String.class});
		List<T> beans=(List<T>)method.invoke(null,line);
		for (int k=0;k<expectedValues.length;k++) {
		  T bean=beans.get(k);
          String value=null;
          if (bean!=null)
             value=bean.toString(); 
          
          if ((value==null && expectedValues[k]!=null)  || (value!=null && expectedValues[k]==null)
					|| (value!=null && expectedValues[k]!=null && !value.toString().equals(expectedValues[k].toString())))
				throw new RuntimeException("Unexpected to string result: list index "+k+", expected = "+expectedValues[k]+", actual = "+value);
		}	
	}
        public void testIsNull(Class parserClass,String parseMethod,String line) throws Exception{
		Method method=parserClass.getMethod(parseMethod, new Class[]{String.class});
		if (method.invoke(null,line)!=null)
			throw new RuntimeException("Null value expected");
	}

}
