package com.alimama.loganalyzer.common;

import java.util.*;

public class Launcher
{

	public static void printErrorAndExit(String message)
	{
		System.out.println(message);
		System.out
				.println("Usage : Launcher [ProcessorClass] [InputPath] [OutputPath] [numOfMappers] [numOfReducers][isInputSequenceFile] <name1=value1> <name2=value2>...");
		System.exit(0);
	}

	public static void main(String args[]) throws Exception
	{
        boolean status = false;
		try
		{
			for (int i = 6; i < args.length; i++)
			{
				String arg = args[i];
				System.out.println("arg "+i+":"+arg);
			}
			
			if (args.length < 6)
				printErrorAndExit("Invalid number of arguments");
			Processor processor = null;
			try
			{
				processor = (Processor) (Class.forName(args[0]).newInstance());
			} catch (Exception e)
			{
				printErrorAndExit("Invalid processor " + args[0]);
			}
			for (int i = 0; i < 6; i++)
			{
				String arg = args[i];
				System.out.println("arg "+i+":"+arg);
			}
			Map<String, String> properties = new HashMap<String, String>();
			if (args.length > 6)
				for (int i = 6; i < args.length; i++)
				{
					String arg = args[i];
					System.out.println("arg "+i+":"+arg);
					int p = arg.indexOf("=");
					if (p != -1)
					{
						properties.put(arg.substring(0, p), arg
								.substring(p + 1));
					} else
						printErrorAndExit("Invalid parameter " + arg);
				}
			status = processor.run(args[1], args[2], Integer
					.parseInt(args[3]), Integer.parseInt(args[4]), Boolean
					.parseBoolean(args[5]), properties);

            System.out.println("Alimama_Status : " + status);
		} catch (Exception e)
		{
			e.printStackTrace();
            status = false;
			System.out.println("Alimama_Status :" + status);
		}

        if(status)
        {
            System.exit(0); 
        }
        else
        {
            System.exit(1);
        }
	}
}
