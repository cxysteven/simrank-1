package com.taobao.research.jobs.simrank;
/**
 * 
 * @author yangxudong.pt
 * @deprecated
 */
public class SimRankIter implements Runnable
{
	private boolean computingQ2Q;
	private SimRankDriver driver;

	public SimRankIter(boolean computingQ2Q, SimRankDriver d)
	{
		driver = d;
		this.computingQ2Q = computingQ2Q;
	}

	
	public void run()
	{
		try
		{
			if (computingQ2Q)
			{
				driver.computeQ2Q();
			}
			else
			{
				driver.computeA2A();
			}
			//driver.barrier_await();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
}
