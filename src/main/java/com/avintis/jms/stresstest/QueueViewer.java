package com.avintis.jms.stresstest;

public class QueueViewer implements Runnable
{
	
	private JMSTester tester;
	private int interval;
	
	public QueueViewer(JMSTester tester, int interval)
	{
		this.tester = tester;
		this.interval = interval;
	}

	public void run()
	{
		while (true)
		{
			System.out.println("Current Messages in Queue: " + tester.getCurrentMessagesInQueue());
			try
			{
				Thread.sleep(interval);
			} catch (InterruptedException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

}
