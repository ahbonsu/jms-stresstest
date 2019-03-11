package com.avintis.jms.stresstest;

import java.util.ArrayList;

import javax.jms.JMSException;

public class JMSTester
{
	
	private static final int MAX_PRODUCER = 100;
	private static final int MAX_CONSUMER = 10;
	private static final int PRODUCTION_FREQUENCY = 100;
	private static final int MAX_MESSAGE_SIZE = 1000*1024;
	
	private static ArrayList<Thread> producers = new ArrayList<Thread>();
	private static ArrayList<Thread> consumers = new ArrayList<Thread>();

	public static void main(String[] args) throws JMSException, InterruptedException
	{
		String brokerUrl = "tcp://localhost:61616";
		String queue = "testQueue";

		//create producers
		for(int i = 0; i < MAX_PRODUCER; i++)
		{
			Thread t = new Thread(new JMSProducer(brokerUrl, MAX_MESSAGE_SIZE, queue, PRODUCTION_FREQUENCY, true, false));
			Thread.sleep(PRODUCTION_FREQUENCY / MAX_PRODUCER);
			t.start();
			producers.add(t);
		}
		//create consumers
		for(int i = 0; i < MAX_CONSUMER; i++)
		{
			Thread t = new Thread(new JMSConsumer(brokerUrl, queue, false));
			t.start();
			consumers.add(t);
		}

	}

}