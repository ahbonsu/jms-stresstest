package com.avintis.jms.stresstest;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.jms.JMSException;

public class JMSTester
{
	
	private static final int MAX_PRODUCER = 1;
	private static final int MAX_CONSUMER = 1;
	private static final int PRODUCTION_FREQUENCY = 1000;
	private static final int MAX_MESSAGE_SIZE = 1000*1024;
	
	private static ArrayList<Thread> producers = new ArrayList<Thread>();
	private static ArrayList<Thread> consumers = new ArrayList<Thread>();
	
	private List<byte[]> messageRefs = Collections.synchronizedList(new ArrayList<byte[]>());

	public static void main(String[] args) throws JMSException, InterruptedException, NoSuchAlgorithmException
	{
		JMSTester tester = new JMSTester();
		tester.test();
	}
	
	public void test() throws NoSuchAlgorithmException, JMSException, InterruptedException
	{
		String brokerUrl = "tcp://localhost:61616";
		String queue = "testQueue";

		//create producers
		for(int i = 0; i < MAX_PRODUCER; i++)
		{
			Thread t = new Thread(new JMSProducer(this, brokerUrl, MAX_MESSAGE_SIZE, queue, PRODUCTION_FREQUENCY, true, false));
			Thread.sleep(PRODUCTION_FREQUENCY / MAX_PRODUCER);
			t.start();
			producers.add(t);
		}
		//create consumers
		for(int i = 0; i < MAX_CONSUMER; i++)
		{
			Thread t = new Thread(new JMSConsumer(this, brokerUrl, queue, false));
			t.start();
			consumers.add(t);
		}
	}
	
	public synchronized void addMessageRef(byte[] ref)
	{
		System.out.println("ADD");
		messageRefs.add(ref);
		
	}
	
	public synchronized void removeMessageRef(byte[] ref)
	{
		System.out.println("Remove");
		messageRefs.remove(ref);
	}

}
