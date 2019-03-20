package com.avintis.jms.stresstest;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import javax.jms.JMSException;

public class JMSTester
{
	private static String brokerURL;
	private static String queueName;
	private static int maxProducer;
	private static int maxConsumer;
	private static int productionFrequency;
	private static int maxMessageSize;
	private static int queueViewerTime;
	private static boolean log;
	private static boolean useRandomSize;
	private static boolean benchmark;
	private static int benchmarkTime;
	
	private static ArrayList<JMSProducer> producers = new ArrayList<JMSProducer>();
	private static ArrayList<JMSConsumer> consumers = new ArrayList<JMSConsumer>();
	
	private List<String> messageRefs = Collections.synchronizedList(new ArrayList<String>());
	
	private int totalMessages = 0;

	public static void main(String[] args) throws JMSException, InterruptedException, NoSuchAlgorithmException, IOException
	{
		Properties props = new Properties();
		
		if (args.length == 1)
		{
			File conf = new File(args[0]);
			FileReader fr = new FileReader(conf);
			props.load(fr);
		}
		else
		{
			props.load(ClassLoader.getSystemClassLoader().getResourceAsStream("config.properties"));
		}

			
		
		
		
		brokerURL = props.getProperty("brokerURL");
		queueName = props.getProperty("queueName");
		maxProducer = Integer.valueOf(props.getProperty("maxProducer"));
		maxConsumer = Integer.valueOf(props.getProperty("maxConsumer"));
		productionFrequency = Integer.valueOf(props.getProperty("productionFrequency"));
		maxMessageSize = Integer.valueOf(props.getProperty("maxMessageSize"));
		queueViewerTime = Integer.valueOf(props.getProperty("queueViewerTime"));
		benchmark = Boolean.valueOf(props.getProperty("benchmark"));
		log = Boolean.valueOf(props.getProperty("log"));
		useRandomSize = Boolean.valueOf(props.getProperty("useRandomSize"));
		benchmarkTime = Integer.valueOf(props.getProperty("benchmarkTime"));

		JMSTester tester = new JMSTester();
		tester.test();

	}
	
	public void test() throws NoSuchAlgorithmException, JMSException, InterruptedException
	{
		String brokerUrl = brokerURL;

		//create producers
		for(int i = 0; i < maxProducer; i++)
		{
			JMSProducer prod = new JMSProducer(this, brokerUrl, maxMessageSize, queueName, productionFrequency, useRandomSize, log);
			Thread t = new Thread(prod);
			//do not start all at the same time
			Thread.sleep(productionFrequency / maxProducer);
			t.start();
			producers.add(prod);
		}
		//create consumers
		for(int i = 0; i < maxConsumer; i++)
		{
			JMSConsumer cons = new JMSConsumer(this, brokerUrl, queueName, log);
			Thread t = new Thread(cons);
			t.start();
			consumers.add(cons);
		}
		
		Thread t = new Thread(new QueueViewer(this, queueViewerTime));
		t.start();
		
		if(benchmark)
		{
			Thread.sleep(benchmarkTime * 1000);
			
			System.out.println("STOPPING!!");
			
			for(JMSProducer prod : producers)
			{
				prod.stop();
			}
			
			System.out.println("ALL PRODUCERS STOPPED!");
			int messagesAfterStopped = getCurrentMessagesInQueue();
			long stopped = System.currentTimeMillis();
			while(getCurrentMessagesInQueue() > 0)
			{
				Thread.sleep(100);
			}
			
			long finished = System.currentTimeMillis();
			
			System.out.println("Took " + (finished -  stopped) + " ms to process the last " + messagesAfterStopped + " of total messages: " + totalMessages);
			
			System.exit(0);
		}
	}
	
	public synchronized void addMessageRef(byte[] ref)
	{
		String str = Base64.getEncoder().encodeToString(ref);
		messageRefs.add(str);
		totalMessages++;
		
	}
	
	public synchronized void removeMessageRef(byte[] ref)
	{
		String str = Base64.getEncoder().encodeToString(ref);
		int index = messageRefs.indexOf(str);
		if(index != -1)
		{
			messageRefs.remove(index);
		}
	}
	
	public int getCurrentMessagesInQueue()
	{
		return messageRefs.size();
	}

}
