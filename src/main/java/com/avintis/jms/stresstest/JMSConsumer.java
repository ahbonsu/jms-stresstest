package com.avintis.jms.stresstest;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;

public class JMSConsumer implements Runnable
{
	private boolean stop = false;
	
	private boolean log = false;
	
	private String brokerUrl;
	private String queue;

	private ConnectionFactory connectionFactory;
	private Connection connection;
	private Session session;
	private MessageConsumer messageConsumer;
	
	public JMSConsumer(String brokerUrl, String queue) throws JMSException
	{
		this.brokerUrl = brokerUrl;
		this.queue = queue;
		
		connectionFactory = new ActiveMQConnectionFactory(this.brokerUrl);
		connection = connectionFactory.createConnection();
		
		connection.start();
		
		session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		
		Destination dest = session.createQueue(this.queue);
		
		messageConsumer = session.createConsumer(dest);
	}
	
	public JMSConsumer(String brokerUrl, String queue, boolean log) throws JMSException
	{
		this(brokerUrl, queue);
		this.log = log;
	}
	
	public void run()
	{
		while(!stop)
		{
			try
			{
				//TextMessage msg = (TextMessage) messageConsumer.receive(1000);
				TextMessage msg = (TextMessage) messageConsumer.receive();
				if(log)
				{
					System.out.println("Received Message");
				}
			} catch (JMSException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			finally
			{
				if(stop)
				{
					try
					{
						connection.close();
					} catch (JMSException ignore)
					{
					}
				}
			}
		}
	}
	
	public void stop()
	{
		stop = true;
	}

}
