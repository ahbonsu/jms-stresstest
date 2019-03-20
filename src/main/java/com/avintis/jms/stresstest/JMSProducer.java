package com.avintis.jms.stresstest;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;

public class JMSProducer implements Runnable
{
	private boolean log = false;
	
	private JMSTester tester;
	
	private String brokerUrl;
	private int maxMessageSize;
	private String queue;
	private long frequency;
	private Random random;
	private boolean randomSize;
	private String username;
	private String password;
	
	private ConnectionFactory connectionFactory;
	private Connection connection;
	private Session session;
	private javax.jms.MessageProducer producer;
	
	private MessageDigest md;
	
	private boolean stop = false;
	
	public JMSProducer(JMSTester tester, String brokerUrl, int maxMessageSize, String queue, long frequency, boolean randomSize, boolean log, String username, String password) throws JMSException, NoSuchAlgorithmException
	{
		this.tester = tester;
		this.brokerUrl = brokerUrl;
		this.maxMessageSize = maxMessageSize;
		this.queue = queue;
		this.frequency = frequency;
		random = new Random();
		this.randomSize = randomSize;
		this.log = log;
		this.username = username;
		this.password = password;

		
		connectionFactory = new ActiveMQConnectionFactory(username, password, this.brokerUrl);
		connection = connectionFactory.createConnection();
		
		connection.start();
		
		session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		Destination dest = session.createQueue(this.queue);
		
		producer = session.createProducer(dest);
		producer.setDeliveryMode(DeliveryMode.PERSISTENT);
		
		md = MessageDigest.getInstance("SHA1");

	}
	
	public JMSProducer(JMSTester tester, String brokerUrl, int maxMessageSize, String queue, long frequency, boolean randomSize, String username, String password) throws JMSException, NoSuchAlgorithmException
	{
		this(tester, brokerUrl, maxMessageSize, queue, frequency, randomSize, false, username, password);
	}
	
	public void run()
	{
		while(!stop)
		{
			try
			{
				TextMessage textMessage = session.createTextMessage();
				if(randomSize)
				{
					int newSize = random.nextInt(maxMessageSize);
					textMessage.setText(createMessage(newSize));
					if(log)
					{
						System.out.println("Message Size: " + newSize);
					}
				}
				else
				{
					textMessage.setText(createMessage(maxMessageSize));
					if(log)
					{
						System.out.println("Message Size: " + maxMessageSize);
					}
				}
				
				if(log)
				{
					System.out.println("Created new Message: " + textMessage.getText());
				}
				tester.addMessageRef(md.digest(textMessage.getText().getBytes()));
				
				producer.send(textMessage);
				
				
				if(log)
				{
					System.out.println("Sleep now for " + frequency + " ms.");
				}
				Thread.sleep(frequency);
				
			} 
			catch (Exception e)
			{
				e.printStackTrace();
			}
			finally
			{
				if(stop)
				{
					System.out.println("THREAD STOPPED!!");
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
	
	
	private String createMessage(int length) throws UnsupportedEncodingException
	{
		 byte[] array = new byte[length];
		 random.nextBytes(array);
		 
		 return new String(array, "UTF-8");
	}
	
	public String toString()
	{
		return brokerUrl + " " + queue;
	}
	
	public void stop()
	{
		this.stop = true;
	}

}
