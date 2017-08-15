package org.ct.aws_sqs_receiver;


import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.amazon.sqs.javamessaging.ProviderConfiguration;
import com.amazon.sqs.javamessaging.SQSConnection;
import com.amazon.sqs.javamessaging.SQSConnectionFactory;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;

/**
 * @author kunalp3
 * This application demonstrate connection to AWS SimpleQueueService
 * using JMS listener for continuous message polling
 */

@SpringBootApplication
public class SQSReceiverDemo 
{
	private static Properties properties = new Properties();
	static Logger log = LoggerFactory.getLogger(SQSReceiverDemo.class);
	static AWSCredentials credentials = null;
	static SQSConnection connection = null;

	public static void main( String[] args )
	{
		SpringApplication.run(SQSReceiverDemo.class, args);

		try {
			properties.load(new FileInputStream("D:/awsConfig/credentials.properties")); //TODO: this file should be present in resources folder
		}catch(IOException e){
			e.printStackTrace();
		}
		String ACCESS_KEY = properties.getProperty("ACCESS_KEY");
		String SECRET_KEY = properties.getProperty("SECRET_KEY");
		credentials = new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY);

		SQSConnectionFactory connectionFactory = new SQSConnectionFactory(
				new ProviderConfiguration(),
				AmazonSQSClientBuilder.standard()
				.withRegion(Regions.US_EAST_2) //TODO: Region can also be made configurable in the above properties file
				);
		try {
			SQSConnection connection = connectionFactory.createConnection(credentials);

			log.info("Connected Successfully...");

			Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			Queue queue = session.createQueue("MyQueueOne");
			MessageConsumer consumer = session.createConsumer(queue);

			consumer.setMessageListener(new TestListener());

			connection.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

class TestListener implements MessageListener {

	/**
	 * Here the contents of the received message are simply printed on console
	 */
	public void onMessage(Message msg) {

		try {
			System.out.println("******************");
			System.out.println("Following message is received:\n"+ ((TextMessage) msg).getText());
			System.out.println("******************");
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
