package com.willhill.amqtest;

import java.io.File;
import java.util.List;
import java.util.Map;

import com.williamhill.pds.jmsclient.IJmsConfig;
import com.williamhill.pds.jmsclient.IJmsConsumer;


public interface AmqTestController {
	
	/**
	 * list consumers
	 * 
	 * @return
	 * 	<n>:<config>
	 */
	List<IJmsConsumer> getAllConsumers();
	
	/**
	 * status producer
	 * status consumer <n>
	 * <connected|disconnected>
	 */
	
	/**
	 * missingmessages <consumer n>
	 * mm <consumer n>
	 * <list of messages>
	 */
	List<TestMessage> getMissingMessages(int consumerId);
	
	/**
	 * start producer
	 */
	void startProducer();
	
	/**
	 * start consumer <n>
	 * <n>:<config>
	 */
	void startConsumer(int consumerId);
	
	/**
	 * create consumer
	 * Map<Integer, IJmsConsumer>
	 */
	Map.Entry<Integer, IJmsConsumer> createConsumer(IJmsConfig config);
	
	/**
	 * stop producer
	 */
	void stopProducer();
	
	/**
	 * stop consumer <n>
	 */
	void stopConsumer(int consumerId);
	
	/**
	 * send <message as string>
	 * . send a message
	 */
	void send(String message);
	
	/**
	 * send -r <message as string>
	 * . send messages continously. extended with a timestamp. press "q" to
	 * stop
	 */
	void sendRepeatedly(String message);
	void stopSendingRepeatedly();
	
	/**
	 * send -f <file name>
	 * . send messages from a file line by line
	 */
	void sendMessagesFromFile(File file);
}
