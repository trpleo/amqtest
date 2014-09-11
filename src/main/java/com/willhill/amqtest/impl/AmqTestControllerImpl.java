package com.willhill.amqtest.impl;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;

import org.joda.time.DateTime;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.willhill.amqtest.AmqTestController;
import com.willhill.amqtest.TestMessage;
import com.willhill.amqtest.utils.TestMessageConverter;
import com.williamhill.pds.jmsclient.IJmsConfig;
import com.williamhill.pds.jmsclient.IJmsConsumer;
import com.williamhill.pds.jmsclient.IJmsConsumerListener;
import com.williamhill.pds.jmsclient.IJmsListener;
import com.williamhill.pds.jmsclient.IJmsProducer;
import com.williamhill.pds.jmsclient.impl.JmsConsumerImpl;
import com.williamhill.pds.jmsclient.impl.JmsProducerImpl;

public class AmqTestControllerImpl implements AmqTestController {

	private static ObjectMapper mapper = new ObjectMapper();
	
	private final List<IJmsConsumer> consumers = new ArrayList<IJmsConsumer>();
	private final List<IJmsConsumerListener> consumerListeners = new ArrayList<IJmsConsumerListener>();
	private final IJmsProducer producer;
	private final Queue<TestMessage> sentMessages = new LinkedList<TestMessage>();
	
	public AmqTestControllerImpl(IJmsConfig jmsConfig, IJmsListener producerListener) {
		super();
		
		this.producer = new JmsProducerImpl(jmsConfig, producerListener);
	}

	public List<IJmsConsumer> getAllConsumers() {
		return this.consumers;
	}

	public List<TestMessage> getMissingMessages(int consumerId) {
		// TODO Auto-generated method stub
		return null;
	}

	public void startProducer() {
		this.producer.start();
	}

	public void startConsumer(int consumerId) {
		if (isConsumerIdExists(consumerId)) {
			this.consumers.get(consumerId).start();
		}
	}

	public Entry<Integer, IJmsConsumer> createConsumer(IJmsConfig config) {
		final IJmsConsumer consumer = new JmsConsumerImpl(config, null);
		this.consumers.add(consumer);
		
		return new Entry<Integer, IJmsConsumer>() {
			
			public IJmsConsumer setValue(IJmsConsumer value) {
				throw new RuntimeException("not allowed by design");
			}
			
			public IJmsConsumer getValue() {
				return consumer;
			}
			
			public Integer getKey() {
				return Integer.valueOf(consumers.indexOf(consumer));
			}
		};
	}

	public void stopProducer() {
		this.producer.stop();
	}

	public void stopConsumer(int consumerId) {
		if (isConsumerIdExists(consumerId)) {
			this.consumers.get(consumerId).stop();
		}
	}
	
	private boolean isConsumerIdExists(int consumerId) {
		if (consumerId < 0 || consumerId > this.consumers.size() - 1) {
			System.out.println("Not existing customer");
			return false;
		}
		
		return true;
	}

	public void send(final String message) {
		final Date timestamp = new Date();
		final TestMessage tMessage = new TestMessage() {
			public long getCreationTime() {
				return timestamp.getTime();
			}
			public String getContent() {
				return message;
			}
		};
		final JsonNode json = mapper.createObjectNode().put("message", message);
		
		this.producer.send(0, timestamp, json);
		this.sentMessages.add(tMessage);
	}

	public void sendRepeatedly(String message) {
		// TODO Auto-generated method stub
		
	}

	public void stopSendingRepeatedly() {
		// TODO Auto-generated method stub
		
	}

	public void sendMessagesFromFile(File file) {
		// TODO Auto-generated method stub
		
	}
}
