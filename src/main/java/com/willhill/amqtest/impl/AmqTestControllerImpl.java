package com.willhill.amqtest.impl;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;

import scala.NotImplementedError;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.willhill.amqtest.AmqTestController;
import com.willhill.amqtest.TestMessage;
import com.williamhill.pds.jmsclient.IJmsConfig;
import com.williamhill.pds.jmsclient.IJmsConsumer;
import com.williamhill.pds.jmsclient.IJmsListener;
import com.williamhill.pds.jmsclient.IJmsProducer;
import com.williamhill.pds.jmsclient.impl.JmsConsumerImpl;
import com.williamhill.pds.jmsclient.impl.JmsProducerImpl;

public class AmqTestControllerImpl implements AmqTestController {

	private static final int PERIOD = 100;
	private static final int DELAY = 100;

	private static ObjectMapper mapper = new ObjectMapper();
	
	private final List<ConsumerContainer> consumerContainers = new ArrayList<ConsumerContainer>();
	private final IJmsProducer producer;
	private final Queue<TestMessage> sentMessages = new LinkedList<TestMessage>();
	
	private Timer timer = null;
	
	public AmqTestControllerImpl(IJmsConfig jmsConfig, IJmsListener producerListener) {
		super();
		
		this.producer = new JmsProducerImpl(jmsConfig, producerListener);
	}

	public List<IJmsConsumer> getAllConsumers() {
		return getConsumers();
	}

	private List<IJmsConsumer> getConsumers() {
		final List<IJmsConsumer> consumers = new ArrayList<IJmsConsumer>();
		
		for (ConsumerContainer cc : consumerContainers) {
			consumers.add(cc.getConsumer());
		}
		
		return consumers;
	}

	public List<TestMessage> getMissingMessages(int consumerId) {
		final Queue<TestMessage> recievedMessages = this.consumerContainers.get(consumerId).getSentMessages();
		final Queue<TestMessage> sentMessagesClone = new LinkedList<TestMessage>();
		
		sentMessagesClone.addAll(this.sentMessages);
		
		sentMessagesClone.removeAll(recievedMessages);
		
		return Arrays.asList(sentMessagesClone.toArray(new TestMessage[sentMessagesClone.size()]));
	}

	public void startProducer() {
		this.producer.start();
	}

	public void startConsumer(int consumerId) {
		if (isConsumerIdExists(consumerId)) {
			this.getConsumers().get(consumerId).start();
		}
	}

	public Entry<Integer, IJmsConsumer> createConsumer(IJmsConfig config) {
		final IJmsConsumer consumer = new JmsConsumerImpl(config, null);
		this.getConsumers().add(consumer);
		
		return new Entry<Integer, IJmsConsumer>() {
			
			public IJmsConsumer setValue(IJmsConsumer value) {
				throw new RuntimeException("not allowed by design");
			}
			
			public IJmsConsumer getValue() {
				return consumer;
			}
			
			public Integer getKey() {
				return Integer.valueOf(getConsumers().indexOf(consumer));
			}
		};
	}

	public void stopProducer() {
		this.producer.stop();
	}

	public void stopConsumer(int consumerId) {
		if (isConsumerIdExists(consumerId)) {
			this.getConsumers().get(consumerId).stop();
		}
	}
	
	public void send(final String message) {
		sendBase(message, this.producer, this.sentMessages);
	}
	
	public void sendRepeatedly(final String message) {
		timer = new Timer();
		timer.scheduleAtFixedRate(new TimerTask() {

			@Override
			public void run() {
				new Thread(new Runnable() {
					public void run() {
						sendBase(message, AmqTestControllerImpl.this.producer, AmqTestControllerImpl.this.sentMessages);
					}
				}).start();
			}
		}, DELAY, PERIOD);
	}

	public void stopSendingRepeatedly() {
		if (timer != null) {
			timer.cancel();
			timer = null;
		}
	}

	public void sendMessagesFromFile(File file) {
		throw new NotImplementedError("not implemented yet");
	}
	
	private boolean isConsumerIdExists(int consumerId) {
		if (consumerId < 0 || consumerId > this.getConsumers().size() - 1) {
			System.out.println("Not existing customer");
			return false;
		}
		
		return true;
	}
	
	private static void sendBase(final String message, final IJmsProducer producer, final Queue<TestMessage> sentMessages) {
		final Date timestamp = new Date();
		final TestMessage tMessage = new TestMessage() {
			public long getCreationTime() {
				return timestamp.getTime();
			}
			public String getContent() {
				return message;
			}
			@Override
			public boolean equals(Object entity) {
				if (this == entity) return true;
				if (entity instanceof TestMessage) {
					final TestMessage other = (TestMessage) entity;
					return other.getCreationTime() == this.getCreationTime() && other.getContent().equals(this.getContent());
				}
				
				return false;
			}
		};
		final JsonNode json = mapper.createObjectNode().put("message", message);
		
		producer.send(0, timestamp, json);
		sentMessages.add(tMessage);
	}
}
