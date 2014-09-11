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
import com.williamhill.pds.jmsclient.IJmsConfig;
import com.williamhill.pds.jmsclient.IJmsConsumer;
import com.williamhill.pds.jmsclient.IJmsConsumerListener;
import com.williamhill.pds.jmsclient.IJmsListener;
import com.williamhill.pds.jmsclient.IJmsProducer;
import com.williamhill.pds.jmsclient.impl.JmsProducerImpl;

public class AmqTestControllerImpl implements AmqTestController {

	private static final int PERIOD = 100;
	private static final int DELAY = 100;

	private static ObjectMapper mapper = new ObjectMapper();
	
	private final List<ConsumerContainer> consumerContainers = new ArrayList<ConsumerContainer>();
	private final IJmsProducer producer;
	private final Queue<String> sentMessages = new LinkedList<String>();
	
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

	public List<String> getMissingMessages(int consumerId) {
		final Queue<String> sentMessagesClone = new LinkedList<String>(this.sentMessages);
		sentMessagesClone.removeAll(this.consumerContainers.get(consumerId).getRecievedMessages());
		return Arrays.asList(sentMessagesClone.toArray(new String[sentMessagesClone.size()]));
	}
	
	public List<String> getRecievedMessages(int consumerId) {
		final Queue<String> recievedMessages = this.consumerContainers.get(consumerId).getRecievedMessages();
		return Arrays.asList(recievedMessages.toArray(new String[recievedMessages.size()]));
	}

	public void startProducer() {
		this.producer.start();
	}

	public void startConsumer(int consumerId) {
		if (isConsumerIdExists(consumerId)) {
			this.getConsumers().get(consumerId).start();
		}
	}

	public Entry<Integer, IJmsConsumer> createConsumer(IJmsConfig config, IJmsConsumerListener listener) {
		final ConsumerContainer cc = new ConsumerContainer(config, listener);
		this.consumerContainers.add(cc);
		
		return new Entry<Integer, IJmsConsumer>() {
			
			public IJmsConsumer setValue(IJmsConsumer value) {
				throw new RuntimeException("not allowed by design");
			}
			
			public IJmsConsumer getValue() {
				return cc.getConsumer();
			}
			
			public Integer getKey() {
				return Integer.valueOf(getConsumers().indexOf(cc.getConsumer()));
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
	
	private static void sendBase(final String message, final IJmsProducer producer, final Queue<String> sentMessages) {
		final JsonNode json = mapper.createObjectNode().put("message", message);
		producer.send(0, new Date(), json);
		sentMessages.add(json.toString());
	}
}
