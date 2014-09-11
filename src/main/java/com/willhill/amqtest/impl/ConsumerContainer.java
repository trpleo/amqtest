package com.willhill.amqtest.impl;

import java.util.LinkedList;
import java.util.Queue;

import com.willhill.amqtest.TestMessage;
import com.williamhill.pds.jmsclient.IJmsConfig;
import com.williamhill.pds.jmsclient.IJmsConsumer;
import com.williamhill.pds.jmsclient.IJmsConsumerListener;
import com.williamhill.pds.jmsclient.impl.JmsConsumerImpl;

public class ConsumerContainer implements IJmsConsumerListener {
	private final IJmsConsumer consumer;
	private final IJmsConsumerListener listener;
	private final Queue<TestMessage> sentMessages = new LinkedList<TestMessage>();
	
	public IJmsConsumer getConsumer() {
		return consumer;
	}

	public IJmsConsumerListener getListener() {
		return listener;
	}

	public Queue<TestMessage> getSentMessages() {
		return sentMessages;
	}

	public ConsumerContainer(IJmsConfig config, IJmsConsumerListener listener) { 
		super();
		
		this.consumer = new JmsConsumerImpl(config, this);
		this.listener = listener;
	}

	public void onConnected() {
		listener.onConnected();
	}

	public void onDisconnected() {
		listener.onDisconnected();
	}

	public void onError(Exception cause) {
		listener.onError(cause);
	}

	public void onMessageReceived(String message) {
		listener.onMessageReceived(message);
	}
}
