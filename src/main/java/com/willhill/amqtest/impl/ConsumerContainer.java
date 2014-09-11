package com.willhill.amqtest.impl;

import java.util.LinkedList;
import java.util.Queue;

import com.williamhill.pds.jmsclient.IJmsConfig;
import com.williamhill.pds.jmsclient.IJmsConsumer;
import com.williamhill.pds.jmsclient.IJmsConsumerListener;
import com.williamhill.pds.jmsclient.impl.JmsConsumerImpl;

public class ConsumerContainer implements IJmsConsumerListener {
	private final IJmsConsumer consumer;
	private final IJmsConsumerListener listener;
	private final Queue<String> recievedMessages = new LinkedList<String>();
	
	public IJmsConsumer getConsumer() {
		return consumer;
	}

	public IJmsConsumerListener getListener() {
		return listener;
	}

	public Queue<String> getRecievedMessages() {
		return recievedMessages;
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
		recievedMessages.add(message);
		listener.onMessageReceived(message);
	}
}
