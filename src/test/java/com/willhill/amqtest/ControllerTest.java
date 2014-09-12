package com.willhill.amqtest;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.broker.BrokerService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.runners.MockitoJUnitRunner;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.willhill.amqtest.AmqTestController;
import com.willhill.amqtest.impl.AmqTestControllerImpl;
import com.williamhill.pds.jmsclient.IJmsConfig;
import com.williamhill.pds.jmsclient.IJmsConsumerListener;
import com.williamhill.pds.jmsclient.IJmsListener;

@RunWith(MockitoJUnitRunner.class)
public class ControllerTest {

	private static ObjectMapper mapper = new ObjectMapper();
	private static final Integer TEST_PORT = 61016;
	
	private AmqTestController testedEntity;
	private BrokerService amqBroker;
	
	@Mock
	private IJmsConfig jmsConfig;
	@Mock
	private IJmsListener producerListener;
	@Mock
	private IJmsConsumerListener consumerListener;
	
	@Before
	public void init() {
		MockitoAnnotations.initMocks(this);
		
		// set test configuration
		when(jmsConfig.getUri()).thenReturn("tcp://localhost:" + TEST_PORT.toString());
		when(jmsConfig.getQueueName()).thenReturn("test-amq");
		when(jmsConfig.getUser()).thenReturn(null);
		when(jmsConfig.getPassword()).thenReturn(null);
		when(jmsConfig.isPersistent()).thenReturn(false);
		when(jmsConfig.resetOnError()).thenReturn(false);
		
		testedEntity = new AmqTestControllerImpl(jmsConfig, producerListener);
	}
	
	@After
	public void tearDown() throws Exception {
		stopAmqBroker();
	}
	
	private void startAmqBroker(final IJmsConfig jmsConfig) throws Exception {
		amqBroker = new BrokerService();
		amqBroker.addConnector(jmsConfig.getUri());
		amqBroker.setBrokerName(jmsConfig.getQueueName());
		amqBroker.setUseShutdownHook(false);
		amqBroker.setPersistent(jmsConfig.isPersistent());
		amqBroker.setUseJmx(false);
		amqBroker.start();
	}
	
	private void stopAmqBroker() throws Exception {
		if (amqBroker != null && !amqBroker.isStopped()) {
			amqBroker.stop();
			amqBroker = null;
		}
	}
	
	@Test
	public void shouldStartController() throws Exception {
		// given
		startAmqBroker(jmsConfig);
		testedEntity.createConsumer(jmsConfig, consumerListener);
		
		// when
		testedEntity.startProducer();
		testedEntity.startConsumer(0);
		
		// then
		verify(producerListener, times(1)).onConnected();
		verify(producerListener, times(0)).onDisconnected();
		
		verify(consumerListener, times(1)).onConnected();
		verify(consumerListener, times(0)).onDisconnected();
		
		assertThat(testedEntity.getAllConsumers().size()).isEqualTo(1);
		assertThat(testedEntity.getMissingMessages(0).size()).isEqualTo(0);
		
		// stopping - since isConnected() mehod is missing from the interface
		testedEntity.stopProducer();
		testedEntity.stopConsumer(0);
	}
	
	@Test
	public void shouldStopController() throws Exception {
		// given
		startAmqBroker(jmsConfig);
		testedEntity.createConsumer(jmsConfig, consumerListener);
		
		// when
		testedEntity.startProducer();
		testedEntity.stopProducer();
		
		testedEntity.startConsumer(0);
		testedEntity.stopConsumer(0);
		
		// then
		verify(producerListener, times(1)).onConnected();
		verify(producerListener, times(1)).onDisconnected();
		
		verify(consumerListener, times(1)).onConnected();
		verify(consumerListener, times(1)).onDisconnected();
		
		assertThat(testedEntity.getAllConsumers().size()).isEqualTo(1);
		assertThat(testedEntity.getMissingMessages(0).size()).isEqualTo(0);
		
		// stopping - since isConnected() mehod is missing from the interface
		testedEntity.stopProducer();
		testedEntity.stopConsumer(0);
	}
	
	@Test
	public void shouldSendOneMessageWhichIsRecieved() throws Exception {
		// given
		final String MESSAGE = "hello cluster";
		startAmqBroker(jmsConfig);
		testedEntity.createConsumer(jmsConfig, consumerListener);
		
		// when
		testedEntity.startProducer();
		testedEntity.startConsumer(0);
		testedEntity.send(MESSAGE);
		
		// then
		verify(producerListener, times(1)).onConnected();
		verify(consumerListener, times(1)).onConnected();
		assertThat(testedEntity.getAllConsumers().size()).isEqualTo(1);
		assertThat(testedEntity.getMissingMessages(0).size()).isEqualTo(0);
		final JsonNode json = mapper.createObjectNode().put("message", MESSAGE);
		assertThat(testedEntity.getRecievedMessages(0).get(0)).isEqualTo(json.toString());
		
		// stopping - since isConnected() mehod is missing from the interface
		testedEntity.stopProducer();
		testedEntity.stopConsumer(0);
	}
	
	@Test
	public void shouldCollectAllMessages() throws Exception {
		// given
		final List<String> MESSAGES = new ArrayList<String>();
		final int BATCH_SIZE = 10;
		
		for (int i = 0; i < BATCH_SIZE; i++) {
			MESSAGES.add("hello cluster" + Integer.toString(i));
		}
		
		startAmqBroker(jmsConfig);
		testedEntity.createConsumer(jmsConfig, consumerListener);
		
		// when
		testedEntity.startProducer();
		testedEntity.startConsumer(0);
		
		for (int i = 0; i < BATCH_SIZE; i++) {
			testedEntity.send(MESSAGES.get(i).toString());
		}
		
		Thread.sleep(500L);
		
		// then
		verify(producerListener, times(1)).onConnected();
		verify(consumerListener, times(1)).onConnected();
		assertThat(testedEntity.getMissingMessages(0).size()).isEqualTo(0);
		for (String message : MESSAGES) {
			final JsonNode json = mapper.createObjectNode().put("message", message);
			verify(consumerListener, times(1)).onMessageReceived(json.toString());
		}
		
		// stopping - since isConnected() mehod is missing from the interface
		testedEntity.stopProducer();
		testedEntity.stopConsumer(0);
	}
}
