package com.willhill;

//import static org.fest.assertions.api.Assertions.assertThat;
//import static org.mockito.Matchers.anyObject;
//import static org.mockito.Matchers.eq;
//import static org.mockito.Mockito.never;
//import static org.mockito.Mockito.times;
//import static org.mockito.Mockito.verify;
//import static org.mockito.Mockito.when;
//
//import java.util.ArrayList;
//import java.util.List;
//
//import org.apache.activemq.broker.BrokerService;
//import org.joda.time.DateTime;
//import org.junit.After;
//import org.junit.Before;
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.mockito.Mock;
//import org.mockito.MockitoAnnotations;
//import org.mockito.runners.MockitoJUnitRunner;
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.willhill.amqtest.AmqTestController;
//import com.willhill.amqtest.TestMessage;
//import com.willhill.amqtest.impl.AmqTestControllerImpl;
//import com.williamhill.pds.jmsclient.IJmsConfig;
//import com.williamhill.pds.jmsclient.IJmsConsumerListener;
//import com.williamhill.pds.jmsclient.IJmsListener;
//
//@RunWith(MockitoJUnitRunner.class)
//public class ControllerTest {
//
//	private static final Integer TEST_PORT = 61016;
//	
//	private AmqTestController testedEntity;
//	private BrokerService amqBroker;
//	
//	@Mock
//	private IJmsConfig jmsConfig;
//	@Mock
//	private IJmsListener producerListener;
//	@Mock
//	private IJmsConsumerListener consumerListener;
//	
//	@Before
//	public void init() {
//		MockitoAnnotations.initMocks(this);
//		
//		// set test configuration
//		when(jmsConfig.getUri()).thenReturn("tcp://localhost:" + TEST_PORT.toString());
//		when(jmsConfig.getQueueName()).thenReturn("test-amq");
//		when(jmsConfig.getUser()).thenReturn(null);
//		when(jmsConfig.getPassword()).thenReturn(null);
//		when(jmsConfig.isPersistent()).thenReturn(false);
//		when(jmsConfig.resetOnError()).thenReturn(false);
//		
//		testedEntity = new AmqTestControllerImpl(jmsConfig, producerListener, consumerListener);
//	}
//	
//	@After
//	public void tearDown() throws Exception {
//		stopAmqBroker();
//	}
//	
//	private void startAmqBroker(final IJmsConfig jmsConfig) throws Exception {
//		amqBroker = new BrokerService();
//		amqBroker.addConnector(jmsConfig.getUri());
//		amqBroker.setBrokerName(jmsConfig.getQueueName());
//		amqBroker.setUseShutdownHook(false);
//		amqBroker.setPersistent(jmsConfig.isPersistent());
//		amqBroker.setUseJmx(false);
//		amqBroker.start();
//	}
//	
//	private void stopAmqBroker() throws Exception {
//		if (amqBroker != null && !amqBroker.isStopped()) {
//			amqBroker.stop();
//			amqBroker = null;
//		}
//	}
//	
//	@Test
//	public void shouldStartController() throws Exception {
//		// given
//		startAmqBroker(jmsConfig);
//		
//		// when
//		testedEntity.startProducer();
//		testedEntity.startConsumer();
//		
//		// then
//		verify(producerListener, times(1)).onConnected();
//		verify(producerListener, times(0)).onDisconnected();
//		
//		verify(consumerListener, times(1)).onConnected();
//		verify(consumerListener, times(0)).onDisconnected();
//		
//		// stopping - since isConnected() mehod is missing from the interface
//		testedEntity.stopProducer();
//		testedEntity.stopConsumer();
//	}
//	
//	@Test
//	public void shouldStopController() {
//		// given
//		startAmqBroker(jmsConfig);
//		
//		// when
//		testedEntity.startProducer();
//		testedEntity.stopProducer();
//		
//		testedEntity.startConsumer();
//		testedEntity.stopConsumer();
//		
//		// then
//		verify(producerListener, times(1)).onConnected();
//		verify(producerListener, times(1)).onDisconnected();
//		
//		verify(consumerListener, times(1)).onConnected();
//		verify(consumerListener, times(1)).onDisconnected();
//		
//		// stopping - since isConnected() mehod is missing from the interface
//		testedEntity.stopProducer();
//		testedEntity.stopConsumer();
//	}
//	
//	@Test
//	public void shouldSendOneMessageWhichIsRecieved() {
//		// given
//		final String MESSAGE = "hello cluster";
//		startAmqBroker(jmsConfig);
//		
//		// when
//		testedEntity.startProducer();
//		
//		testedEntity.startConsumer();
//		
//		// then
//		verify(producerListener, times(1)).onConnected();
//		verify(consumerListener, times(1)).onConnected();
//		verify(consumerListener, times(1)).onMessageReceived(eq(MESSAGE));
//		
//		// stopping - since isConnected() mehod is missing from the interface
//		testedEntity.stopProducer();
//		testedEntity.stopConsumer();
//	}
//	
//	@Test
//	public void shouldCollectMissingMessages() {
//		// given
//		final List<TestMessage> messages = new ArrayList<TestMessage>();
//		final int batchSize = 10;
//		final int toMiss = batchSize - 3;
//		
//		for (final int i = 0; i < batchSize; i++) {
//			messages.add(new TestMessage() {
//
//				public long getCreationTime() {
//					return new DateTime().getMillis();
//				}
//
//				public String getContent() {
//					return "hello cluster" + Integer.toString(i);
//				}
//			});
//		}
//		
//		startAmqBroker(jmsConfig);
//		
//		// when
//		testedEntity.startProducer();
//		testedEntity.startConsumer();
//		
//		for (int i = 0; i < batchSize; i++) {
//			
//			if (i == toMiss)
//				continue;
//			
//			testedEntity.send(messages.get(i));
//		}
//		
//		// then
//		verify(producerListener, times(1)).onConnected();
//		verify(consumerListener, times(1)).onConnected();
//		verify(consumerListener, never()).onMessageReceived(messages.get(toMiss).toString());
//		assertThat(testedEntity.getMissingMessages().size()).isEqualTo(1);
//		verify(consumerListener, times(batchSize - 1)).onMessageReceived((String) anyObject());
//		
//		// stopping - since isConnected() mehod is missing from the interface
//		testedEntity.stopProducer();
//		testedEntity.stopConsumer();
//	}
//}
