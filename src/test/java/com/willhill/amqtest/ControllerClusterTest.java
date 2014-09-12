package com.willhill.amqtest;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.util.IOHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.runners.MockitoJUnitRunner;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.willhill.amqtest.impl.AmqTestControllerImpl;
import com.williamhill.pds.jmsclient.IJmsConfig;
import com.williamhill.pds.jmsclient.IJmsConsumerListener;
import com.williamhill.pds.jmsclient.IJmsListener;

@RunWith(MockitoJUnitRunner.class)
public class ControllerClusterTest {

	private static ObjectMapper mapper = new ObjectMapper();
	private static final String TEST_AMQ = "test-amq";
	private static final Integer MASTER_PORT = 61016;
	private static final Integer SLAVE_PORT = 61116;
	private static final String KAHADB_DIR = "testkahadb";
	
	@Rule
    public TemporaryFolder kahadbFolder = new TemporaryFolder();
	
	@Mock
	private IJmsConfig jmsMasterConfig;
	@Mock
	private IJmsConfig jmsSlaveConfig;
	@Mock
	private IJmsConfig jmsClientConfig;
	@Mock
	private IJmsListener producerListener;
	@Mock
	private IJmsConsumerListener consumerListener;
	
	private BrokerService masterBroker;
	private BrokerService slaveBroker;
	
    private AmqTestController testedEntity;
	
    @Before
	public void init() throws Exception {
		MockitoAnnotations.initMocks(this);
		
		File dataFileDir = kahadbFolder.newFolder(KAHADB_DIR);
		
		when(jmsClientConfig.getUri()).thenReturn("failover:(tcp://localhost:" + MASTER_PORT + ",tcp://localhost:" + SLAVE_PORT + ")");
		when(jmsClientConfig.getQueueName()).thenReturn(TEST_AMQ);
		when(jmsClientConfig.getUser()).thenReturn(null);
		when(jmsClientConfig.getPassword()).thenReturn(null);
		when(jmsClientConfig.isPersistent()).thenReturn(false);
		when(jmsClientConfig.resetOnError()).thenReturn(false);
		
		when(jmsMasterConfig.getUri()).thenReturn("tcp://localhost:" + MASTER_PORT.toString());
		when(jmsMasterConfig.getQueueName()).thenReturn(TEST_AMQ);
		when(jmsMasterConfig.getUser()).thenReturn(null);
		when(jmsMasterConfig.getPassword()).thenReturn(null);
		when(jmsMasterConfig.isPersistent()).thenReturn(false);
		when(jmsMasterConfig.resetOnError()).thenReturn(false);
		
		when(jmsSlaveConfig.getUri()).thenReturn("tcp://localhost:" + SLAVE_PORT);
		when(jmsSlaveConfig.getQueueName()).thenReturn(TEST_AMQ);
		when(jmsSlaveConfig.getUser()).thenReturn(null);
		when(jmsSlaveConfig.getPassword()).thenReturn(null);
		when(jmsSlaveConfig.isPersistent()).thenReturn(false);
		when(jmsSlaveConfig.resetOnError()).thenReturn(false);
		
		masterBroker = buildBroker(dataFileDir, jmsMasterConfig);
		
		slaveBroker = buildBroker(dataFileDir, jmsSlaveConfig);
		
		testedEntity = new AmqTestControllerImpl(jmsClientConfig, producerListener);
	}

	private BrokerService buildBroker(File dataFileDir, IJmsConfig jmsConfig) throws Exception, IOException {
		BrokerService broker = new BrokerService();
		
		broker.addConnector(jmsConfig.getUri());
		broker.setBrokerName(jmsConfig.getQueueName());
		broker.setUseShutdownHook(false);
		broker.setPersistent(jmsConfig.isPersistent());
		broker.setUseJmx(false);
		broker.setWaitForSlaveTimeout(1000L);
//		masterBroker.setDeleteAllMessagesOnStartup(true);
		broker.setPersistenceAdapter(adaptorBuilder(dataFileDir, 1000, 1000));
		broker.start();
		
		return broker;
	}
    
    @After
    public void tearDown() throws Exception {
    	stopAmqBroker(slaveBroker);
    	stopAmqBroker(masterBroker);
    }
    
    private void stopAmqBroker(BrokerService borker) throws Exception {
		if (borker != null && !borker.isStopped()) {
			borker.stop();
			borker = null;
		}
	}

	private KahaDBPersistenceAdapter adaptorBuilder(File dataFileDir, int writeBatchSize, int cacheSize) {
		KahaDBPersistenceAdapter kahaDB = new KahaDBPersistenceAdapter();
        IOHelper.deleteChildren(dataFileDir);
        kahaDB.setDirectory(dataFileDir);
        kahaDB.setIndexWriteBatchSize(writeBatchSize);	// small batch means more frequent and smaller writes
        kahaDB.setIndexCacheSize(cacheSize);
		return kahaDB;
	}

	@Test
	public void shouldConnectToCluster() throws InterruptedException {
		// given
		testedEntity.createConsumer(jmsClientConfig, consumerListener);
		
		// when
		testedEntity.startProducer();
		testedEntity.startConsumer(0);
		
		// then
		verify(producerListener, times(1)).onConnected();
		verify(producerListener, times(0)).onDisconnected();
		
		verify(consumerListener, times(1)).onConnected();
		verify(consumerListener, times(0)).onDisconnected();
		
		assertThat(testedEntity.getAllConsumers().size()).isEqualTo(1);
		
		// stopping - since isConnected() mehod is missing from the interface
		testedEntity.stopProducer();
		testedEntity.stopConsumer(0);
	}
	
	@Test
	public void shouldPassAMessageThroughCluster() throws InterruptedException {
		// given
		testedEntity.createConsumer(jmsClientConfig, consumerListener);
		
		// when
		testedEntity.startProducer();
		testedEntity.startConsumer(0);
		
		final String MESSAGE = "hello cluster";
		testedEntity.send(MESSAGE);
		
		Thread.sleep(500L);
		
		// then
		verify(producerListener, times(1)).onConnected();
		verify(producerListener, times(0)).onDisconnected();
		
		verify(consumerListener, times(1)).onConnected();
		verify(consumerListener, times(0)).onDisconnected();
		
		assertThat(testedEntity.getAllConsumers().size()).isEqualTo(1);
		assertThat(testedEntity.getMissingMessages(0).size()).isEqualTo(0);
		
		final JsonNode json = mapper.createObjectNode().put("message", MESSAGE);
		assertThat(testedEntity.getRecievedMessages(0).get(0)).isEqualTo(json.toString());
		
		// stopping - since isConnected() mehod is missing from the interface
		testedEntity.stopProducer();
		testedEntity.stopConsumer(0);
	}
	
	@Ignore
	@Test
	public void shouldPassAMessageIfMasterDies() throws InterruptedException {
		// given
		testedEntity.createConsumer(jmsClientConfig, consumerListener);
		
		// when
		testedEntity.startProducer();
		testedEntity.startConsumer(0);
		
		final String MESSAGE = "hello cluster";
		testedEntity.send(MESSAGE);
		
		Thread.sleep(500L);
		
		// then
		verify(producerListener, times(1)).onConnected();
		verify(producerListener, times(0)).onDisconnected();
		
		verify(consumerListener, times(1)).onConnected();
		verify(consumerListener, times(0)).onDisconnected();
		
		assertThat(testedEntity.getAllConsumers().size()).isEqualTo(1);
		assertThat(testedEntity.getMissingMessages(0).size()).isEqualTo(0);
		
		final JsonNode json = mapper.createObjectNode().put("message", MESSAGE);
		assertThat(testedEntity.getRecievedMessages(0).get(0)).isEqualTo(json.toString());
		
		// stopping - since isConnected() mehod is missing from the interface
		testedEntity.stopProducer();
		testedEntity.stopConsumer(0);
	}
}
