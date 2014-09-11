package com.willhill.amqtest;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.willhill.amqtest.impl.AmqTestControllerImpl;
import com.williamhill.pds.jmsclient.IJmsConfig;
import com.williamhill.pds.jmsclient.IJmsConsumer;
import com.williamhill.pds.jmsclient.IJmsListener;

public class TestAmqCluster {
	
	// Defaults
	final AmqTestController controller = new AmqTestControllerImpl(new IJmsConfig() {

		public String getUri() {
			return "tcp://localhost:61616";
		}

		public String getQueueName() {
			return "clustered-amq";
		}

		public String getUser() {
			return null;
		}

		public String getPassword() {
			return null;
		}

		public boolean isPersistent() {
			return false;
		}

		public boolean resetOnError() {
			return false;
		}
		
	}, new IJmsListener() {

		public void onConnected() {
			System.out.println("onConnected was called");
		}

		public void onDisconnected() {
			System.out.println("onDisconnected was called");
		}

		public void onError(Exception cause) {
			System.out.println("onException was called, with error: \n" + cause.getMessage());
		}
	});

	// CLI Options
	final Option help = new Option("h", "prints this message");
	final Option listConsumers = OptionBuilder.withArgName("lc").withDescription("list all consumers").create("lc");
	final Option missingmessages = OptionBuilder.withArgName("mm").hasArg().withDescription("missing messages for consumer <n>").create("mm");
	final Option startProducer = OptionBuilder.withArgName("startp").hasArg().withDescription("start the producer").create("startp");
	final Option startConsumer = OptionBuilder.withArgName("startc").hasArg().withDescription("start a consumer with id <n>").create("startc");
	final Option createConsumer = OptionBuilder.withArgName("cc").hasArg().withDescription("create a consumer with the given properites").create("cc");
	final Option stopp = OptionBuilder.withArgName("stopp").hasArg().withDescription("stop the producer").create("stopp");
	final Option stopc = OptionBuilder.withArgName("stopc").hasArg().withDescription("stop a consumer with id <n>").create("stopc");
	final Option send = OptionBuilder.withArgName("send").hasArg().withDescription("send a message").create("send");
	final Option rsend = OptionBuilder.withArgName("rsend").hasArg().withDescription("send a message repeatedly (100 times)").create("rsend");
	final Option stopRSend = OptionBuilder.withArgName("stoprsend").hasArg().withDescription("stop sending messages (repeatedly)").create("stoprsend");
	final Option fSend = OptionBuilder.withArgName("fsend").hasArg().withDescription("send messages from file").create("sfend");
	
	final Options options = new Options();
	
	public TestAmqCluster() {
		super();
		
		options.addOption(help);
		options.addOption(listConsumers);
		options.addOption(missingmessages);
		options.addOption(startProducer);
		options.addOption(startConsumer);
		options.addOption(createConsumer);
		options.addOption(stopp);
		options.addOption(stopc);
		options.addOption(send);
		options.addOption(rsend);
		options.addOption(stopRSend);
		options.addOption(fSend);
	}
	
	public static void main(String[] args) {
		final TestAmqCluster tester = new TestAmqCluster();
		final BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String userName = null;

		System.out.print("Waiting for your command: ");
		
		try {
			while(!(userName = br.readLine()).equals("q")) {
				tester.doCommand(userName.split(" "));
			}
		} catch (IOException e) {
			System.out.println(e.getMessage());
		}
	}
	
	public void doCommand(String[] args) {
		// CLI Parser
		CommandLineParser parser = new GnuParser();
		try {
			CommandLine line = parser.parse(options, args);
			if (line.hasOption("h")) {
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp("LyraPolicy", options);
			} else {
				if (line.hasOption("lc")) {
					int cnt = 0;
					System.out.println("list of consumers:");
					for (IJmsConsumer iJmsConsumer : controller.getAllConsumers()) {
						System.out.println("[" + cnt++ + "] : [" + iJmsConsumer.toString() + "]");
					}
				}

				if (line.hasOption("mm")) {
					Integer consumerId = Integer.getInteger(line.getOptionValue("mm"));
					System.out.println("list of test messages");
					for (TestMessage testMessage : controller.getMissingMessages(consumerId)) {
						System.out.println(testMessage.toString());
					}
				}

				if (line.hasOption("startp")) {
					controller.startProducer();
				}

				if (line.hasOption("startc")) {
					controller.startConsumer(Integer.getInteger(line.getOptionValue("startc")));
				}

				if (line.hasOption("cc")) {
					controller.createConsumer(new IJmsConfig() {

						public String getUri() {
							return null;
						}

						public String getQueueName() {
							return null;
						}

						public String getUser() {
							return null;
						}

						public String getPassword() {
							return null;
						}

						public boolean isPersistent() {
							return false;
						}

						public boolean resetOnError() {
							return false;
						}
					});
				}

				if (line.hasOption("stopp")) {
					controller.stopProducer();
				}
				
				if (line.hasOption("stopc")) {
					controller.stopConsumer(Integer.getInteger(line.getOptionValue("stopc")));
				}
				
				if (line.hasOption("send")) {
					controller.send(line.getOptionValue("send"));
				}
				
				if (line.hasOption("rsend")) {
					controller.sendRepeatedly(line.getOptionValue("rsend"));
				}
				
				if (line.hasOption("stoprsend")) {
					controller.stopSendingRepeatedly();
				}
				
				if (line.hasOption("fsend")) {
					controller.sendMessagesFromFile(new File(line.getOptionValue("fsend")));
				}

				// here we'll start the "server" later.
				// Server server = new Server(serverAddress, serverPort,
				// serverThreads, serverThreadTimeout, verboseOutput);
				// server.run();
			}
		} catch (ParseException exp) {
			System.err.println("Parsing failed. Reason: " + exp.getMessage());
		}
	}
}
