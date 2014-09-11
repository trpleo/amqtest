package com.willhill.amqtest;

import java.io.File;

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

public class TestAmqCluster {

	public static void main(String[] args) {
		// Defaults
		final AmqTestController controller = new AmqTestControllerImpl(null, null);

		// CLI Options
		Option help = new Option("h", "prints this message");
		Option listConsumers = OptionBuilder.withArgName("lc").hasArg().withDescription("list all consumers").create("lc");
		Option missingmessages = OptionBuilder.withArgName("mm").hasArg().withDescription("missing messages for consumer <n>").create("mm");
		Option startProducer = OptionBuilder.withArgName("startp").hasArg().withDescription("start the producer").create("startp");
		Option startConsumer = OptionBuilder.withArgName("startc").hasArg().withDescription("start a consumer with id <n>").create("startc");
		Option createConsumer = OptionBuilder.withArgName("cc").hasArg().withDescription("create a consumer with the given properites").create("cc");
		Option stopp = OptionBuilder.withArgName("stopp").hasArg().withDescription("stop the producer").create("stopp");
		Option stopc = OptionBuilder.withArgName("stopc").hasArg().withDescription("stop a consumer with id <n>").create("stopc");
		Option send = OptionBuilder.withArgName("send").hasArg().withDescription("send a message").create("send");
		Option rsend = OptionBuilder.withArgName("rsend").hasArg().withDescription("send a message repeatedly (100 times)").create("rsend");
		Option stopRSend = OptionBuilder.withArgName("stoprsend").hasArg().withDescription("stop sending messages (repeatedly)").create("stoprsend");
		Option fSend = OptionBuilder.withArgName("fsend").hasArg().withDescription("send messages from file").create("sfend");

		Options options = new Options();
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
					for (IJmsConsumer iJmsConsumer : controller.getAllConsumers()) {
						System.out.println("[" + cnt++ + "] : [" + iJmsConsumer.toString() + "]");
					}
				}

				if (line.hasOption("mm")) {
					Integer consumerId = Integer.getInteger(line.getOptionValue("mm"));
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
