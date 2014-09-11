package com.willhill.amqtest.utils;

import com.willhill.amqtest.TestMessage;

public class TestMessageConverter {
	
	private static final String SEPARATOR = "#";

	public static TestMessage strToTestMessage(String message) {
		
		final String[] fragments = message.split(SEPARATOR);
		
		return new TestMessage() {

			public long getCreationTime() {
				return Long.parseLong(fragments[0]);
			}

			public String getContent() {
				return fragments[1];
			}
			
		};
	}
	
	public static String testMessageToString(TestMessage message) {
		
		return new StringBuilder()
				.append(Long.toString(message.getCreationTime()))
				.append(SEPARATOR)
				.append(message.getContent()).toString();
	}
}
