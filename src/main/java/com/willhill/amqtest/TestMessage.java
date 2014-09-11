package com.willhill.amqtest;

public interface TestMessage {
	long getCreationTime();
	String getContent();
	@Override
	public boolean equals(Object o);
}
