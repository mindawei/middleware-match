package com.alibaba.middleware.race.jstorm;

import java.io.Serializable;

public class Message implements Serializable{
	
	private static final long serialVersionUID = 1L;
	
	private String topic;
	private byte[] body;
	
	public Message() {
	}

	public Message(String topic, byte[] body) {
		super();
		this.topic = topic;
		this.body = body;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public byte[] getBody() {
		return body;
	}

	public void setBody(byte[] body) {
		this.body = body;
	}

}
