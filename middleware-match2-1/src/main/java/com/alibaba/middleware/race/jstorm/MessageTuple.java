package com.alibaba.middleware.race.jstorm;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class MessageTuple implements Serializable{

	private static final long serialVersionUID = 1L;
	
	private List<Message> messages = new ArrayList<>();

	public MessageTuple() {
	}

	public List<Message> getMessages() {
		return messages;
	}
	
	public void addMessages(Message message) {
		messages.add(message);
	}
	
}
