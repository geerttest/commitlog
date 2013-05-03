package net.geertvos.test.Logtest;

import java.util.UUID;

public class Message {

	private String message;
	private UUID id;
	
	public Message(String message, UUID id) {
		this.message = message;
		this.id = id;
	}
	
	public Message() {
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public UUID getId() {
		return id;
	}

	public void setId(UUID id) {
		this.id = id;
	}
	
}

