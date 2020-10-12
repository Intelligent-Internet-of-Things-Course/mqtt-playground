package it.unimore.dipi.iot.mqtt.playground.model;

public class MessageDescriptor {
	
	private long timestamp;
	
	private String type;
	
	private double value;

	public MessageDescriptor() {
	}

	public MessageDescriptor(long timestamp, String type, double value) {
		this.timestamp = timestamp;
		this.type = type;
		this.value = value;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public double getValue() {
		return value;
	}

	public void setValue(double value) {
		this.value = value;
	}

}
