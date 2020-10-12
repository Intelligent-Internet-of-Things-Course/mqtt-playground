package it.unimore.dipi.iot.mqtt.playground.sensors;

import it.unimore.dipi.iot.mqtt.playground.model.GpsData;

public class GpsSensor {

	public GpsData readData() {
		return new GpsData(10.44, 44.10, 10, 100);
	}

}
