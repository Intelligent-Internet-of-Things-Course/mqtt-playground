package com.iot.demo.mqtt.sensors;

import com.iot.demo.mqtt.model.GpsData;

public class GpsSensor {

	public GpsData readData() {
		return new GpsData(10.44, 44.10, 10, 100);
	}

}
