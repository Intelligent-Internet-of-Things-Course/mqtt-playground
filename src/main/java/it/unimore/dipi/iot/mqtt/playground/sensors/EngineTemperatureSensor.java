package it.unimore.dipi.iot.mqtt.playground.sensors;

import java.util.Random;

public class EngineTemperatureSensor {

    private Random rnd;
    
    private double temperatureValue;
    
    public EngineTemperatureSensor() {
        this.rnd = new Random(System.currentTimeMillis());
        this.temperatureValue = 0.0; 
    }
 
    private void generateEngineTemperature() {
    	temperatureValue =  80 + rnd.nextDouble() * 20.0;     
    }

	public double getTemperatureValue() {
		generateEngineTemperature();
		return temperatureValue;
	}

	public void setTemperatureValue(double temperatureValue) {
		this.temperatureValue = temperatureValue;
	}

	@Override
	public String toString() {
		return "EngineTemperatureSensor [temperatureValue=" + temperatureValue + "]";
	}
    
}