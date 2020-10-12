package it.unimore.dipi.iot.mqtt.playground.process;

import it.unimore.dipi.iot.mqtt.playground.sensors.EngineTemperatureSensor;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SimpleProducer {

    private final static Logger logger = LoggerFactory.getLogger(SimpleProducer.class);
    
    private static String SERVER_URI = "tcp://127.0.0.1:1883";

    private static final int MESSAGE_COUNT = 1000;
    
    private static final String TOPIC = "sensor/temperature";
    
    public static void main(String[] args) {

        logger.info("SimpleProducer started ...");

        try{
        	
            String publisherId = UUID.randomUUID().toString();
            
            MqttClientPersistence persistence = new MemoryPersistence();
            
            IMqttClient client = new MqttClient(SERVER_URI,publisherId, persistence);
            
            MqttConnectOptions options = new MqttConnectOptions();
            options.setAutomaticReconnect(true);
            options.setCleanSession(true);
            options.setConnectionTimeout(10);
            
            client.connect(options);

            logger.info("Connected !");
            
            EngineTemperatureSensor engineTemperatureSensor = new EngineTemperatureSensor();
            
            for(int i = 0; i < MESSAGE_COUNT; i++) {
            	
            	double sensorValue = engineTemperatureSensor.getTemperatureValue();
            	String payloadString = Double.toString(sensorValue);
            	publishData(client, TOPIC, payloadString);
            	Thread.sleep(1000);
            }
            	
            client.disconnect();
            client.close();

            logger.info("Disconnected !");

        }catch (Exception e){
            e.printStackTrace();
        }

    }
    
    public static void publishData(IMqttClient mqttClient, String topic, String msgString) throws MqttException {

        logger.debug("Publishing to Topic: {} Data: {}", topic, msgString);

        if (mqttClient.isConnected() && msgString != null && topic != null) {
        	
            MqttMessage msg = new MqttMessage(msgString.getBytes());
            msg.setQos(0);
            msg.setRetained(false);
            mqttClient.publish(topic,msg);
            
            logger.debug("Data Correctly Published !");
        }
        else{
            logger.error("Error: Topic or Msg = Null or MQTT Client is not Connected !");
        }

    }



}
