package com.iot.demo.mqtt.process;

import com.iot.demo.mqtt.sensors.EngineTemperatureSensor;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class AuthProducer {

    private final static Logger logger = LoggerFactory.getLogger(AuthProducer.class);
    
    private static String SERVER_URI = "tcp://155.185.228.19:7883";

    private static final int MESSAGE_COUNT = 1000;

    private static final String MQTT_USERNAME = "000001";

    private static final String MQTT_PASSWORD = "lgmoihgr";

    private static final String MQTT_BASIC_TOPIC = "/iot/user/000001/";

    private static final String TOPIC = "sensor/temperature";

    public static void main(String[] args) {

        logger.info("Auth SimpleProducer started ...");

        try{
        	
            String publisherId = UUID.randomUUID().toString();

            //Define e Persistence implemented through a local memory
            MqttClientPersistence persistence = new MemoryPersistence();
            
            IMqttClient client = new MqttClient(SERVER_URI, publisherId, persistence);
            
            MqttConnectOptions options = new MqttConnectOptions();
            options.setUserName(MQTT_USERNAME);
            options.setPassword(new String(MQTT_PASSWORD).toCharArray());
            options.setAutomaticReconnect(true);
            options.setCleanSession(true);
            options.setConnectionTimeout(10);
            
            client.connect(options);

            logger.info("Connected !");
            
            EngineTemperatureSensor engineTemperatureSensor = new EngineTemperatureSensor();
            
            for(int i = 0; i < MESSAGE_COUNT; i++) {
            	
            	double sensorValue = engineTemperatureSensor.getTemperatureValue();
            	String payloadString = Double.toString(sensorValue);
            	publishData(client, MQTT_BASIC_TOPIC + TOPIC, payloadString);
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
            
            logger.debug("(If Authorized by Broker ACL) Data Correctly Published !");
        }
        else{
            logger.error("Error: Topic or Msg = Null or MQTT Client is not Connected !");
        }

    }



}
