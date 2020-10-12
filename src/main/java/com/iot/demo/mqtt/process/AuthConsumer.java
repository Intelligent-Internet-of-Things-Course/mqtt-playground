package com.iot.demo.mqtt.process;

import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class AuthConsumer {

    private final static Logger logger = LoggerFactory.getLogger(AuthConsumer.class);
    
    private static String BROKER_ADDRESS = "155.185.228.19";
    
    private static int BROKER_PORT = 7883;

    private static final String MQTT_USERNAME = "000001";

    private static final String MQTT_PASSWORD = "lgmoihgr";

    private static final String MQTT_BASIC_TOPIC = "/iot/user/000001/";

    public static void main(String [ ] args) {

    	logger.info("MQTT Consumer Tester Started ...");

        try{

            String clientId = UUID.randomUUID().toString();

            MqttClientPersistence persistence = new MemoryPersistence();

            IMqttClient client = new MqttClient(
                    String.format("tcp://%s:%d", BROKER_ADDRESS, BROKER_PORT),
                    clientId,
                    persistence);

            MqttConnectOptions options = new MqttConnectOptions();
            options.setUserName(MQTT_USERNAME);
            options.setPassword(new String(MQTT_PASSWORD).toCharArray());
            options.setAutomaticReconnect(true);
            options.setCleanSession(true);
            options.setConnectionTimeout(10);
            client.connect(options);

            logger.info("Connected !");
            
            client.subscribe(MQTT_BASIC_TOPIC + "#", (topic, msg) -> {
            	byte[] payload = msg.getPayload();
                logger.info("Message Received ({}) Message Received: {}", topic, new String(payload));
            });

        }catch (Exception e){
            e.printStackTrace();
        }

    }
}
