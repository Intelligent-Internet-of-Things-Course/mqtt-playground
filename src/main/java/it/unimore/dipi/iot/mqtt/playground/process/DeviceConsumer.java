package it.unimore.dipi.iot.mqtt.playground.process;

import it.unimore.dipi.iot.mqtt.playground.model.DeviceDescriptor;
import it.unimore.dipi.iot.mqtt.playground.model.MessageDescriptor;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import java.util.UUID;

public class DeviceConsumer {

    private final static Logger logger = LoggerFactory.getLogger(DeviceConsumer.class);
    
    private static String BROKER_ADDRESS = "127.0.0.1";
    
    private static int BROKER_PORT = 1883;
    
    private static Gson gson = new Gson();

    public static void main(String [ ] args) {

    	logger.info("MQTT Consumer Tester Started ...");

        try{

            String publisherId = UUID.randomUUID().toString();

            MqttClientPersistence persistence = new MemoryPersistence();

            IMqttClient subscriber = new MqttClient(
                    String.format("tcp://%s:%d", BROKER_ADDRESS, BROKER_PORT),
                    publisherId,
                    persistence);

            MqttConnectOptions options = new MqttConnectOptions();
            options.setAutomaticReconnect(true);
            options.setCleanSession(true);
            options.setConnectionTimeout(10);
            subscriber.connect(options);

            logger.info("Connected !");

            //Subscribe to device information
            subscriber.subscribe("device/+/info", (topic, msg) -> {

            	byte[] payload = msg.getPayload();

            	DeviceDescriptor deviceDescriptor = parseDeviceJsonMessage(payload);

                if(deviceDescriptor != null)
                	logger.info("Device Descriptor ({}) Data -> Id: {}, Producer: {}, Software Version: {}", 
                			topic, 
                			deviceDescriptor.getDeviceId(), 
                			deviceDescriptor.getProducer(), 
                			deviceDescriptor.getSoftwareVersion());
                else
                	logger.info("Message Received ({}) Message Received: {}", topic, new String(payload));
            });
            
            //Subscribe to all incoming telemetry data
            subscriber.subscribe("device/+/sensor/#", (topic, msg) -> {

            	byte[] payload = msg.getPayload();

                MessageDescriptor msgDescriptor = parseJsonMessage(payload);

                if(msgDescriptor != null)
                	logger.info("MessageDescriptor Received ({}) Data -> Timestamp: {}, Type: {}, Value: {}", topic, msgDescriptor.getTimestamp(), msgDescriptor.getType(), msgDescriptor.getValue());
                else
                	logger.info("Message Received ({}) Message Received: {}", topic, new String(payload));
            });

        }catch (Exception e){
            e.printStackTrace();
        }

    }
    
    public static DeviceDescriptor parseDeviceJsonMessage(byte[] payload) {
    	
    	try {
    		return (DeviceDescriptor)gson.fromJson(new String(payload), DeviceDescriptor.class);
    	}catch(Exception e) {
    		return null;
    	}
    	
    }
    
    public static MessageDescriptor parseJsonMessage(byte[] payload) {
    	try {		
    		return (MessageDescriptor)gson.fromJson(new String(payload), MessageDescriptor.class);
    	}catch(Exception e) {
    		return null;
    	}
    	
    }
}
