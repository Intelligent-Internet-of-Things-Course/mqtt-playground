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

/**
 * MQTT Consumer using the library Eclipse Paho
 * and consuming JSON messages following a structured message description
 * It consumes also device information and descriptors published as retained messages by active things.
 *
 * @author Marco Picone, Ph.D. - picone.m@gmail.com
 * @project mqtt-playground
 * @created 14/10/2020 - 09:19
 */
public class DeviceConsumer {

    private final static Logger logger = LoggerFactory.getLogger(DeviceConsumer.class);

    //IP Address of the target MQTT Broker
    private static String BROKER_ADDRESS = "127.0.0.1";

    //PORT of the target MQTT Broker
    private static int BROKER_PORT = 1883;
    
    private static Gson gson = new Gson();

    public static void main(String [ ] args) {

    	logger.info("MQTT DeviceConsumer Tester Started ...");

        try{

            //Generate a random MQTT client ID using the UUID class
            String publisherId = UUID.randomUUID().toString();

            //Represents a persistent data store, used to store outbound and inbound messages while they
            //are in flight, enabling delivery to the QoS specified. In that case use a memory persistence.
            //When the application stops all the temporary data will be deleted.
            MqttClientPersistence persistence = new MemoryPersistence();

            //The the persistence is not passed to the constructor the default file persistence is used.
            //In case of a file-based storage the same MQTT client UUID should be used
            IMqttClient subscriber = new MqttClient(
                    String.format("tcp://%s:%d", BROKER_ADDRESS, BROKER_PORT),
                    publisherId,
                    persistence);

            //Define MQTT Connection Options such as reconnection, persistent/clean session and connection timeout
            //Authentication option can be added -> See AuthProducer example
            MqttConnectOptions options = new MqttConnectOptions();
            options.setAutomaticReconnect(true);
            options.setCleanSession(true);
            options.setConnectionTimeout(10);

            //Connect to the target broker
            subscriber.connect(options);

            logger.info("Connected !");

            //Subscribe to device information
            subscriber.subscribe("device/+/info", (topic, msg) -> {

            	byte[] payload = msg.getPayload();

            	//Parse DeviceDescriptor with a dedicated internal method
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
            
            //Subscribe to device incoming telemetry data
            subscriber.subscribe("device/+/sensor/#", (topic, msg) -> {

            	byte[] payload = msg.getPayload();

            	//Parse DeviceDescriptor with a dedicated internal method
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

    /**
     * Parse the received MQTT message into a DeviceDescriptor object or null in case of error
     *
     * @param payload
     * @return the parsed DeviceDescriptor object or null in case or error.
     */
    public static DeviceDescriptor parseDeviceJsonMessage(byte[] payload) {
    	
    	try {
    		return (DeviceDescriptor)gson.fromJson(new String(payload), DeviceDescriptor.class);
    	}catch(Exception e) {
    		return null;
    	}
    	
    }

    /**
     * Parse the received MQTT message into a MessageDescriptor object or null in case of error
     *
     * @param payload
     * @return the parsed MessageDescriptor object or null in case or error.
     */
    public static MessageDescriptor parseJsonMessage(byte[] payload) {
    	try {		
    		return (MessageDescriptor)gson.fromJson(new String(payload), MessageDescriptor.class);
    	}catch(Exception e) {
    		return null;
    	}
    	
    }
}
