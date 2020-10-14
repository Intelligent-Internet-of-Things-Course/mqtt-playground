package it.unimore.dipi.iot.mqtt.playground.process;

import com.google.gson.Gson;
import it.unimore.dipi.iot.mqtt.playground.model.DeviceDescriptor;
import it.unimore.dipi.iot.mqtt.playground.model.MessageDescriptor;
import it.unimore.dipi.iot.mqtt.playground.model.EngineTemperatureSensor;
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

/**
 * Simple MQTT Producer using the library Eclipse Paho
 * and generating JSON structured messages.
 * Furthermore, it publish its DeviceDescriptor as a retained message in order to allow new consumers
 * to gather device information at connection.
 *
 * @author Marco Picone, Ph.D. - picone.m@gmail.com
 * @project mqtt-playground
 * @created 14/10/2020 - 09:19
 */
public class DeviceProducer {

    private final static Logger logger = LoggerFactory.getLogger(DeviceProducer.class);

    //BROKER URL
    private static String BROKER_URL = "tcp://127.0.0.1:1883";

    //Message Limit generated and sent by the producer
    private static final int MESSAGE_COUNT = 1000;

    //Topic used to publish device information
    private static final String DEVICE_TOPIC = "device";

    //Topic used to publish generated demo data
    private static final String SENSOR_TOPIC = "sensor/temperature";

    //Internal GSON instance to generate and parse JSON messages
    private static Gson gson = new Gson();
    
    public static void main(String[] args) {

        logger.info("DeviceProducer started ...");

        try{

            //Generate a random MQTT client ID using the UUID class
            String publisherId = UUID.randomUUID().toString();

            //Represents a persistent data store, used to store outbound and inbound messages while they
            //are in flight, enabling delivery to the QoS specified. In that case use a memory persistence.
            //When the application stops all the temporary data will be deleted.
            MqttClientPersistence persistence = new MemoryPersistence();

            //The the persistence is not passed to the constructor the default file persistence is used.
            //In case of a file-based storage the same MQTT client UUID should be used
            IMqttClient client = new MqttClient(BROKER_URL,publisherId, persistence);

            //Define MQTT Connection Options such as reconnection, persistent/clean session and connection timeout
            //Authentication option can be added -> See AuthProducer example
            MqttConnectOptions options = new MqttConnectOptions();
            options.setAutomaticReconnect(true);
            options.setCleanSession(true);
            options.setConnectionTimeout(10);

            //Connect to the target broker
            client.connect(options);

            logger.info("Connected !");
            
            //Create a new descriptor for the device
            DeviceDescriptor deviceDescriptor = new DeviceDescriptor(UUID.randomUUID().toString(), "ACME_CORPORATION", "0.1-beta");

            //Internal method to publish the device information as retained messages
            publishDeviceInfo(client, deviceDescriptor);

            //Create an instance of an Engine Temperature Sensor
            EngineTemperatureSensor engineTemperatureSensor = new EngineTemperatureSensor();

            //Start to publish MESSAGE_COUNT messages
            for(int i = 0; i < MESSAGE_COUNT; i++) {

                //Get updated temperature value and build the associated Json Message
                //through the internal method buildJsonMessage
            	double sensorValue = engineTemperatureSensor.getTemperatureValue();
            	String payloadString = buildJsonMessage(sensorValue);

                //Internal Method to publish MQTT data using the created MQTT Client
            	if(payloadString != null)
            	    //The topic is combined with a hierarchical structure
            		publishData(client, String.format("%s/%s/%s", DEVICE_TOPIC, deviceDescriptor.getDeviceId(), SENSOR_TOPIC), payloadString);
            	else
            		logger.error("Skipping message send due to NULL Payload !");
            	
            	Thread.sleep(1000);
            }

            //Disconnect from the broker and close connection
            client.disconnect();
            client.close();

            logger.info("Disconnected !");

        }catch (Exception e){
            e.printStackTrace();
        }

    }

    /**
     * Publish Device information as a retained message
     *
     * @param mqttClient
     * @param deviceDescriptor
     */
    public static void publishDeviceInfo(IMqttClient mqttClient, DeviceDescriptor deviceDescriptor) {
    	
    	try {
    		
            if (mqttClient.isConnected() ) {
            	
            	String topic = String.format("%s/%s/info", DEVICE_TOPIC, deviceDescriptor.getDeviceId()); 
            	
                MqttMessage msg = new MqttMessage(gson.toJson(deviceDescriptor).getBytes());
                msg.setQos(0);
                msg.setRetained(true);
                mqttClient.publish(topic,msg);
                
                logger.debug("Device Data Correctly Published !");
            }
            else{
                logger.error("Error: Topic or Msg = Null or MQTT Client is not Connected !");
            }
    		
    	}catch(Exception e) {
    		logger.error("Error Publishing Device Information ! Error: {}", e.getLocalizedMessage());
    	}
    	
    }

    /**
     * Create structure JSON message starting from the passed sensorValue
     * and using the MessageDescriptor class
     *
     * @param sensorValue
     * @return
     */
    public static String buildJsonMessage(double sensorValue) {
    	
    	try {
    		    	
        	MessageDescriptor messageDescriptor = new MessageDescriptor(System.currentTimeMillis(), "ENGINE_TEMPERATURE_SENSOR", sensorValue);
        	
        	String jsonStringPayload = gson.toJson(messageDescriptor);
        	
        	return jsonStringPayload;
    		
    	}catch(Exception e) {
    		logger.error("Error creating json payload ! Message: {}", e.getLocalizedMessage());
    		return null;
    	}
    }

    /**
     * Send a target String Payload to the specified MQTT topic
     *
     * @param mqttClient
     * @param topic
     * @param msgString
     * @throws MqttException
     */
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
