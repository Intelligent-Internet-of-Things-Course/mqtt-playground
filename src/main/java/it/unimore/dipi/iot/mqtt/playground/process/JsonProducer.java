package it.unimore.dipi.iot.mqtt.playground.process;

import com.google.gson.Gson;
import it.unimore.dipi.iot.mqtt.playground.model.MessageDescriptor;
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

/**
 * Simple MQTT Producer using the library Eclipse Paho
 * and generating JSON structured messages
 *
 * @author Marco Picone, Ph.D. - picone.m@gmail.com
 * @project mqtt-playground
 * @created 14/10/2020 - 09:19
 */
public class JsonProducer {

    private final static Logger logger = LoggerFactory.getLogger(JsonProducer.class);

    //BROKER URL
    private static String BROKER_URI = "tcp://127.0.0.1:1883";

    //Message Limit generated and sent by the producer
    private static final int MESSAGE_COUNT = 1000;

    //Topic used to publish generated demo data
    private static final String TOPIC = "sensor/temperature";
    
    public static void main(String[] args) {

        logger.info("JsonProducer started ...");

        try{

            //Generate a random MQTT client ID using the UUID class
            String mqttClientId = UUID.randomUUID().toString();

            //Represents a persistent data store, used to store outbound and inbound messages while they
            //are in flight, enabling delivery to the QoS specified. In that case use a memory persistence.
            //When the application stops all the temporary data will be deleted.
            MqttClientPersistence persistence = new MemoryPersistence();

            //The the persistence is not passed to the constructor the default file persistence is used.
            //In case of a file-based storage the same MQTT client UUID should be used
            IMqttClient client = new MqttClient(BROKER_URI,mqttClientId, persistence);

            //Define MQTT Connection Options such as reconnection, persistent/clean session and connection timeout
            //Authentication option can be added -> See AuthProducer example
            MqttConnectOptions options = new MqttConnectOptions();
            options.setAutomaticReconnect(true);
            options.setCleanSession(true);
            options.setConnectionTimeout(10);
            client.connect(options);

            //Connect to the target broker
            logger.info("Connected !");

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
            		publishData(client, TOPIC, payloadString);
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
     * Create structure JSON message starting from the passed sensorValue
     * and using the MessageDescriptor class
     *
     * @param sensorValue
     * @return
     */
    public static String buildJsonMessage(double sensorValue) {
    	
    	try {
    		
    		Gson gson = new Gson();
        	
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
