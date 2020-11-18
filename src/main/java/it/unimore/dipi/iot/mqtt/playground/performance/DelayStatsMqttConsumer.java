package it.unimore.dipi.iot.mqtt.playground.performance;

import com.google.gson.Gson;
import it.unimore.dipi.iot.mqtt.playground.model.MessageDescriptor;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Simple MQTT Consumer measuring transmission delay
 *
 * @author Marco Picone, Ph.D. - picone.m@gmail.com
 * @project mqtt-playground
 * @created 14/10/2020 - 09:19
 */
public class DelayStatsMqttConsumer {

    private final static Logger logger = LoggerFactory.getLogger(DelayStatsMqttConsumer.class);

    //TODO Update it with correct your base topic associated to the MQTT User (If Necessary)
    private static final String TARGET_TOPIC = "/iot/performance/#";

    private static final int MESSAGE_NUMBER_LIMIT = 10;

    private static Map<Long, String> messageMap = new HashMap<>();

    private static int receivedMessageCount = 0;

    private static long startTime;

    public static void main(String [ ] args) {

    	logger.info("MQTT Consumer Tester Started ...");

        try{

            //Generate a random MQTT client ID using the UUID class
            String clientId = UUID.randomUUID().toString();
            MqttClientPersistence persistence = new MemoryPersistence();
            IMqttClient client = new MqttClient(
                    DelayTestMqttConfiguration.BROKER_URL, //Create the URL from IP and PORT
                    clientId,
                    persistence);

            MqttConnectOptions options = new MqttConnectOptions();

            if(DelayTestMqttConfiguration.isAuthenticationRequired){
                options.setUserName(DelayTestMqttCredentials.MQTT_USERNAME);
                options.setPassword(new String(DelayTestMqttCredentials.MQTT_PASSWORD).toCharArray());
            }

            options.setAutomaticReconnect(true);
            options.setCleanSession(true);
            options.setConnectionTimeout(10);

            //Connect to the target broker
            client.connect(options);

            logger.info("Connected ! Client Id: {}", clientId);

            startTime = System.currentTimeMillis();

            //Subscribe to the target topic #. In that case the consumer will receive (if authorized) all the message
            //passing through the broker
            client.subscribe(TARGET_TOPIC, (topic, msg) -> {

                //The topic variable contain the specific topic associated to the received message. Using MQTT wildcards
                //messaged from multiple and different topic can be received with the same subscription
                //The msg variable is a MqttMessage object containing all the information about the received message
                receivedMessageCount ++;

                if(messageMap.size() == MESSAGE_NUMBER_LIMIT || receivedMessageCount == MESSAGE_NUMBER_LIMIT || (System.currentTimeMillis() - startTime) > (1000*60*5)) {
                    client.unsubscribe(TARGET_TOPIC);
                    startStatsThread();
                }
                else {
                    logger.info("Message Processed: {}", receivedMessageCount);
                    messageMap.put(System.currentTimeMillis(), new String(msg.getPayload()));
                }

            });

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private static void startStatsThread(){

       logger.info("Starting Statistics Thread ...");

       new Thread(new Runnable() {
           @Override
           public void run() {

               logger.info("Total Received Messages: {}/{}", receivedMessageCount, MESSAGE_NUMBER_LIMIT);

               List<Long> delayList = new ArrayList<>();

               messageMap.entrySet().forEach(messageEntry -> {
                    try{

                        long incomingTimestamp = messageEntry.getKey();
                        String payloadString = messageEntry.getValue();
                        MessageDescriptor messageDescriptor = parseJsonMessage(payloadString.getBytes());

                        long diff = incomingTimestamp - messageDescriptor.getTimestamp();

                        delayList.add(diff);

                        logger.info("Delay: {}", diff);

                    }catch (Exception e){
                        e.printStackTrace();
                    }
               });

               long sum = 0;
               for(long delay: delayList)
                   sum = sum + delay;

               long averageDelay = sum / (long)delayList.size();

               logger.info("AVERAGE DELAY: {}", averageDelay);
           }
       }).start();

    }

    /**
     * Parse the received MQTT message into a MessageDescriptor object or null in case of error
     *
     * @param payload
     * @return the parsed MessageDescriptor object or null in case or error.
     */
    public static MessageDescriptor parseJsonMessage(byte[] payload) {

        try {

            Gson gson = new Gson();
            return (MessageDescriptor)gson.fromJson(new String(payload), MessageDescriptor.class);

        }catch(Exception e) {
            return null;
        }

    }

}
