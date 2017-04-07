package eu.h2020.symbiote.messaging;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;

import com.google.gson.Gson;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.web.client.AsyncRestTemplate;
import org.springframework.web.client.RestTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpEntity;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import com.google.gson.Gson;
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.util.Queue;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.springframework.util.concurrent.ListenableFuture;

/**
 * RabbitMQ Consumer implementation used for getting the resource details from Enabler Logic
 *
 * Created by vasgl
 */
public class StartDataAcquisitionConsumer extends DefaultConsumer {

    private static Log log = LogFactory.getLog(StartDataAcquisitionConsumer.class);
    private RabbitManager rabbitManager;

    private final Queue<ListenableFuture<ResponseEntity<JSONObject>>> futuresQueue = 
                   new ConcurrentLinkedQueue<ListenableFuture<ResponseEntity<JSONObject>>>();

    @Autowired
    private AsyncRestTemplate asyncRestTemplate;

    @Autowired
    private RestTemplate restTemplate;

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    @Qualifier("symbIoTeCoreUrl")
    private String symbIoTeCoreUrl;

    /**
     * Constructs a new instance and records its association to the passed-in channel.
     * Managers beans passed as parameters because of lack of possibility to inject it to consumer.
     *
     * @param channel           the channel to which this consumer is attached
     * @param rabbitManager     rabbit manager bean passed for access to messages manager
     */
    public StartDataAcquisitionConsumer(Channel channel,
                                           RabbitManager rabbitManager) {
        super(channel);
        this.rabbitManager = rabbitManager;
    }

    /**
     * Called when a <code><b>basic.deliver</b></code> is received for this consumer.
     *
     * @param consumerTag the <i>consumer tag</i> associated with the consumer
     * @param envelope    packaging data for the message
     * @param properties  content header data for the message
     * @param body        the message body (opaque, client-specific byte array)
     * @throws IOException if the consumer encounters an I/O error while processing the message
     * @see Envelope
     */
    @Override
    public void handleDelivery(String consumerTag, Envelope envelope,
                               AMQP.BasicProperties properties, byte[] body)
            throws IOException {

        String message = "Search for resources";

        String requestInString = new String(body, "UTF-8");

        log.info("Received StartDataAcquisition request : " + requestInString);

        JSONParser parser = new JSONParser();
        JSONObject messageToEnablerLogic = new JSONObject();

        try {
            Object obj = parser.parse(requestInString);
            JSONObject requestObject = (JSONObject) obj;
            JSONArray requestArray = (JSONArray) requestObject.get("resources");

            if (requestArray.size() != 0) {

                for (Iterator<JSONObject> iter = requestArray.iterator(); iter.hasNext();) {
                    JSONObject resourceRequest = (JSONObject) iter.next();
                    log.info("DEBUG: " + resourceRequest);

                    String url = symbIoTeCoreUrl + "/query?";
                    if (resourceRequest.get("location") != null)
                        url += "location=" + resourceRequest.get("location");
                    if (resourceRequest.get("observesProperty") != null) {
                        url += "&observed_property=";
                        JSONArray observesProperty = (JSONArray) resourceRequest.get("observesProperty");
                        log.info("observesProperty= " + observesProperty);

                        for (Iterator<String> it = observesProperty.iterator(); it.hasNext();) {
                            String property = (String) it.next();
                            url += property + ',';
                            log.info("property = " + property + ", url = " + url);
                        }
                       url = url.substring(0, url.length() - 1);                   
                    }

                    log.info("url= " + url);

                    HttpHeaders httpHeaders = new HttpHeaders();
                    httpHeaders.set("Accept", MediaType.APPLICATION_JSON_VALUE);
                    // httpHeaders.setContentType(MediaType.APPLICATION_JSON);
                    HttpEntity<String> entity = new HttpEntity<>("test", httpHeaders); 

                    ResponseEntity<String> queryResponse = restTemplate.exchange(
                        url, HttpMethod.GET, entity, String.class);

                    log.info("SymbIoTe Core Response: " + queryResponse);

                    JSONParser parserOfQueryResult = new JSONParser();
                    try {

                        Object queryResponseObj = parser.parse((String) queryResponse.getBody());
                        JSONArray queryResult = (JSONArray) queryResponseObj;
                        log.info("queryResult: " + queryResult);

                        JSONArray resourceIds = new JSONArray();
                        Integer numberOfResourcesNeeded = Integer.parseInt((String) resourceRequest.get("count"));
                        Integer count = 0;

                        for (Iterator<JSONObject> it = queryResult.iterator(); it.hasNext() && count < numberOfResourcesNeeded; count++) {
                            JSONObject resource = (JSONObject) it.next();
                            resourceIds.add(resource.get("id"));
                        } 
                        resourceRequest.put("resourceIds", resourceIds);
                        log.info("resourceRequest: " + resourceRequest);

                        // ListenableFuture<ResponseEntity<JSONObject>> future = asyncRestTemplate.exchange(
                        //     url, HttpMethod.GET, entity, JSONObject.class);

                        // RestAPICallback<ResponseEntity<JSONObject>> callback = 
                        //     new RestAPICallback<ResponseEntity<JSONObject>> (message, properties, futuresQueue, future, rabbitTemplate);
                        // future.addCallback(callback);
                        
                        // futuresQueue.add(future);
                    }
                    catch (ParseException e) {}
                }
            }
        } 
        catch (ParseException e) {}
    }
}
