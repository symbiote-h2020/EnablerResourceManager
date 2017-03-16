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
import org.springframework.http.ResponseEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpEntity;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import org.json.simple.JSONObject;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.springframework.util.concurrent.ListenableFuture;

/**
 * RabbitMQ Consumer implementation used for getting the resource details from Enabler Logic
 *
 * Created by vasgl
 */
public class GetResourceDetailsConsumer extends DefaultConsumer {

    private static Log log = LogFactory.getLog(GetResourceDetailsConsumer.class);
    private RabbitManager rabbitManager;

    private final Queue<ListenableFuture<ResponseEntity<JSONObject>>> futuresQueue = 
                   new ConcurrentLinkedQueue<ListenableFuture<ResponseEntity<JSONObject>>>();

    @Autowired
    private AsyncRestTemplate asyncRestTemplate;

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
     * @param repositoryManager repository manager bean passed for persistence actions
     */
    public GetResourceDetailsConsumer(Channel channel,
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
        String url = symbIoTeCoreUrl;
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.set("Accept", MediaType.APPLICATION_JSON_VALUE);
        // httpHeaders.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> entity = new HttpEntity<>("test", httpHeaders); 

        // Gson gson = new Gson();
        // JSONObject jsonResponse = new JSONObject();
        // jsonResponse.put("status", "ok");

        // AMQP.BasicProperties replyProps = new AMQP.BasicProperties
        //         .Builder()
        //         .correlationId(properties.getCorrelationId())
        //         .contentType("application/json")
        //         .build();

        // PlaceholderResponse placeholderResponse = new PlaceholderResponse();
        // try {

            // do stuff
            // placeholderResponse.setStatus(200);

        // } catch (Exception e) {
        //     log.error("Error occurred during Placeholder", e);
        //     placeholderResponse.setStatus(400);
        // }

        
       // this.getChannel().basicPublish("", properties.getReplyTo(), replyProps, response.getBytes());
        // rabbitManager.rabbitTemplate.convertAndSend(properties.getReplyTo(), jsonResponse,
        //     m -> {
        //             // m.setMessageProperties(properties);
        //             m.getMessageProperties().setCorrelationIdString(properties.getCorrelationId());
        //             log.info(m.getMessageProperties());
        //             return m;
        //          });

        ListenableFuture<ResponseEntity<JSONObject>> future = asyncRestTemplate.exchange(
            url, HttpMethod.POST, entity, JSONObject.class);

        RestAPICallback<ResponseEntity<JSONObject>> callback = 
            new RestAPICallback<ResponseEntity<JSONObject>> (message, properties, futuresQueue, future, rabbitTemplate);
        future.addCallback(callback);
        
        futuresQueue.add(future);

    }
}
