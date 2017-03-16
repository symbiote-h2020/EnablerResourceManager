package eu.h2020.symbiote.messaging;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.http.ResponseEntity;

import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.rabbitmq.client.AMQP;

import org.json.simple.JSONObject;

import java.util.Map;
import java.util.Queue;

/**
* <h1>A Callback for listening to asynchronous REST replies </h1>
* This class extends the ListenableFutureCallback class and manually sends
* back the HTTP reply it receives to the specified "reply-To" queue of the
* RabbitMQ request received by the RPCServer.
*
* @author  Vasileios Glykantzis
* @version 1.0
* @since   2017-01-26
*/
public class RestAPICallback<T> implements ListenableFutureCallback<T> {

    private static Log log = LogFactory.getLog(RestAPICallback.class);

    private String request;
    private AMQP.BasicProperties properties;
    private ListenableFuture<ResponseEntity<JSONObject>> future;
    private RabbitTemplate rabbitTemplate;
    private Queue<ListenableFuture<ResponseEntity<JSONObject>>> futuresQueue;

   /**
   * Constructor of the RestAPICallback
   *
   * @param request String describing the type of request. Used in logging.
   * @param properties  The properties of the AMQP request
   * @param futuresQueue  The Queue containing all the not yet served ListenableFutures
   * @param future  The ListenableFuture which the class is set as callback for. 
   * @param rabbitTemplate  The RabbitTemplate used to send the Spring AMQP reply
   */
    public RestAPICallback(String request, AMQP.BasicProperties properties, Queue<ListenableFuture<ResponseEntity<JSONObject>>> futuresQueue,
            ListenableFuture<ResponseEntity<JSONObject>> future, RabbitTemplate rabbitTemplate) {
        this.request = request;
        this.properties = properties;  
        this.futuresQueue = futuresQueue;  
        this.future = future;
        this.rabbitTemplate = rabbitTemplate;    
    }


    @Override
    public void onSuccess(T result) {
        ResponseEntity responseEntity = (ResponseEntity) result;
        log.info(request + ": Successfully received response from server: " + result);

        rabbitTemplate.convertAndSend(properties.getReplyTo(), responseEntity.getBody(),
            m -> {
                    m.getMessageProperties().setCorrelationIdString(properties.getCorrelationId());
                    return m;
                 });

        futuresQueue.remove(future);
    }
 

    @Override
    public void onFailure(Throwable t) {
        log.info(request + ": Failed to fetch result from remote service", t);
        JSONObject newObject = new JSONObject();
        newObject.put("exception", t);

        rabbitTemplate.convertAndSend(properties.getReplyTo(), newObject,
            m -> {
                    m.getMessageProperties().setCorrelationIdString(properties.getCorrelationId());
                    return m;
                 });

        futuresQueue.remove(future);
    }
}