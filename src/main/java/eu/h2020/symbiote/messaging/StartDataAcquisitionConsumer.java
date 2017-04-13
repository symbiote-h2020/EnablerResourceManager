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

import java.io.IOException;
import java.util.Queue;
import java.util.Iterator;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;

import eu.h2020.symbiote.model.*;
import eu.h2020.symbiote.core.model.resources.Resource;

/**
 * RabbitMQ Consumer implementation used for getting the resource details from Enabler Logic
 *
 * Created by vasgl
 */
public class StartDataAcquisitionConsumer extends DefaultConsumer {

    private static Log log = LogFactory.getLog(StartDataAcquisitionConsumer.class);
    private RabbitManager rabbitManager;

    @Autowired
    private AsyncRestTemplate asyncRestTemplate;

    @Autowired
    private RestTemplate restTemplate;

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Value("${rabbit.exchange.enablerPlatformProxy.name}") 
    private String platformProxyExchange; 

    @Value("${rabbit.routingKey.enablerPlatformProxy.acquisitionStartRequested}") 
    private String platformProxyAcquisitionStartRequestedRoutingKey; 

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

        Gson gson = new Gson();
        EnablerLogicAcquisitionStartRequest request  = gson.fromJson(requestInString, EnablerLogicAcquisitionStartRequest.class);
        EnablerLogicAcquisitionStartResponse response  = new EnablerLogicAcquisitionStartResponse();
        ArrayList<EnablerLogicTaskInfoResponse> responseList = new ArrayList<EnablerLogicTaskInfoResponse>();
        ArrayList<PlatformProxyAcquisitionStartRequest> messagesToPlatformProxy = new ArrayList<PlatformProxyAcquisitionStartRequest>();

        for (Iterator<EnablerLogicTaskInfoRequest> iter = request.getResources().iterator(); iter.hasNext();) {
            EnablerLogicTaskInfoRequest taskInfoRequest = (EnablerLogicTaskInfoRequest) iter.next();
            
            String url = symbIoTeCoreUrl + "/query?";
            if (taskInfoRequest.getLocation() != null)
                url += "location=" + taskInfoRequest.getLocation();
            if (taskInfoRequest.getObservesProperty() != null) {
                url += "&observed_property=";
   
                for (Iterator<String> it = taskInfoRequest.getObservesProperty().iterator(); it.hasNext();) {
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
            HttpEntity<String> entity = new HttpEntity<>(httpHeaders); 

            ResponseEntity<Resource[]> queryResponse = restTemplate.exchange(
                url, HttpMethod.GET, entity, Resource[].class);

            log.info("SymbIoTe Core Response: " + queryResponse);

            EnablerLogicTaskInfoResponse taskInfoResponse = new EnablerLogicTaskInfoResponse(taskInfoRequest);
            ArrayList<String> resourceIds = new ArrayList<String>();
            PlatformProxyAcquisitionStartRequest requestToPlatformProxy = new PlatformProxyAcquisitionStartRequest();
            ArrayList<PlatformProxyResourceInfo> platformProxyResources = new ArrayList<PlatformProxyResourceInfo>();


            List<Resource> queryResult = Arrays.asList(queryResponse.getBody());
            Integer count = 0;

            for (Iterator<Resource> it = queryResult.iterator(); it.hasNext() && count < taskInfoResponse.getCount(); count++) {
                Resource resource = (Resource) it.next();

                resourceIds.add(resource.getId());

                PlatformProxyResourceInfo platformProxyResourceInfo = new PlatformProxyResourceInfo();
                platformProxyResourceInfo.setResourceId(resource.getId());
                platformProxyResourceInfo.setAccessURL(resource.getInterworkingServiceURL());
                platformProxyResources.add(platformProxyResourceInfo);
            }

            taskInfoResponse.setResourceIds(resourceIds);
            responseList.add(taskInfoResponse);
            requestToPlatformProxy.setTaskId(taskInfoResponse.getTaskId());
            requestToPlatformProxy.setInterval(taskInfoResponse.getInterval());
            requestToPlatformProxy.setResources(platformProxyResources);
            messagesToPlatformProxy.add(requestToPlatformProxy);
        }

        response.setResources(responseList);
        rabbitTemplate.convertAndSend(properties.getReplyTo(), response,
        m -> {
                m.getMessageProperties().setCorrelationIdString(properties.getCorrelationId());
                return m;
             });

       for (Iterator<PlatformProxyAcquisitionStartRequest> it = messagesToPlatformProxy.iterator(); it.hasNext();) {
            PlatformProxyAcquisitionStartRequest req = (PlatformProxyAcquisitionStartRequest) it.next();

            rabbitTemplate.convertAndSend(platformProxyExchange, platformProxyAcquisitionStartRequestedRoutingKey, req,
            m -> {
                    m.getMessageProperties().setCorrelationIdString(properties.getCorrelationId());
                    return m;
                 }); 
        }

    }
}
