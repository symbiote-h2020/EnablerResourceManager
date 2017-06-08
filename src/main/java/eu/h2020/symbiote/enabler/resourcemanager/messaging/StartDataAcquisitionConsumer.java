package eu.h2020.symbiote.enabler.resourcemanager.messaging;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;

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
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.core.ParameterizedTypeReference;

import java.io.IOException;
import java.util.Queue;
import java.util.Iterator;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.core.JsonParseException;

import eu.h2020.symbiote.enabler.messaging.model.*;
import eu.h2020.symbiote.core.model.resources.Resource;
import eu.h2020.symbiote.core.ci.QueryResponse;
import eu.h2020.symbiote.core.ci.QueryResourceResult;
import eu.h2020.symbiote.security.token.Token;
import eu.h2020.symbiote.security.constants.AAMConstants;

import eu.h2020.symbiote.enabler.resourcemanager.utils.SecurityManager;


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

    @Autowired
    private SecurityManager securityManager;

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

        ObjectMapper mapper = new ObjectMapper();
        String requestInString = new String(body, "UTF-8");
            ResourceManagerAcquisitionStartResponse response  = new ResourceManagerAcquisitionStartResponse();
            ArrayList<ResourceManagerTaskInfoResponse> responseList = new ArrayList<ResourceManagerTaskInfoResponse>();
            ArrayList<PlatformProxyAcquisitionStartRequest> messagesToPlatformProxy = new ArrayList<PlatformProxyAcquisitionStartRequest>();

        log.info("Received StartDataAcquisition request : " + requestInString);

        try {

            ResourceManagerAcquisitionStartRequest request  = mapper.readValue(requestInString, ResourceManagerAcquisitionStartRequest.class);
            
            for (Iterator<ResourceManagerTaskInfoRequest> iter = request.getResources().iterator(); iter.hasNext();) {
                ResourceManagerTaskInfoRequest taskInfoRequest = (ResourceManagerTaskInfoRequest) iter.next();
                
                // Building the query url for each task
                String url = symbIoTeCoreUrl + "/query?";
                if (taskInfoRequest.getLocation() != null)
                    url += "location_name=" + taskInfoRequest.getLocation();
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


                // Query the core for each task
                HttpHeaders httpHeaders = new HttpHeaders();
                httpHeaders.set("Accept", MediaType.APPLICATION_JSON_VALUE);
                httpHeaders.set("X-Auth-Token", "Token");
                httpHeaders.setContentType(MediaType.APPLICATION_JSON);
                HttpEntity<String> entity = new HttpEntity<>(httpHeaders); 

                // Building the response for each task
                ResourceManagerTaskInfoResponse taskInfoResponse = new ResourceManagerTaskInfoResponse(taskInfoRequest);
                ArrayList<String> resourceIds = new ArrayList<String>();
                PlatformProxyAcquisitionStartRequest requestToPlatformProxy = new PlatformProxyAcquisitionStartRequest();
                ArrayList<PlatformProxyResourceInfo> platformProxyResources = new ArrayList<PlatformProxyResourceInfo>();

                // FIX ME: Consider Connection timeouts or errors
                try {
                    ResponseEntity<QueryResponse> queryResponseEntity = restTemplate.exchange(
                    url, HttpMethod.GET, entity, QueryResponse.class);

                    log.info("SymbIoTe Core Response: " + mapper.writeValueAsString(queryResponseEntity));

                    QueryResponse queryResponse = queryResponseEntity.getBody();
                    List<QueryResourceResult> queryResult = queryResponse.getResources();
                    Integer count = 0;

                    for (Iterator<QueryResourceResult> it = queryResult.iterator(); it.hasNext() && count < taskInfoResponse.getCount();) {
                        QueryResourceResult resource = (QueryResourceResult) it.next();
                        
                        
                        try {
                            Token token = securityManager.requestPlatformToken(resource.getPlatformId());
                            log.info("Platform Token from platform " + resource.getPlatformId() + " acquired: " + token);
                            
                            // Request resourceUrl from CRAM
                            String cramRequestUrl = symbIoTeCoreUrl + "/resourceUrls?id=" + resource.getId();
                            HttpHeaders cramHttpHeaders = new HttpHeaders();
                            cramHttpHeaders.set("Accept", MediaType.APPLICATION_JSON_VALUE);
                            // cramHttpHeaders.set(AAMConstants.TOKEN_HEADER_NAME, token.getToken());
                            cramHttpHeaders.set(AAMConstants.TOKEN_HEADER_NAME, securityManager.requestCoreToken().getToken());
                            cramHttpHeaders.setContentType(MediaType.APPLICATION_JSON);
                            HttpEntity<String> cramEntity = new HttpEntity<>(cramHttpHeaders); 
                            ParameterizedTypeReference<Map<String, String>> typeRef = new ParameterizedTypeReference<Map<String, String>>() {};
                            ResponseEntity<Map<String, String>> cramResponseEntity = restTemplate.exchange(
                                                    cramRequestUrl, HttpMethod.GET, cramEntity, typeRef);
                            Map<String, String> cramResponse = cramResponseEntity.getBody();

                            log.info("CRAM Response: " + cramResponse);

                            if (cramResponse != null) {
                                String resourceUrl = cramResponse.get(resource.getId());
                                if (resourceUrl != null) {
                                    // Building the request to PlatformProxy
                                    PlatformProxyResourceInfo platformProxyResourceInfo = new PlatformProxyResourceInfo();
                                    platformProxyResourceInfo.setResourceId(resource.getId());
                                    platformProxyResourceInfo.setAccessURL(resourceUrl);
                                    platformProxyResources.add(platformProxyResourceInfo);
                                    count++;

                                    // Save the id for returning it to the EnablerLogic
                                    resourceIds.add(resource.getId());
                                }
                            }
                        } catch (SecurityException e) {
                            log.info(e.toString());
                        } catch (HttpClientErrorException e) {
                            log.info(e.getStatusCode());
                            log.info(e.getResponseBodyAsString());             
                        } 
                    }
           
                    // Finalizing task response to EnablerLogic
                    taskInfoResponse.setResourceIds(resourceIds);

                    // Finallizing request to PlatformProxy
                    requestToPlatformProxy.setTaskId(taskInfoResponse.getTaskId());
                    requestToPlatformProxy.setInterval(taskInfoResponse.getInterval());
                    requestToPlatformProxy.setResources(platformProxyResources);

                    // Store all requests to PlatformProxy
                    messagesToPlatformProxy.add(requestToPlatformProxy);
                }
                catch (HttpClientErrorException e) {
                    log.info(e.getStatusCode());
                    log.info(e.getResponseBodyAsString());             
                } 

                responseList.add(taskInfoResponse);

            }
        } catch (JsonParseException | JsonMappingException e) {
            log.error("Error occured during deserializing ResourceManagerAcquisitionStartRequest", e);
        }

        // Sending response to EnablerLogic
        response.setResources(responseList);
        rabbitTemplate.convertAndSend(properties.getReplyTo(), response,
        m -> {
                m.getMessageProperties().setCorrelationIdString(properties.getCorrelationId());
                return m;
             });

       // Sending requests to PlatformProxy
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
