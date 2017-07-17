package eu.h2020.symbiote.enabler.resourcemanager.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.web.client.RestTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.http.MediaType;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpEntity;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.core.ParameterizedTypeReference;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.core.JsonParseException;

import eu.h2020.symbiote.enabler.messaging.model.*;
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
     */
    StartDataAcquisitionConsumer(Channel channel) {
        super(channel);
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
                               AMQP.BasicProperties properties, byte[] body) throws IOException {

        ObjectMapper mapper = new ObjectMapper();
        String requestInString = new String(body, "UTF-8");
        ResourceManagerAcquisitionStartResponse response  = new ResourceManagerAcquisitionStartResponse();
        QueryAndProcessSearchResponseResult queryAndProcessSearchResponseResult = new QueryAndProcessSearchResponseResult();

        log.info("Received StartDataAcquisition request : " + requestInString);

        try {
            ResourceManagerAcquisitionStartRequest request  = mapper.readValue(requestInString, ResourceManagerAcquisitionStartRequest.class);

            // Process each task request
            for (ResourceManagerTaskInfoRequest taskInfoRequest : request.getResources()) {
                String queryUrl = buildRequestUrl(symbIoTeCoreUrl, taskInfoRequest);
                QueryAndProcessSearchResponseResult newQueryAndProcessSearchResponseResult = queryAndProcessSearchResponse(mapper, queryUrl, taskInfoRequest);
                queryAndProcessSearchResponseResult.add(newQueryAndProcessSearchResponseResult);
            }

            // Sending response to EnablerLogic
            ArrayList<ResourceManagerTaskInfoResponse> resourceManagerTaskInfoResponseList = queryAndProcessSearchResponseResult.getResourceManagerTaskInfoResponseList();
            response.setResources(resourceManagerTaskInfoResponseList);
            rabbitTemplate.convertAndSend(properties.getReplyTo(), response,
                    m -> {
                        m.getMessageProperties().setCorrelationIdString(properties.getCorrelationId());
                        return m;
                    });

            // Sending requests to PlatformProxy
            ArrayList<PlatformProxyAcquisitionStartRequest> messagesToPlatformProxy = queryAndProcessSearchResponseResult.getPlatformProxyAcquisitionStartRequestList();
            for (PlatformProxyAcquisitionStartRequest req : messagesToPlatformProxy) {
                log.info("Sending requests to Platform Proxy");
                rabbitTemplate.convertAndSend(platformProxyExchange, platformProxyAcquisitionStartRequestedRoutingKey, req);
            }
        } catch (JsonParseException | JsonMappingException e) {
            log.error("Error occurred during deserializing ResourceManagerAcquisitionStartRequest", e);
        }


    }

    private String buildRequestUrl(String symbIoTeCoreUrl, ResourceManagerTaskInfoRequest taskInfoRequest) {
        // Building the query url for each task
        String url = symbIoTeCoreUrl + "/query?";
        if (taskInfoRequest.getLocation() != null)
            url += "location_name=" + taskInfoRequest.getLocation();
        if (taskInfoRequest.getObservesProperty() != null) {
            url += "&observed_property=";

            for (String property : taskInfoRequest.getObservesProperty()) {
                url += property + ',';
                log.info("property = " + property + ", url = " + url);
            }
            url = url.substring(0, url.length() - 1);
        }
        log.info("url= " + url);

        return url;
    }

    private QueryAndProcessSearchResponseResult queryAndProcessSearchResponse (ObjectMapper mapper, String queryUrl, ResourceManagerTaskInfoRequest taskInfoRequest)  {

        // Query the core for each task
        QueryAndProcessSearchResponseResult queryAndProcessSearchResponseResult = new QueryAndProcessSearchResponseResult();
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.set("Accept", MediaType.APPLICATION_JSON_VALUE);
        httpHeaders.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> entity = new HttpEntity<>(httpHeaders);

        ResourceManagerTaskInfoResponse taskInfoResponse = new ResourceManagerTaskInfoResponse(taskInfoRequest);
        PlatformProxyAcquisitionStartRequest requestToPlatformProxy = new PlatformProxyAcquisitionStartRequest();

        // FIX ME: Consider Connection timeouts or errors
        try {
            ResponseEntity<QueryResponse> queryResponseEntity = restTemplate.exchange(
                    queryUrl, HttpMethod.GET, entity, QueryResponse.class);

            try {
                log.info("SymbIoTe Core Response: " + mapper.writeValueAsString(queryResponseEntity));
            } catch (JsonProcessingException e) {
                log.info(e);
            }

            QueryResponse queryResponse = queryResponseEntity.getBody();
            TaskResponseToComponents taskResponseToComponents  = processSearchResponse(queryResponse, taskInfoRequest);

            // Finalizing task response to EnablerLogic
            taskInfoResponse.setResourceIds(taskResponseToComponents.getResourceIdsForEnablerLogic());

            // Finallizing request to PlatformProxy
            if (taskInfoRequest.getInformPlatformProxy()) {
                requestToPlatformProxy.setTaskId(taskInfoResponse.getTaskId());
                requestToPlatformProxy.setInterval(taskInfoResponse.getInterval());
                requestToPlatformProxy.setResources(taskResponseToComponents.getPlatformProxyResourceInfoList());

                // Store all requests that need to be forwarded to PlatformProxy
                queryAndProcessSearchResponseResult.addToPlatformProxyAcquisitionStartRequestList(requestToPlatformProxy);
            }
        }
        catch (HttpClientErrorException e) {
            log.info(e.getStatusCode());
            log.info(e.getResponseBodyAsString());
        }

        queryAndProcessSearchResponseResult.addToResourceManagerTaskInfoResponseList(taskInfoResponse);
        return queryAndProcessSearchResponseResult;
    }

    private TaskResponseToComponents processSearchResponse(QueryResponse queryResponse, ResourceManagerTaskInfoRequest taskInfoRequest) {

        List<QueryResourceResult> queryResultLists = queryResponse.getResources();
        TaskResponseToComponents taskResponseToComponents = new TaskResponseToComponents();

        // Process the response for each task
        for (QueryResourceResult queryResourceResult : queryResultLists) {

            if (taskResponseToComponents.getCount() >= taskInfoRequest.getCount())
                break;

            TaskResponseToComponents newTaskResponseToComponents = getUrlsFromCram(queryResourceResult);
            taskResponseToComponents.add(newTaskResponseToComponents);

        }

        return taskResponseToComponents;
    }

    private TaskResponseToComponents getUrlsFromCram(QueryResourceResult queryResourceResult) {
        TaskResponseToComponents taskResponseToComponents = new TaskResponseToComponents();

        try {
            Token token = securityManager.requestPlatformToken(queryResourceResult.getPlatformId());
            log.info("Platform Token from platform " + queryResourceResult.getPlatformId() + " acquired: " + token);

            // Request resourceUrl from CRAM
            String cramRequestUrl = symbIoTeCoreUrl + "/resourceUrls?id=" + queryResourceResult.getId();
            HttpHeaders cramHttpHeaders = new HttpHeaders();
            cramHttpHeaders.set("Accept", MediaType.APPLICATION_JSON_VALUE);
            cramHttpHeaders.set(AAMConstants.TOKEN_HEADER_NAME, token.getToken());
            cramHttpHeaders.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<String> cramEntity = new HttpEntity<>(cramHttpHeaders);
            ParameterizedTypeReference<Map<String, String>> typeRef = new ParameterizedTypeReference<Map<String, String>>() {};
            ResponseEntity<Map<String, String>> cramResponseEntity = restTemplate.exchange(
                    cramRequestUrl, HttpMethod.GET, cramEntity, typeRef);
            Map<String, String> cramResponse = cramResponseEntity.getBody();

            log.info("CRAM Response: " + cramResponse);

            if (cramResponse != null) {
                String resourceUrl = cramResponse.get(queryResourceResult.getId());
                if (resourceUrl != null) {
                    // Building the request to PlatformProxy
                    PlatformProxyResourceInfo platformProxyResourceInfo = new PlatformProxyResourceInfo();
                    platformProxyResourceInfo.setResourceId(queryResourceResult.getId());
                    platformProxyResourceInfo.setAccessURL(resourceUrl);
                    taskResponseToComponents.addToPlatformProxyResourceInfoList(platformProxyResourceInfo);
                    taskResponseToComponents.addToCount(1);

                    // Save the id for returning it to the EnablerLogic
                    taskResponseToComponents.addToResourceIdsForEnablerLogic(queryResourceResult.getId());
                }
            }
        } catch (SecurityException e) {
            log.info(e.toString());
        } catch (HttpClientErrorException e) {
            log.info(e.getStatusCode());
            log.info(e.getResponseBodyAsString());
        }

        return taskResponseToComponents;
    }

    private class TaskResponseToComponents {
        private ArrayList<PlatformProxyResourceInfo> platformProxyResourceInfoList;
        private ArrayList<String> resourceIdsForEnablerLogic;
        private Integer count;

        TaskResponseToComponents() {
            platformProxyResourceInfoList = new ArrayList<>();
            resourceIdsForEnablerLogic = new ArrayList<>();
            count = 0;
        }

        ArrayList<PlatformProxyResourceInfo> getPlatformProxyResourceInfoList() { return this.platformProxyResourceInfoList; }
        ArrayList<String> getResourceIdsForEnablerLogic() { return this.resourceIdsForEnablerLogic; }
        Integer getCount() {return this.count; }

        void addToResourceIdsForEnablerLogic(String id) {
            resourceIdsForEnablerLogic.add(id);
        }

        void addToPlatformProxyResourceInfoList(PlatformProxyResourceInfo platformProxyResourceInfo) {
            platformProxyResourceInfoList.add(platformProxyResourceInfo);
        }

        void addToCount(Integer num) {
            count += num;
        }

        void add(TaskResponseToComponents taskResponseToComponents) {
            this.platformProxyResourceInfoList.addAll(taskResponseToComponents.getPlatformProxyResourceInfoList());
            this.resourceIdsForEnablerLogic.addAll(taskResponseToComponents.getResourceIdsForEnablerLogic());
            this.count += taskResponseToComponents.getCount();
        }
    }

    private class QueryAndProcessSearchResponseResult {
        private ArrayList<ResourceManagerTaskInfoResponse> resourceManagerTaskInfoResponseList;
        private ArrayList<PlatformProxyAcquisitionStartRequest> platformProxyAcquisitionStartRequestList;

        QueryAndProcessSearchResponseResult() {
            resourceManagerTaskInfoResponseList = new ArrayList<>();
            platformProxyAcquisitionStartRequestList = new ArrayList<>();
        }

        ArrayList<ResourceManagerTaskInfoResponse> getResourceManagerTaskInfoResponseList() { return this.resourceManagerTaskInfoResponseList; }
        ArrayList<PlatformProxyAcquisitionStartRequest> getPlatformProxyAcquisitionStartRequestList() { return this.platformProxyAcquisitionStartRequestList; }

        void addToResourceManagerTaskInfoResponseList(ResourceManagerTaskInfoResponse item) {
            resourceManagerTaskInfoResponseList.add(item);
        }

        void addToPlatformProxyAcquisitionStartRequestList(PlatformProxyAcquisitionStartRequest item) {
            platformProxyAcquisitionStartRequestList.add(item);
        }

        void add(QueryAndProcessSearchResponseResult queryAndProcessSearchResponseResult) {
            this.resourceManagerTaskInfoResponseList.addAll(queryAndProcessSearchResponseResult.getResourceManagerTaskInfoResponseList());
            this.platformProxyAcquisitionStartRequestList.addAll(queryAndProcessSearchResponseResult.getPlatformProxyAcquisitionStartRequestList());
        }
    }
}
