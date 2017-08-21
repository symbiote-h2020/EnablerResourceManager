package eu.h2020.symbiote.enabler.resourcemanager.messaging.consumers;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import eu.h2020.symbiote.core.internal.CoreQueryRequest;
import eu.h2020.symbiote.enabler.messaging.model.*;
import eu.h2020.symbiote.enabler.resourcemanager.model.QueryAndProcessSearchResponseResult;
import eu.h2020.symbiote.enabler.resourcemanager.model.TaskInfo;
import eu.h2020.symbiote.enabler.resourcemanager.repository.TaskInfoRepository;
import eu.h2020.symbiote.enabler.resourcemanager.utils.SearchHelper;

import eu.h2020.symbiote.util.IntervalFormatter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * RabbitMQ Consumer implementation used for updating the resource details from Enabler Logic
 *
 * Created by vasgl
 */
public class UpdateTaskConsumer extends DefaultConsumer {

    private static Log log = LogFactory.getLog(UpdateTaskConsumer.class);

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private SearchHelper searchHelper;

    @Value("${rabbit.exchange.enablerPlatformProxy.name}")
    private String platformProxyExchange;

    @Value("${rabbit.routingKey.enablerPlatformProxy.acquisitionStartRequested}")
    private String platformProxyAcquisitionStartRequestedRoutingKey;

    @Value("${rabbit.routingKey.enablerPlatformProxy.taskUpdated}")
    private String platformProxyTaskUpdatedKey;

    @Value("${rabbit.routingKey.enablerPlatformProxy.cancelTasks}")
    private String platformProxyCancelTasksRoutingKey;

    @Autowired
    @Qualifier("symbIoTeCoreUrl")
    private String symbIoTeCoreUrl;

    @Autowired
    private TaskInfoRepository taskInfoRepository;

    /**
     * Constructs a new instance and records its association to the passed-in channel.
     * Managers beans passed as parameters because of lack of possibility to inject it to consumer.
     *
     * @param channel           the channel to which this consumer is attached
     */
    public UpdateTaskConsumer(Channel channel) {
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
        ArrayList<ResourceManagerTaskInfoResponse> resourceManagerTaskInfoResponseList = new ArrayList<>();
        ArrayList<PlatformProxyTaskInfo> platformProxyAcquisitionStartRequestList = new ArrayList<>();
        ArrayList<PlatformProxyTaskInfo> platformProxyUpdateRequestList = new ArrayList<>();
        CancelTaskRequest cancelTaskRequest = new CancelTaskRequest();
        cancelTaskRequest.setTaskIdList(new ArrayList<>());

        log.info("Received UpdateTask request : " + requestInString);

        try {
            ResourceManagerAcquisitionStartRequest request  = mapper.readValue(requestInString, ResourceManagerAcquisitionStartRequest.class);

            // Process each task request
            for (ResourceManagerTaskInfoRequest taskInfoRequest : request.getResources()) {
                TaskInfo storedTaskInfo = taskInfoRepository.findByTaskId(taskInfoRequest.getTaskId());

                // ToDO: Check if Task exists
                if (taskInfoRequest.getCoreQueryRequest() == null ||
                        taskInfoRequest.getCoreQueryRequest().equals(storedTaskInfo.getCoreQueryRequest())){

                    log.info("The CoreQueryRequest of the task " + taskInfoRequest.getTaskId() + " did not change.");

                    TaskInfo updatedTaskInfo = new TaskInfo(taskInfoRequest);
                    List<PlatformProxyResourceInfo> platformProxyResourceInfoList = new ArrayList<>();

                    // runtime checking variable
                    boolean informPlatformProxy = true;
                    ResourceManagerTaskInfoResponseStatus responseStatus = ResourceManagerTaskInfoResponseStatus.UNKNOWN;

                    // If a value is null, retain the previous value
                    if (updatedTaskInfo.getMinNoResources() == null)
                        updatedTaskInfo.setMinNoResources(storedTaskInfo.getMinNoResources());
                    if (updatedTaskInfo.getQueryInterval() == null)
                        updatedTaskInfo.setQueryInterval(storedTaskInfo.getQueryInterval());
                    if (updatedTaskInfo.getAllowCaching() == null)
                        updatedTaskInfo.setAllowCaching(storedTaskInfo.getAllowCaching());
                    if (updatedTaskInfo.getCachingInterval() == null)
                        updatedTaskInfo.setCachingInterval(storedTaskInfo.getCachingInterval());
                    if (updatedTaskInfo.getInformPlatformProxy() == null)
                        updatedTaskInfo.setInformPlatformProxy(storedTaskInfo.getInformPlatformProxy());
                    if (updatedTaskInfo.getEnablerLogicName() == null)
                        updatedTaskInfo.setEnablerLogicName(storedTaskInfo.getEnablerLogicName());

                    // Always retain the following values in the updatedTaskInfo if the CoreQuery Request does not change
                    updatedTaskInfo.setCoreQueryRequest(CoreQueryRequest.newInstance(storedTaskInfo.getCoreQueryRequest()));
                    updatedTaskInfo.setResourceIds(new ArrayList<>(storedTaskInfo.getResourceIds()));
                    updatedTaskInfo.setStoredResourceIds(new ArrayList<>(storedTaskInfo.getStoredResourceIds()));
                    updatedTaskInfo.setResourceUrls(new HashMap<>(storedTaskInfo.getResourceUrls()));

                    // Check if allowCaching changed value
                    if (storedTaskInfo.getAllowCaching() != updatedTaskInfo.getAllowCaching()) {
                        if (updatedTaskInfo.getAllowCaching()) {
                            log.debug("AllowCaching changed value from false to true");

                            // Acquire resources
                            QueryAndProcessSearchResponseResult newQueryAndProcessSearchResponseResult = searchHelper
                                    .queryAndProcessSearchResponse(updatedTaskInfo);

                            TaskInfo newTaskInfo = newQueryAndProcessSearchResponseResult.getTaskInfo();
                            updatedTaskInfo.setStoredResourceIds(new ArrayList<>());

                            // Add the resource ids of the newTask that are not already in the resourceIds list
                            for (String resource : newTaskInfo.getResourceIds()) {
                                if (!updatedTaskInfo.getResourceIds().contains(resource)) {
                                    updatedTaskInfo.getStoredResourceIds().add(resource);
                                }
                            }

                            // Also, add the storedResource ids of the newTask
                            updatedTaskInfo.getStoredResourceIds().addAll(newTaskInfo.getStoredResourceIds());

                            // ToDo: Configure the timer
                        } else {
                            log.debug("AllowCaching changed value from true to false");

                            updatedTaskInfo.getStoredResourceIds().clear();
                            // Todo: Clear The timer
                        }
                    }

                    // Check if minNoResource increased
                    if (updatedTaskInfo.getMinNoResources() > storedTaskInfo.getMinNoResources()) {
                        log.info("The update on task " + updatedTaskInfo.getTaskId() + " requests more resources: " +
                        storedTaskInfo.getMinNoResources() + " -> " + updatedTaskInfo.getMinNoResources());

                        // ToDo: Behavior when allowCaching == false
                        int noNewResourcesNeeded = updatedTaskInfo.getMinNoResources() - storedTaskInfo.getMinNoResources();
                        if (updatedTaskInfo.getAllowCaching()) {

                            Map<String, String> newResourceUrls = new HashMap<>();
                            QueryAndProcessSearchResponseResult queryAndProcessSearchResponseResult = null;

                            while (newResourceUrls.size() != noNewResourcesNeeded &&
                                    updatedTaskInfo.getStoredResourceIds().size() != 0) {
                                String candidateResourceId = updatedTaskInfo.getStoredResourceIds().get(0);
                                String candidateResourceUrl = searchHelper.querySingleResource(candidateResourceId);

                                if (candidateResourceUrl != null) {
                                    newResourceUrls.put(candidateResourceId, candidateResourceUrl);

                                    PlatformProxyResourceInfo candidatePlatformProxyResourceInfo = new PlatformProxyResourceInfo();
                                    candidatePlatformProxyResourceInfo.setResourceId(candidateResourceId);
                                    candidatePlatformProxyResourceInfo.setAccessURL(candidateResourceUrl);

                                    platformProxyResourceInfoList.add(candidatePlatformProxyResourceInfo);
                                }

                                //ToDo: add it to another list if CRAM does not respond with a url
                                updatedTaskInfo.getStoredResourceIds().remove(0);
                            }

                            updatedTaskInfo.addResourceIds(newResourceUrls);

                            if (newResourceUrls.size() == noNewResourcesNeeded) {
                                log.info("There were enough resources to be added");
                                responseStatus = ResourceManagerTaskInfoResponseStatus.SUCCESS;

                            } else {
                                log.info("Not enough resources are available.");
                                if (responseStatus == ResourceManagerTaskInfoResponseStatus.UNKNOWN) {
                                    responseStatus = ResourceManagerTaskInfoResponseStatus.NOT_ENOUGH_RESOURCES;
                                }
                                informPlatformProxy = false;

                            }
                        }
                    } else if (updatedTaskInfo.getMinNoResources() < storedTaskInfo.getMinNoResources()) { // Check if minNoResources decreased
                        if (updatedTaskInfo.getMinNoResources() <= updatedTaskInfo.getResourceIds().size() &&
                                storedTaskInfo.getStatus() == ResourceManagerTaskInfoResponseStatus.NOT_ENOUGH_RESOURCES) {
                            updatedTaskInfo.setStatus(ResourceManagerTaskInfoResponseStatus.SUCCESS);
                        }
                    }

                    // Inform Enabler Logic in any case
                    ResourceManagerTaskInfoResponse resourceManagerTaskInfoResponse =
                            new ResourceManagerTaskInfoResponse(updatedTaskInfo);
                    if (responseStatus == ResourceManagerTaskInfoResponseStatus.UNKNOWN)
                        responseStatus = ResourceManagerTaskInfoResponseStatus.SUCCESS;
                    resourceManagerTaskInfoResponse.setStatus(responseStatus);
                    resourceManagerTaskInfoResponseList.add(resourceManagerTaskInfoResponse);

                    updatedTaskInfo.setStatus(responseStatus);

                    // Check if there is a need to inform Platform Proxy

                    // Check if informPlatformProxy changed value
                    if (storedTaskInfo.getInformPlatformProxy() != updatedTaskInfo.getInformPlatformProxy()) {
                        processPlatformProxyTransition(updatedTaskInfo, platformProxyAcquisitionStartRequestList,
                                cancelTaskRequest);
                    }
                    else if (updatedTaskInfo.getInformPlatformProxy() && informPlatformProxy &&
                            (!updatedTaskInfo.getQueryInterval().equals(storedTaskInfo.getQueryInterval()) ||
                                    !updatedTaskInfo.getEnablerLogicName().equals(storedTaskInfo.getEnablerLogicName()) ||
                                            platformProxyResourceInfoList.size() > 0 ||
                                    (updatedTaskInfo.getStatus() != storedTaskInfo.getStatus() &&
                                            updatedTaskInfo.getStatus() == ResourceManagerTaskInfoResponseStatus.SUCCESS))) {

                        PlatformProxyUpdateRequest updateRequest = new PlatformProxyUpdateRequest();
                        updateRequest.setTaskId(updatedTaskInfo.getTaskId());
                        updateRequest.setEnablerLogicName(updatedTaskInfo.getEnablerLogicName());
                        updateRequest.setQueryInterval_ms(new IntervalFormatter(updatedTaskInfo.getQueryInterval())
                                .getMillis());
                        updateRequest.setResources(updatedTaskInfo.createPlatformProxyResourceInfoList());
                        platformProxyUpdateRequestList.add(updateRequest);
                    }


                    taskInfoRepository.save(updatedTaskInfo);

                } else {
                    log.info("The CoreQueryRequest of the task " + taskInfoRequest.getTaskId() + " changed.");

                    // Always request ranked results
                    taskInfoRequest.getCoreQueryRequest().setShould_rank(true);

                    QueryAndProcessSearchResponseResult newQueryAndProcessSearchResponseResult = searchHelper
                            .queryAndProcessSearchResponse(taskInfoRequest);

                    // Reply to Enabler Logic
                    if (newQueryAndProcessSearchResponseResult.getResourceManagerTaskInfoResponse() != null)
                        resourceManagerTaskInfoResponseList.add(
                                newQueryAndProcessSearchResponseResult.getResourceManagerTaskInfoResponse());


                    // Store the taskInfo
                    TaskInfo taskInfo = newQueryAndProcessSearchResponseResult.getTaskInfo();
                    taskInfoRepository.save(taskInfo);

                    // Inform Platform Proxy depending on the informPlatformProxy transition
                    if (storedTaskInfo.getInformPlatformProxy() != taskInfo.getInformPlatformProxy()) {
                        processPlatformProxyTransition(taskInfo, platformProxyAcquisitionStartRequestList,
                                cancelTaskRequest);
                    } else if (newQueryAndProcessSearchResponseResult.getPlatformProxyTaskInfo() != null) {
                        // send UpdateTaskRequest
                        platformProxyUpdateRequestList.add(
                                newQueryAndProcessSearchResponseResult.getPlatformProxyTaskInfo());
                    }
                }
            }


            // Sending response to EnablerLogic
            response.setResources(resourceManagerTaskInfoResponseList);
            rabbitTemplate.convertAndSend(properties.getReplyTo(), response,
                    m -> {
                        m.getMessageProperties().setCorrelationIdString(properties.getCorrelationId());
                        return m;
                    });

            // Sending requests to PlatformProxy about startAcquisition tasks
            for (PlatformProxyTaskInfo req : platformProxyAcquisitionStartRequestList) {
                log.info("Sending startAcquisition request to Platform Proxy for task " + req.getTaskId());
                rabbitTemplate.convertAndSend(platformProxyExchange, platformProxyAcquisitionStartRequestedRoutingKey, req);
            }

            // Sending requests to PlatformProxy about updated tasks
            for (PlatformProxyTaskInfo req : platformProxyUpdateRequestList) {
                log.info("Sending update request to Platform Proxy for task " + req.getTaskId());
                rabbitTemplate.convertAndSend(platformProxyExchange, platformProxyTaskUpdatedKey, req);
            }

            // Inform Platform Proxy about canceled tasks
            if (cancelTaskRequest.getTaskIdList().size() > 0) {
                rabbitTemplate.convertAndSend(platformProxyExchange, platformProxyCancelTasksRoutingKey, cancelTaskRequest);
            }

        } catch (JsonParseException | JsonMappingException e) {
            log.error("Error occurred during deserializing ResourceManagerAcquisitionStartRequest", e);
        } catch (IllegalArgumentException e) {
            log.info("Interval Wrong Format: " + e.toString());

        }

        ResourceManagerAcquisitionStartResponse wrongIntervalFormatResponse = new ResourceManagerAcquisitionStartResponse();
        wrongIntervalFormatResponse.setStatus(ResourceManagerAcquisitionStartResponseStatus.FAILED_WRONG_FORMAT_INTERVAL);
        rabbitTemplate.convertAndSend(properties.getReplyTo(), wrongIntervalFormatResponse,
                m -> {
                    m.getMessageProperties().setCorrelationIdString(properties.getCorrelationId());
                    return m;
                });
    }

    private void processPlatformProxyTransition(TaskInfo taskInfo, List<PlatformProxyTaskInfo> platformProxyAcquisitionStartRequestList,
                      CancelTaskRequest cancelTaskRequest) {
        if (taskInfo.getInformPlatformProxy()) {
            // send StartAcquisitionRequest
            PlatformProxyAcquisitionStartRequest startRequest = new PlatformProxyAcquisitionStartRequest();

            startRequest.setTaskId(taskInfo.getTaskId());
            startRequest.setEnablerLogicName(taskInfo.getEnablerLogicName());
            startRequest.setQueryInterval_ms(new IntervalFormatter(taskInfo.getQueryInterval())
                    .getMillis());
            startRequest.setResources(taskInfo.createPlatformProxyResourceInfoList());
            platformProxyAcquisitionStartRequestList.add(startRequest);
        }
        else {
            // send CancelTaskRequest
            cancelTaskRequest.getTaskIdList().add(taskInfo.getTaskId());
        }
    }
}

