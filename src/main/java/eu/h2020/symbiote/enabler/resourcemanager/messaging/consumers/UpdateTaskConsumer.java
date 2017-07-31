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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;

import java.io.IOException;
import java.util.ArrayList;


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
        ArrayList<PlatformProxyTaskInfo> platformProxyUpdateRequestArrayList = new ArrayList<>();
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

                    // If a value is null, retain the previous value
                    if (updatedTaskInfo.getMinNoResources() == null)
                        updatedTaskInfo.setMinNoResources(storedTaskInfo.getMinNoResources());
                    if (updatedTaskInfo.getQueryInterval_ms() == null)
                        updatedTaskInfo.setQueryInterval_ms(storedTaskInfo.getQueryInterval_ms());
                    if (updatedTaskInfo.getAllowCaching() == null)
                        updatedTaskInfo.setAllowCaching(storedTaskInfo.getAllowCaching());
                    if (updatedTaskInfo.getCachingInterval_ms() == null)
                        updatedTaskInfo.setCachingInterval_ms(storedTaskInfo.getCachingInterval_ms());
                    if (updatedTaskInfo.getInformPlatformProxy() == null)
                        updatedTaskInfo.setInformPlatformProxy(storedTaskInfo.getInformPlatformProxy());
                    if (updatedTaskInfo.getEnablerLogicName() == null)
                        updatedTaskInfo.setEnablerLogicName(storedTaskInfo.getEnablerLogicName());

                    // Always retain the following values in the updatedTaskInfo if the CoreQuery Request does not change
                    updatedTaskInfo.setCoreQueryRequest(CoreQueryRequest.newInstance(storedTaskInfo.getCoreQueryRequest()));
                    updatedTaskInfo.setResourceIds(new ArrayList<>(storedTaskInfo.getResourceIds()));
                    updatedTaskInfo.setStoredResourceIds(new ArrayList<>(storedTaskInfo.getStoredResourceIds()));

                    taskInfoRepository.save(updatedTaskInfo);

                    // Inform Enabler Logic in any case
                    resourceManagerTaskInfoResponseList.add(new ResourceManagerTaskInfoResponse(updatedTaskInfo));

                    // Inform Platform Proxy
                    if (storedTaskInfo.getInformPlatformProxy() != updatedTaskInfo.getInformPlatformProxy()) {
                        if (updatedTaskInfo.getInformPlatformProxy()) {
                            // send StartAcquisitionRequest
                        }
                        else {
                            // send CancelTaskRequest
                            cancelTaskRequest.getTaskIdList().add(updatedTaskInfo.getTaskId());
                        }
                    }
                    else if (updatedTaskInfo.getInformPlatformProxy() == true &&
                            (updatedTaskInfo.getQueryInterval_ms() != storedTaskInfo.getQueryInterval_ms() ||
                            !updatedTaskInfo.getEnablerLogicName().equals(storedTaskInfo.getEnablerLogicName()))) {

                        PlatformProxyUpdateRequest updateRequest = new PlatformProxyUpdateRequest();
                        updateRequest.setTaskId(updatedTaskInfo.getTaskId());
                        updateRequest.setEnablerLogicName(updatedTaskInfo.getEnablerLogicName());
                        updateRequest.setQueryInterval_ms(updatedTaskInfo.getQueryInterval_ms());
                        updateRequest.setResources(null); // Setting resources to null if there are no updates

                        platformProxyUpdateRequestArrayList.add(updateRequest);
                    }


                } else {
                    log.info("The CoreQueryRequest of the task " + taskInfoRequest.getTaskId() + " changed.");

                    String queryUrl = searchHelper.buildRequestUrl(taskInfoRequest);
                    QueryAndProcessSearchResponseResult newQueryAndProcessSearchResponseResult = searchHelper
                            .queryAndProcessSearchResponse(queryUrl, taskInfoRequest);

                    if (newQueryAndProcessSearchResponseResult.getResourceManagerTaskInfoResponse() != null)
                        resourceManagerTaskInfoResponseList.add(
                                newQueryAndProcessSearchResponseResult.getResourceManagerTaskInfoResponse());
                    if (newQueryAndProcessSearchResponseResult.getPlatformProxyTaskInfo() != null)
                        platformProxyUpdateRequestArrayList.add(
                                newQueryAndProcessSearchResponseResult.getPlatformProxyTaskInfo());

                    // Store the taskInfo
                    TaskInfo taskInfo = newQueryAndProcessSearchResponseResult.getTaskInfo();
                    taskInfoRepository.save(taskInfo);

                    // Inform Platform Proxy depending on the informPlatformProxy transition
                    if (storedTaskInfo.getInformPlatformProxy() != taskInfo.getInformPlatformProxy()) {
                        if (taskInfo.getInformPlatformProxy()) {
                            // send StartAcquisitionRequest
                        }
                        else {
                            // send CancelTaskRequest
                            cancelTaskRequest.getTaskIdList().add(taskInfo.getTaskId());
                        }
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

            // Sending requests to PlatformProxy about updated tasks
            for (PlatformProxyTaskInfo req : platformProxyUpdateRequestArrayList) {
                log.info("Sending request to Platform Proxy for task " + req.getTaskId());
                rabbitTemplate.convertAndSend(platformProxyExchange, platformProxyTaskUpdatedKey, req);
            }

            // Inform Platform Proxy
            if (cancelTaskRequest.getTaskIdList().size() > 0) {
                rabbitTemplate.convertAndSend(platformProxyExchange, platformProxyCancelTasksRoutingKey, cancelTaskRequest);
            }

        } catch (JsonParseException | JsonMappingException e) {
            log.error("Error occurred during deserializing ResourceManagerAcquisitionStartRequest", e);
        }
    }
}
