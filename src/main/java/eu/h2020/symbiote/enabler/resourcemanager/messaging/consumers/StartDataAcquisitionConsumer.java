package eu.h2020.symbiote.enabler.resourcemanager.messaging.consumers;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import eu.h2020.symbiote.enabler.messaging.model.*;
import eu.h2020.symbiote.enabler.resourcemanager.model.QueryAndProcessSearchResponseResult;
import eu.h2020.symbiote.enabler.resourcemanager.model.TaskInfo;
import eu.h2020.symbiote.enabler.resourcemanager.repository.TaskInfoRepository;
import eu.h2020.symbiote.enabler.resourcemanager.utils.SearchHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.util.Assert;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;



/**
 * RabbitMQ Consumer implementation used for getting the resource details from Enabler Logic
 *
 * Created by vasgl
 */
public class StartDataAcquisitionConsumer extends DefaultConsumer {

    private static Log log = LogFactory.getLog(StartDataAcquisitionConsumer.class);

    private TaskInfoRepository taskInfoRepository;
    private RabbitTemplate rabbitTemplate;
    private SearchHelper searchHelper;
    private String platformProxyExchange;
    private String platformProxyAcquisitionStartRequestedRoutingKey;


    /**
     * Constructs a new instance and records its association to the passed-in channel.
     * Managers beans passed as parameters because of lack of possibility to inject it to consumer.
     *
     * @param channel           the channel to which this consumer is attached
     * @param taskInfoRepository    the TaskInfoRepository
     * @param rabbitTemplate        the rabbitTemplate to be used to send messages
     * @param searchHelper          the search helper
     * @param platformProxyExchange the name of the platform exchange
     * @param platformProxyAcquisitionStartRequestedRoutingKey the key for starting tasks on the Platform Proxy
     */
    public StartDataAcquisitionConsumer(Channel channel, TaskInfoRepository taskInfoRepository,
                                        RabbitTemplate rabbitTemplate, SearchHelper searchHelper,
                                        String platformProxyExchange, String platformProxyAcquisitionStartRequestedRoutingKey) {
        super(channel);

        Assert.notNull(taskInfoRepository,"taskInfoRepository can not be null!");
        this.taskInfoRepository = taskInfoRepository;

        Assert.notNull(rabbitTemplate,"rabbitTemplate can not be null!");
        this.rabbitTemplate = rabbitTemplate;

        Assert.notNull(searchHelper,"searchHelper can not be null!");
        this.searchHelper = searchHelper;

        Assert.notNull(taskInfoRepository,"platformProxyExchange can not be null!");
        this.platformProxyExchange = platformProxyExchange;

        Assert.notNull(taskInfoRepository,"platformProxyAcquisitionStartRequestedRoutingKey can not be null!");
        this.platformProxyAcquisitionStartRequestedRoutingKey = platformProxyAcquisitionStartRequestedRoutingKey;
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
        String requestInString = new String(body, StandardCharsets.UTF_8);
        ResourceManagerAcquisitionStartResponse response  = new ResourceManagerAcquisitionStartResponse();
        ArrayList<ResourceManagerTaskInfoResponse> resourceManagerTaskInfoResponseList = new ArrayList<>();
        ArrayList<PlatformProxyTaskInfo> platformProxyAcquisitionStartRequestList = new ArrayList<>();

        log.info("Received StartDataAcquisition request : " + requestInString);

        try {
            ResourceManagerAcquisitionStartRequest request  = mapper.readValue(requestInString, ResourceManagerAcquisitionStartRequest.class);

            // Process each task request
            for (ResourceManagerTaskInfoRequest taskInfoRequest : request.getTasks()) {

                // Perform the request
                QueryAndProcessSearchResponseResult newQueryAndProcessSearchResponseResult = searchHelper
                        .queryAndProcessSearchResponse(taskInfoRequest);

                if (newQueryAndProcessSearchResponseResult.getResourceManagerTaskInfoResponse() != null)
                    resourceManagerTaskInfoResponseList.add(
                            newQueryAndProcessSearchResponseResult.getResourceManagerTaskInfoResponse());
                if (newQueryAndProcessSearchResponseResult.getPlatformProxyTaskInfo() != null)
                    platformProxyAcquisitionStartRequestList.add(
                            newQueryAndProcessSearchResponseResult.getPlatformProxyTaskInfo());

                // Store the taskInfo
                TaskInfo taskInfo = newQueryAndProcessSearchResponseResult.getTaskInfo();
                if (taskInfo.getStatus() == ResourceManagerTaskInfoResponseStatus.SUCCESS ||
                        taskInfo.getStatus() == ResourceManagerTaskInfoResponseStatus.NOT_ENOUGH_RESOURCES)
                    taskInfoRepository.save(taskInfo);
            }


            // Sending response to EnablerLogic
            ArrayList<String> failedTasks = new ArrayList<>();

            response.setTasks(resourceManagerTaskInfoResponseList);

            for (ResourceManagerTaskInfoResponse taskInfoResponse : response.getTasks()) {
                if (taskInfoResponse.getStatus() != ResourceManagerTaskInfoResponseStatus.SUCCESS)
                    failedTasks.add(taskInfoResponse.getTaskId());
            }

            if (failedTasks.size() == 0) {
                String message = "ALL the task requests were successful!";
                log.info(message);
                response.setStatus(ResourceManagerTasksStatus.SUCCESS);
                response.setMessage(message);
            } else if (failedTasks.size() == response.getTasks().size()){
                String message = "NONE of the task requests were successful";
                log.info(message);
                response.setStatus(ResourceManagerTasksStatus.FAILED);
                response.setMessage(message);
            } else if (failedTasks.size() < response.getTasks().size()) {
                StringBuilder sb = new StringBuilder("Failed tasks id : [");

                for (String id : failedTasks) {
                    sb.append(id).append(", ");
                }
                sb.delete(sb.length() - 2, sb.length());
                sb.append("]");

                String message = sb.toString();
                log.info(message);
                response.setStatus(ResourceManagerTasksStatus.PARTIAL_SUCCESS);
                response.setMessage(message);
            }

            rabbitTemplate.convertAndSend(properties.getReplyTo(), response,
                    m -> {
                        m.getMessageProperties().setCorrelationId(properties.getCorrelationId());
                        return m;
                    });

            // Sending requests to PlatformProxy
            for (PlatformProxyTaskInfo req : platformProxyAcquisitionStartRequestList) {
                log.info("Sending request to Platform Proxy for task " + req.getTaskId());
                rabbitTemplate.convertAndSend(platformProxyExchange, platformProxyAcquisitionStartRequestedRoutingKey, req);
            }
        } catch (JsonParseException | JsonMappingException e) {
            log.error("Error occurred during deserializing ResourceManagerAcquisitionStartRequest", e);

            ResourceManagerAcquisitionStartResponse errorResponse = new ResourceManagerAcquisitionStartResponse();
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));

            if (sw.toString().contains(IllegalArgumentException.class.getName() + ": Invalid format:")) {
                log.info("Nested exception: " + IllegalArgumentException.class.getName());
                errorResponse.setStatus(ResourceManagerTasksStatus.FAILED_WRONG_FORMAT_INTERVAL);
                errorResponse.setMessage(IllegalArgumentException.class.getName() + ": " + e.getMessage());

            } else {
                errorResponse.setStatus(ResourceManagerTasksStatus.FAILED);
                errorResponse.setMessage(e.toString());
            }

            rabbitTemplate.convertAndSend(properties.getReplyTo(), errorResponse,
                    m -> {
                        m.getMessageProperties().setCorrelationId(properties.getCorrelationId());
                        return m;
                    });
        } catch (Throwable t) {
            log.error("Error occurred during deserializing ResourceManagerAcquisitionStartRequest", t);

            ResourceManagerAcquisitionStartResponse errorResponse = new ResourceManagerAcquisitionStartResponse();
            errorResponse.setStatus(ResourceManagerTasksStatus.FAILED);
            errorResponse.setMessage(t.toString());

            rabbitTemplate.convertAndSend(properties.getReplyTo(), errorResponse,
                    m -> {
                        m.getMessageProperties().setCorrelationId(properties.getCorrelationId());
                        return m;
                    });
        }

        getChannel().basicAck(envelope.getDeliveryTag(), false);
    }
}
