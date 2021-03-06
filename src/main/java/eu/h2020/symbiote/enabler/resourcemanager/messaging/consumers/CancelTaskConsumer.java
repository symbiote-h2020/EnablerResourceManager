package eu.h2020.symbiote.enabler.resourcemanager.messaging.consumers;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import eu.h2020.symbiote.enabler.messaging.model.CancelTaskRequest;
import eu.h2020.symbiote.enabler.messaging.model.CancelTaskResponse;
import eu.h2020.symbiote.enabler.messaging.model.CancelTaskResponseStatus;
import eu.h2020.symbiote.enabler.resourcemanager.model.TaskInfo;
import eu.h2020.symbiote.enabler.resourcemanager.repository.TaskInfoRepository;

import eu.h2020.symbiote.enabler.resourcemanager.utils.SearchHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by vasgl on 7/18/2017.
 */
public class CancelTaskConsumer extends DefaultConsumer {

    private static Log log = LogFactory.getLog(CancelTaskConsumer.class);


    @Autowired
    private TaskInfoRepository taskInfoRepository;

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private SearchHelper searchHelper;

    @Value("${rabbit.exchange.enablerPlatformProxy.name}")
    private String platformProxyExchange;

    @Value("${rabbit.routingKey.enablerPlatformProxy.cancelTasks}")
    private String platformProxyCancelTasksRoutingKey;

    /**
     * Constructs a new instance and records its association to the passed-in channel.
     * Managers beans passed as parameters because of lack of possibility to inject it to consumer.
     *
     * @param channel           the channel to which this consumer is attached
     */
    public CancelTaskConsumer(Channel channel) {
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
        CancelTaskRequest cancelTaskRequestToPlatformProxy = new CancelTaskRequest();
        CancelTaskResponse cancelTaskResponse = new CancelTaskResponse();
        ArrayList<String> nonExistentTasks = new ArrayList<>();

        log.info("Received CancelTaskRequest: " + requestInString);

        try {
            CancelTaskRequest cancelTaskRequest = mapper.readValue(requestInString, CancelTaskRequest.class);

            for (String id : cancelTaskRequest.getTaskIdList()) {
                TaskInfo taskInfo = taskInfoRepository.findByTaskId(id);

                if (taskInfo != null) {
                    log.info("The task with id = " + id + " was deleted.");
                    taskInfoRepository.delete(taskInfo);

                    if (taskInfo.getAllowCaching())
                        searchHelper.removeTaskTimer(taskInfo.getTaskId());

                    if (taskInfo.getInformPlatformProxy())
                        cancelTaskRequestToPlatformProxy.getTaskIdList().add(taskInfo.getTaskId());
                } else {
                    log.info("The task with id = " + id + " does not exist.");
                    nonExistentTasks.add(id);
                }
            }
        } catch (JsonParseException | JsonMappingException e) {
            log.error("Error occurred during deserializing CancelTaskRequest", e);

            CancelTaskResponse errorResponse = new CancelTaskResponse();
            errorResponse.setStatus(CancelTaskResponseStatus.FAILED);
            errorResponse.setMessage(e.toString());

            rabbitTemplate.convertAndSend(properties.getReplyTo(), errorResponse,
                    m -> {
                        m.getMessageProperties().setCorrelationId(properties.getCorrelationId());
                        return m;
                    });
        } catch (Exception e) {
            log.error("Error occurred during deserializing CancelTaskRequest", e);

            CancelTaskResponse errorResponse = new CancelTaskResponse();
            errorResponse.setStatus(CancelTaskResponseStatus.FAILED);
            errorResponse.setMessage(e.toString());

            rabbitTemplate.convertAndSend(properties.getReplyTo(), errorResponse,
                    m -> {
                        m.getMessageProperties().setCorrelationId(properties.getCorrelationId());
                        return m;
                    });
        }

        // Inform Enabler Logic
        if (nonExistentTasks.size() == 0) {
            cancelTaskResponse.setStatus(CancelTaskResponseStatus.SUCCESS);
            cancelTaskResponse.setMessage("ALL tasks were deleted!");
        } else {
            String message = "Non-existent task ids : [";

            for (String id : nonExistentTasks) {
                message += id + ", ";
            }
            message = message.substring(0, message.length() - 2);
            message += "]";

            cancelTaskResponse.setStatus(CancelTaskResponseStatus.NOT_ALL_TASKS_EXIST);
            cancelTaskResponse.setMessage(message);
        }

        rabbitTemplate.convertAndSend(properties.getReplyTo(), cancelTaskResponse,
                m -> {
                    m.getMessageProperties().setCorrelationId(properties.getCorrelationId());
                    return m;
                });

        getChannel().basicAck(envelope.getDeliveryTag(), false);

        // Inform Platform Proxy
        if (cancelTaskRequestToPlatformProxy.getTaskIdList().size() > 0) {
            rabbitTemplate.convertAndSend(platformProxyExchange, platformProxyCancelTasksRoutingKey, cancelTaskRequestToPlatformProxy);
        }
    }
}
