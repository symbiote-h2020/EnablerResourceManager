package eu.h2020.symbiote.enabler.resourcemanager.messaging;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.beans.factory.annotation.Autowired;

import eu.h2020.symbiote.enabler.resourcemanager.repository.TaskInfoRepository;
import eu.h2020.symbiote.enabler.messaging.model.CancelTaskRequest;
import eu.h2020.symbiote.enabler.resourcemanager.model.TaskInfo;

import java.io.IOException;

/**
 * Created by vasgl on 7/18/2017.
 */
public class CancelTaskConsumer extends DefaultConsumer {

    private static Log log = LogFactory.getLog(CancelTaskConsumer.class);


    @Autowired
    private TaskInfoRepository taskInfoRepository;


    /**
     * Constructs a new instance and records its association to the passed-in channel.
     * Managers beans passed as parameters because of lack of possibility to inject it to consumer.
     *
     * @param channel           the channel to which this consumer is attached
     */
    CancelTaskConsumer(Channel channel) {
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

        log.info("Received CancelTaskRequest: " + requestInString);

        try {
            CancelTaskRequest cancelTaskRequest = mapper.readValue(requestInString, CancelTaskRequest.class);

            for (String id : cancelTaskRequest.getTaskIdList()) {
                TaskInfo taskInfo = taskInfoRepository.findByTaskId(id);

                if (taskInfo != null) {
                    log.info("Task with id = " + id + " was deleted.");
                    taskInfoRepository.delete(taskInfo);
                }
            }
        } catch (JsonParseException | JsonMappingException e) {
            log.error("Error occurred during deserializing CancelTaskRequest", e);
        }
    }
}
