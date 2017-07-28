package eu.h2020.symbiote.enabler.resourcemanager.messaging.consumers;

import eu.h2020.symbiote.enabler.resourcemanager.model.QueryAndProcessSearchResponseResult;
import eu.h2020.symbiote.enabler.resourcemanager.model.TaskInfo;
import eu.h2020.symbiote.enabler.resourcemanager.repository.TaskInfoRepository;
import eu.h2020.symbiote.enabler.resourcemanager.utils.SearchHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.rabbit.core.RabbitTemplate;


import java.io.IOException;
import java.util.ArrayList;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.core.JsonParseException;

import eu.h2020.symbiote.enabler.messaging.model.*;



/**
 * RabbitMQ Consumer implementation used for getting the resource details from Enabler Logic
 *
 * Created by vasgl
 */
public class StartDataAcquisitionConsumer extends DefaultConsumer {

    private static Log log = LogFactory.getLog(StartDataAcquisitionConsumer.class);

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private SearchHelper searchHelper;

    @Value("${rabbit.exchange.enablerPlatformProxy.name}") 
    private String platformProxyExchange; 

    @Value("${rabbit.routingKey.enablerPlatformProxy.acquisitionStartRequested}") 
    private String platformProxyAcquisitionStartRequestedRoutingKey; 

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
    public StartDataAcquisitionConsumer(Channel channel) {
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
        ArrayList<PlatformProxyAcquisitionStartRequest> platformProxyAcquisitionStartRequestList = new ArrayList<>();

        log.info("Received StartDataAcquisition request : " + requestInString);

        try {
            ResourceManagerAcquisitionStartRequest request  = mapper.readValue(requestInString, ResourceManagerAcquisitionStartRequest.class);

            // Process each task request
            for (ResourceManagerTaskInfoRequest taskInfoRequest : request.getResources()) {
                String queryUrl = searchHelper.buildRequestUrl(taskInfoRequest);
                QueryAndProcessSearchResponseResult newQueryAndProcessSearchResponseResult = searchHelper.queryAndProcessSearchResponse(queryUrl, taskInfoRequest);

                if (newQueryAndProcessSearchResponseResult.getResourceManagerTaskInfoResponse() != null)
                    resourceManagerTaskInfoResponseList.add(newQueryAndProcessSearchResponseResult.getResourceManagerTaskInfoResponse());
                if (newQueryAndProcessSearchResponseResult.getPlatformProxyAcquisitionStartRequest() != null)
                    platformProxyAcquisitionStartRequestList.add(newQueryAndProcessSearchResponseResult.getPlatformProxyAcquisitionStartRequest());

                // Store the taskInfo
                TaskInfo taskInfo = newQueryAndProcessSearchResponseResult.getTaskInfo();
                taskInfoRepository.save(taskInfo);
            }


            // Sending response to EnablerLogic
            response.setResources(resourceManagerTaskInfoResponseList);
            rabbitTemplate.convertAndSend(properties.getReplyTo(), response,
                    m -> {
                        m.getMessageProperties().setCorrelationIdString(properties.getCorrelationId());
                        return m;
                    });

            // Sending requests to PlatformProxy
            for (PlatformProxyAcquisitionStartRequest req : platformProxyAcquisitionStartRequestList) {
                log.info("Sending request to Platform Proxy for task " + req.getTaskId());
                rabbitTemplate.convertAndSend(platformProxyExchange, platformProxyAcquisitionStartRequestedRoutingKey, req);
            }
        } catch (JsonParseException | JsonMappingException e) {
            log.error("Error occurred during deserializing ResourceManagerAcquisitionStartRequest", e);
        }


    }
}
