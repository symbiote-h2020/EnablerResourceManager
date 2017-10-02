package eu.h2020.symbiote.enabler.resourcemanager.dummyListeners;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.h2020.symbiote.enabler.messaging.model.NotEnoughResourcesAvailable;
import eu.h2020.symbiote.enabler.messaging.model.ResourcesUpdated;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by vasgl on 7/17/2017.
 */
@Component
public class DummyEnablerLogicListener {
    private static Log log = LogFactory.getLog(DummyEnablerLogicListener.class);

    private List<ResourcesUpdated> updateResourcesReceivedByListener = new ArrayList<>();
    private List<NotEnoughResourcesAvailable> notEnoughResourcesMessagesReceivedByListener = new ArrayList<>();
    private ObjectMapper mapper = new ObjectMapper();


    public List<ResourcesUpdated> getUpdateResourcesReceivedByListener() {
        return updateResourcesReceivedByListener;
    }

    public List<NotEnoughResourcesAvailable> getNotEnoughResourcesMessagesReceivedByListener() {
        return notEnoughResourcesMessagesReceivedByListener;
    }

    public int updateResourcesReceived() { return updateResourcesReceivedByListener.size(); }
    public int notEnoughResourcesMessagesReceived() { return  notEnoughResourcesMessagesReceivedByListener.size(); }

    public void clearRequestsReceivedByListener() {
        updateResourcesReceivedByListener.clear();
        notEnoughResourcesMessagesReceivedByListener.clear();
    }

    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(value = "${rabbit.queueName.el.resourcesUpdated}", durable = "true", autoDelete = "true", exclusive = "true"),
            exchange = @Exchange(value = "${rabbit.exchange.enablerLogic.name}", ignoreDeclarationExceptions = "true",
                    durable = "${rabbit.exchange.enablerLogic.durable}", autoDelete  = "${rabbit.exchange.enablerLogic.autodelete}",
                    internal = "${rabbit.exchange.enablerLogic.internal}", type = "${rabbit.exchange.enablerLogic.type}"),
            key = "${rabbit.routingKey.enablerLogic.resourcesUpdated}.testEnablerLogic")
    )
    public void enablerLogicResourcesUpdatedListener(ResourcesUpdated resourcesUpdated) {
        updateResourcesReceivedByListener.add(resourcesUpdated);

        try {
            String responseInString = mapper.writeValueAsString(resourcesUpdated);
            log.info("EnablerLogicListener received update request: " + responseInString);
            log.info("updateResourcesReceivedByListener.size() = " + updateResourcesReceivedByListener.size());

            for(String resource : resourcesUpdated.getNewResources()) {
                log.info("resourcesUpdated = " + resource);
            }
        } catch (JsonProcessingException e) {
            log.info(e.toString());
        }
    }

    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(value = "${rabbit.queueName.el.notEnoughResources}", durable = "true", autoDelete = "true", exclusive = "true"),
            exchange = @Exchange(value = "${rabbit.exchange.enablerLogic.name}", ignoreDeclarationExceptions = "true",
                    durable = "${rabbit.exchange.enablerLogic.durable}", autoDelete  = "${rabbit.exchange.enablerLogic.autodelete}",
                    internal = "${rabbit.exchange.enablerLogic.internal}", type = "${rabbit.exchange.enablerLogic.type}"),
            key = "${rabbit.routingKey.enablerLogic.notEnoughResources}.testEnablerLogic")
    )
    public void enablerLogicNotEnoughResourcesListener(NotEnoughResourcesAvailable notEnoughResourcesAvailable) {
        notEnoughResourcesMessagesReceivedByListener.add(notEnoughResourcesAvailable);

        try {
            String responseInString = mapper.writeValueAsString(notEnoughResourcesAvailable);
            log.info("EnablerLogicListener NotEnoughResources notification: " + responseInString);
            log.info("notEnoughResourcesMessagesReceivedByListener.size() = " + notEnoughResourcesMessagesReceivedByListener.size());

        } catch (JsonProcessingException e) {
            log.info(e.toString());
        }
    }
}
