package eu.h2020.symbiote.enabler.resourcemanager.dummyListeners;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.h2020.symbiote.enabler.messaging.model.PlatformProxyResourceInfo;
import eu.h2020.symbiote.enabler.messaging.model.ResourcesUpdated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by lebro_000 on 7/17/2017.
 */
@Component
public class DummyEnablerLogicListener {
    private static Logger log = LoggerFactory
            .getLogger(DummyEnablerLogicListener.class);

    private List<ResourcesUpdated> updateResourcesReceivedByListener = new ArrayList<>();
    private ObjectMapper mapper = new ObjectMapper();


    public List<ResourcesUpdated> getUpdateResourcesReceivedByListener() {
        return updateResourcesReceivedByListener;
    }
    public void setUpdateResourcesReceivedByListener(List<ResourcesUpdated> list) {
        this.updateResourcesReceivedByListener = list;
    }

    public int updateResourcesReceived() { return updateResourcesReceivedByListener.size(); }

    public void clearRequestsReceivedByListener() {
        updateResourcesReceivedByListener.clear();
    }

    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(value = "symbIoTe-el-resourcesUpdated", durable = "true", autoDelete = "true", exclusive = "true"),
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
}
