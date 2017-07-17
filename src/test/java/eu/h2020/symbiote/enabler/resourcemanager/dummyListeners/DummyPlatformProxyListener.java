package eu.h2020.symbiote.enabler.resourcemanager.dummyListeners;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.h2020.symbiote.enabler.messaging.model.PlatformProxyAcquisitionStartRequest;
import eu.h2020.symbiote.enabler.messaging.model.PlatformProxyResourceInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.util.ArrayList;

/**
 * Created by lebro_000 on 7/17/2017.
 */
@Component
public class DummyPlatformProxyListener {
    private static Logger log = LoggerFactory
            .getLogger(DummyPlatformProxyListener.class);

    private ArrayList<PlatformProxyAcquisitionStartRequest> requestReceivedByListener = new ArrayList<>();
    private ObjectMapper mapper = new ObjectMapper();

    public ArrayList<PlatformProxyAcquisitionStartRequest> getRequestReceivedByListener() { return this.requestReceivedByListener; }
    public void setRequestReceivedByListener(ArrayList<PlatformProxyAcquisitionStartRequest> list) { this.requestReceivedByListener = list; }

    public int messagesReceived() { return requestReceivedByListener.size(); }
    public void clearRequestReceivedByListener() { requestReceivedByListener.clear(); }

    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(value = "symbIoTe-rap-writeResource", durable = "true", autoDelete = "true", exclusive = "true"),
            exchange = @Exchange(value = "symbIoTe.enablerPlatformProxy", ignoreDeclarationExceptions = "true", type = ExchangeTypes.TOPIC),
            key = "symbIoTe.enablerPlatformProxy.acquisitionStartRequested")
    )
    public void platformProxyListener(PlatformProxyAcquisitionStartRequest request) {
        requestReceivedByListener.add(request);

        try {
            String responseInString = mapper.writeValueAsString(request);
            log.info("PlatformProxyListener received request: " + responseInString);
            log.info("requestReceivedByListener.size() = " + requestReceivedByListener.size());

            for(PlatformProxyResourceInfo req : request.getResources()) {
                log.info("request = " + req.getResourceId());
            }
        } catch (JsonProcessingException e) {
            log.info(e.toString());
        }
    }
}
