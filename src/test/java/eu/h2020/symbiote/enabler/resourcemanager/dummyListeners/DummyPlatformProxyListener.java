package eu.h2020.symbiote.enabler.resourcemanager.dummyListeners;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.h2020.symbiote.enabler.messaging.model.CancelTaskRequest;
import eu.h2020.symbiote.enabler.messaging.model.PlatformProxyAcquisitionStartRequest;
import eu.h2020.symbiote.enabler.messaging.model.PlatformProxyUpdateRequest;
import eu.h2020.symbiote.enabler.messaging.model.PlatformProxyResourceInfo;

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
public class DummyPlatformProxyListener {
    private static Logger log = LoggerFactory
            .getLogger(DummyPlatformProxyListener.class);

    private List<PlatformProxyAcquisitionStartRequest> startAcquisitionRequestsReceivedByListener = new ArrayList<>();
    private List<PlatformProxyUpdateRequest> updateAcquisitionRequestsReceivedByListener = new ArrayList<>();
    private List<CancelTaskRequest> cancelTaskRequestsReceivedByListener = new ArrayList<>();
    private ObjectMapper mapper = new ObjectMapper();

    public List<PlatformProxyAcquisitionStartRequest> getStartAcquisitionRequestsReceivedByListener() {
        return startAcquisitionRequestsReceivedByListener;
    }
    public void setStartAcquisitionRequestsReceivedByListener(List<PlatformProxyAcquisitionStartRequest> list) {
        this.startAcquisitionRequestsReceivedByListener = list;
    }

    public List<PlatformProxyUpdateRequest> getUpdateAcquisitionRequestsReceivedByListener() {
        return updateAcquisitionRequestsReceivedByListener;
    }
    public void setUpdateAcquisitionRequestsReceivedByListener(List<PlatformProxyUpdateRequest> list) {
        this.updateAcquisitionRequestsReceivedByListener = list;
    }

    public List<CancelTaskRequest> getCancelTaskRequestsReceivedByListener() {
        return cancelTaskRequestsReceivedByListener;
    }
    public void setCancelTaskRequestsReceivedByListener(List<CancelTaskRequest> list) {
        this.cancelTaskRequestsReceivedByListener = list;
    }

    public int startAcquisitionRequestsReceived() { return startAcquisitionRequestsReceivedByListener.size(); }
    public int updateAcquisitionRequestsReceived() { return updateAcquisitionRequestsReceivedByListener.size(); }
    public int cancelTaskRequestsReceived() { return cancelTaskRequestsReceivedByListener.size(); }

    public void clearRequestsReceivedByListener() {
        startAcquisitionRequestsReceivedByListener.clear();
        updateAcquisitionRequestsReceivedByListener.clear();
        cancelTaskRequestsReceivedByListener.clear();
    }

    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(value = "symbIoTe-pl-acquisitionStartRequested", durable = "${rabbit.exchange.enablerPlatformProxy.durable}",
                    autoDelete = "${rabbit.exchange.enablerPlatformProxy.autodelete}", exclusive = "true"),
            exchange = @Exchange(value = "${rabbit.exchange.enablerPlatformProxy.name}", ignoreDeclarationExceptions = "true",
                    durable = "${rabbit.exchange.enablerPlatformProxy.durable}", autoDelete  = "${rabbit.exchange.enablerPlatformProxy.autodelete}",
                    internal = "${rabbit.exchange.enablerPlatformProxy.internal}", type = "${rabbit.exchange.enablerPlatformProxy.type}"),
            key = "${rabbit.routingKey.enablerPlatformProxy.acquisitionStartRequested}")
    )
    public void platformProxyAcquisitionStartRequestedListener(PlatformProxyAcquisitionStartRequest request) {
        startAcquisitionRequestsReceivedByListener.add(request);

        try {
            String responseInString = mapper.writeValueAsString(request);
            log.info("PlatformProxyListener received request: " + responseInString);
            log.info("requestReceivedByListener.size() = " + startAcquisitionRequestsReceivedByListener.size());

            for(PlatformProxyResourceInfo req : request.getResources()) {
                log.info("request = " + req.getResourceId());
            }
        } catch (JsonProcessingException e) {
            log.info(e.toString());
        }
    }

    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(value = "symbIoTe-pl-cancelTasks", durable = "${rabbit.exchange.enablerPlatformProxy.durable}",
                    autoDelete = "${rabbit.exchange.enablerPlatformProxy.autodelete}", exclusive = "true"),
            exchange = @Exchange(value = "${rabbit.exchange.enablerPlatformProxy.name}", ignoreDeclarationExceptions = "true",
                    durable = "${rabbit.exchange.enablerPlatformProxy.durable}", autoDelete  = "${rabbit.exchange.enablerPlatformProxy.autodelete}",
                    internal = "${rabbit.exchange.enablerPlatformProxy.internal}", type = "${rabbit.exchange.enablerPlatformProxy.type}"),
            key = "${rabbit.routingKey.enablerPlatformProxy.resourcesUpdated}")
    )
    public void platformProxyResourcesUpdatedListener(PlatformProxyUpdateRequest request) {
        updateAcquisitionRequestsReceivedByListener.add(request);

        try {
            String responseInString = mapper.writeValueAsString(request);
            log.info("PlatformProxyListener received update request: " + responseInString);
            log.info("updateRequestReceivedByListener.size() = " + updateAcquisitionRequestsReceivedByListener.size());

            for(PlatformProxyResourceInfo req : request.getNewResources()) {
                log.info("request = " + req.getResourceId());
            }
        } catch (JsonProcessingException e) {
            log.info(e.toString());
        }
    }

    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(value = "symbIoTe-pl-acquisitionUpdateRequested", durable = "${rabbit.exchange.enablerPlatformProxy.durable}",
                    autoDelete = "${rabbit.exchange.enablerPlatformProxy.autodelete}", exclusive = "true"),
            exchange = @Exchange(value = "${rabbit.exchange.enablerPlatformProxy.name}", ignoreDeclarationExceptions = "true",
                    durable = "${rabbit.exchange.enablerPlatformProxy.durable}", autoDelete  = "${rabbit.exchange.enablerPlatformProxy.autodelete}",
                    internal = "${rabbit.exchange.enablerPlatformProxy.internal}", type = "${rabbit.exchange.enablerPlatformProxy.type}"),
            key = "${rabbit.routingKey.enablerPlatformProxy.cancelTasks}")
    )
    public void platformProxyCancelTasksListener(CancelTaskRequest request) {
        cancelTaskRequestsReceivedByListener.add(request);

        try {
            String responseInString = mapper.writeValueAsString(request);
            log.info("PlatformProxyListener received cancel request: " + responseInString);
            log.info("cancelTaskRequestsReceivedByListener.size() = " + cancelTaskRequestsReceivedByListener.size());

            for(String id : request.getTaskIdList()) {
                log.info("id = " + id);
            }
        } catch (JsonProcessingException e) {
            log.info(e.toString());
        }
    }
}
