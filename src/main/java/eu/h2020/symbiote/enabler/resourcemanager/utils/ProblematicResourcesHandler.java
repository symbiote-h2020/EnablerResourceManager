package eu.h2020.symbiote.enabler.resourcemanager.utils;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.h2020.symbiote.enabler.messaging.model.*;
import eu.h2020.symbiote.enabler.resourcemanager.model.QueryAndProcessSearchResponseResult;
import eu.h2020.symbiote.enabler.resourcemanager.model.TaskInfo;
import eu.h2020.symbiote.enabler.resourcemanager.model.ProblematicResourcesHandlerStatus;
import eu.h2020.symbiote.enabler.resourcemanager.model.ProblematicResourcesHandlerResult;
import eu.h2020.symbiote.enabler.resourcemanager.repository.TaskInfoRepository;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by vasgl on 7/20/2017.
 */
@Component
public final class ProblematicResourcesHandler {
    private static Log log = LogFactory.getLog(ProblematicResourcesHandler.class);

    private RabbitTemplate rabbitTemplate;
    private SearchHelper searchHelper;

    @Value("${rabbit.exchange.enablerPlatformProxy.name}")
    private String platformProxyExchange;
    @Value("${rabbit.exchange.enablerLogic.name}")
    private String enablerLogicExchange;

    @Value("${rabbit.routingKey.enablerPlatformProxy.resourcesUpdated}")
    private String platformProxyResourcesUpdatedKey;
    @Value("${rabbit.routingKey.enablerLogic.resourcesUpdated}")
    private String genericEnablerLogicResourcesUpdatedKey;


    @Autowired
    private ProblematicResourcesHandler(RabbitTemplate rabbitTemplate, SearchHelper searchHelper) {
        Assert.notNull(rabbitTemplate,"RabbitTemplate can not be null!");
        this.rabbitTemplate = rabbitTemplate;

        Assert.notNull(searchHelper,"SearchHelper can not be null!");
        this.searchHelper = searchHelper;
    }

    public void replaceProblematicResources(String requestInString,
                                                   TaskInfoRepository taskInfoRepository) throws IOException {

        ObjectMapper mapper = new ObjectMapper();

        try {
            ProblematicResourcesMessage problematicResourcesMessage =  mapper.readValue(requestInString, ProblematicResourcesMessage.class);

            for(ProblematicResourcesInfo problematicResourcesInfo : problematicResourcesMessage.getProblematicResourcesInfoList()) {
                TaskInfo taskInfo = taskInfoRepository.findByTaskId(problematicResourcesInfo.getTaskId());
                if (taskInfo == null) {
                    log.info("The task with id = " + problematicResourcesInfo.getTaskId() + " does not exist!");
                } else {
                    ProblematicResourcesHandlerResult problematicResourcesHandlerResult =
                            replaceProblematicResourcesIfTaskExists(problematicResourcesInfo, taskInfo);

                    TaskInfo newTaskInfo = problematicResourcesHandlerResult.getTaskInfo();
                    taskInfoRepository.save(newTaskInfo);

                    // ToDo: reply to PlatformProxy and EnablerLogic
                    // Inform Platform Proxy
                    if (taskInfo.getInformPlatformProxy()) {

                        PlatformProxyUpdateRequest platformProxyUpdateRequest = new PlatformProxyUpdateRequest(newTaskInfo.getTaskId(),
                                problematicResourcesHandlerResult.getPlatformProxyResourceInfoList());

                        // Sending requests to PlatformProxy
                        rabbitTemplate.convertAndSend(platformProxyExchange, platformProxyResourcesUpdatedKey,
                                platformProxyUpdateRequest);
                    }

                    // Inform Enabler Logic
                    ResourcesUpdated resourcesUpdated = new ResourcesUpdated(newTaskInfo.getTaskId(),
                            problematicResourcesHandlerResult.getNewResources());
                    String specificEnablerLogicResourcesUpdatedKey = genericEnablerLogicResourcesUpdatedKey + "." + newTaskInfo.getEnablerLogicName();
                    rabbitTemplate.convertAndSend(enablerLogicExchange, specificEnablerLogicResourcesUpdatedKey,
                            resourcesUpdated);

                }
            }
        } catch (JsonParseException | JsonMappingException e) {
            log.error("Error occurred during deserializing ProblematicResourcesMessage", e);
        }

    }

    public ProblematicResourcesHandlerResult replaceProblematicResourcesIfTaskExists(ProblematicResourcesInfo problematicResourcesInfo,
                                               TaskInfo taskInfo) {

        // ToDo: Implement behavior when resourcesIds are cached and not enough resources are available
        // ToDo: Implement behavior when resourcesIds are not cached
        // ToDo: Distinguish between unavailable and wrong data resources

        log.info("problematicResourcesInfo = " + problematicResourcesInfo);
        String taskId = problematicResourcesInfo.getTaskId();

        log.debug("taskInfo = " + taskInfo);
        log.debug("taskInfo.getMinNoResources() = " + taskInfo.getMinNoResources());
        log.debug("taskInfo.getResourceIds().size() = " + taskInfo.getResourceIds().size());
        log.debug("problematicResourcesInfo.getProblematicResourceIds().size() = " + problematicResourcesInfo.getProblematicResourceIds().size());

        Integer noNewResourcesNeeded = taskInfo.getMinNoResources() - taskInfo.getResourceIds().size() +
                problematicResourcesInfo.getProblematicResourceIds().size();

        if (taskInfo.getAllowCaching()) {
            if (noNewResourcesNeeded <= taskInfo.getStoredResourceIds().size()) {
                log.info("Task with id = " + taskId + " has enough resources to replace the problematic ones.");

                List<String> newResourceIds = new ArrayList<>();
                QueryAndProcessSearchResponseResult queryAndProcessSearchResponseResult = null;
                List<PlatformProxyResourceInfo> platformProxyResourceInfoList = new ArrayList<>();

                while (newResourceIds.size() != noNewResourcesNeeded) {
                    String candidateResourceId = taskInfo.getStoredResourceIds().get(0);
                    String queryUrl = searchHelper.buildRequestUrl(candidateResourceId);

                    queryAndProcessSearchResponseResult =
                            searchHelper.queryAndProcessSearchResponse(queryUrl, taskInfo);

                    if (queryAndProcessSearchResponseResult.getTaskInfo().getResourceIds().size() != 0) {
                        newResourceIds.add(candidateResourceId);

                        if (taskInfo.getInformPlatformProxy()) {
                            platformProxyResourceInfoList.addAll(queryAndProcessSearchResponseResult.
                                    getPlatformProxyAcquisitionStartRequest().getResources());
                        }
                    }

                    //ToDo: add it to another list if CRAM does not respond with a url
                    taskInfo.getStoredResourceIds().remove(0);
                }

                taskInfo.getResourceIds().removeAll(problematicResourcesInfo.getProblematicResourceIds());
                taskInfo.getResourceIds().addAll(newResourceIds);

                return resourcesReplacedSuccessfully(taskInfo, platformProxyResourceInfoList, newResourceIds);
            }
        }

        return unknownMessage();
    }

    private ProblematicResourcesHandlerResult resourcesReplacedSuccessfully(TaskInfo taskInfo,
                                                                            List<PlatformProxyResourceInfo> platformProxyResourceInfoList,
                                                                            List<String> newResourceIds) {
        return new ProblematicResourcesHandlerResult(ProblematicResourcesHandlerStatus.RESOURCES_REPLACED_SUCCESSFULLY,
                taskInfo, platformProxyResourceInfoList, newResourceIds);
    }

    private ProblematicResourcesHandlerResult unknownMessage() {
        return new ProblematicResourcesHandlerResult(ProblematicResourcesHandlerStatus.UNKNOWN, null, null, null);
    }
}
