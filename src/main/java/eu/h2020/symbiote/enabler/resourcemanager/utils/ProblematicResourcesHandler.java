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

import eu.h2020.symbiote.util.IntervalFormatter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

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

    @Value("${rabbit.routingKey.enablerPlatformProxy.taskUpdated}")
    private String platformProxyTaskUpdatedKey;
    @Value("${rabbit.routingKey.enablerLogic.resourcesUpdated}")
    private String genericEnablerLogicResourcesUpdatedKey;
    @Value("${rabbit.routingKey.enablerLogic.notEnoughResources}")
    private String genericEnablerLogicNotEnoughResourcesKey;

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

                    if (problematicResourcesHandlerResult.getStatus() != ProblematicResourcesHandlerStatus.UNKNOWN) {
                        TaskInfo newTaskInfo = problematicResourcesHandlerResult.getTaskInfo();
                        taskInfoRepository.save(newTaskInfo);

                        if (problematicResourcesHandlerResult.getStatus() ==
                                ProblematicResourcesHandlerStatus.RESOURCES_REPLACED_SUCCESSFULLY) {

                            log.info("There were enough resources to replace the problematic ones for task " + newTaskInfo.getTaskId());
                            informComponents(newTaskInfo);
                        } else if (problematicResourcesHandlerResult.getStatus() == ProblematicResourcesHandlerStatus.ENOUGH_RESOURCES) {

                            log.info("There were enough remaining resources to replace the problematic ones for task " + newTaskInfo.getTaskId());
                            informComponents(newTaskInfo);
                        } else if (problematicResourcesHandlerResult.getStatus() == ProblematicResourcesHandlerStatus.NOT_ENOUGH_RESOURCES) {
                            log.info("There were NOT enough resources to replace the problematic ones for task " + newTaskInfo.getTaskId());

                            // Inform Enabler Logic that not enough resources are available
                            String specificEnablerLogicNotEnoughResourcesKey = genericEnablerLogicNotEnoughResourcesKey +
                                    "." + newTaskInfo.getEnablerLogicName();

                            rabbitTemplate.convertAndSend(enablerLogicExchange, specificEnablerLogicNotEnoughResourcesKey,
                                    problematicResourcesHandlerResult.getNotEnoughResourcesAvailable());
                        }
                    }
                }
            }
        } catch (JsonParseException | JsonMappingException e) {
            log.error("Error occurred during deserializing ProblematicResourcesMessage", e);
        }

    }

    public ProblematicResourcesHandlerResult replaceProblematicResourcesIfTaskExists(ProblematicResourcesInfo problematicResourcesInfo,
                                               TaskInfo taskInfo) {

        // ToDo: Implement behavior when resourcesIds are not cached
        // ToDo: Distinguish between unavailable and wrong data resources

        log.debug("problematicResourcesInfo = " + problematicResourcesInfo);
        String taskId = problematicResourcesInfo.getTaskId();

        log.debug("taskInfo = " + taskInfo);
        log.debug("taskInfo.getMinNoResources() = " + taskInfo.getMinNoResources());
        log.debug("taskInfo.getResourceIds().size() = " + taskInfo.getResourceIds().size());
        log.debug("problematicResourcesInfo.getProblematicResourceIds().size() = " +
                problematicResourcesInfo.getProblematicResourceIds().size());

        Integer noNewResourcesNeeded = taskInfo.getMinNoResources() - taskInfo.getResourceIds().size() +
                problematicResourcesInfo.getProblematicResourceIds().size();

        if (noNewResourcesNeeded > 0) {
            if (taskInfo.getAllowCaching()) {
                log.debug("Task " + taskInfo.getTaskId() + " has allowCaching = TRUE");

                Map<String, String> newResourceUrls = new HashMap<>();

                while (newResourceUrls.size() != noNewResourcesNeeded &&
                        taskInfo.getStoredResourceIds().size() != 0) {
                    String candidateResourceId = taskInfo.getStoredResourceIds().get(0);
                    String candidateResourceUrl = searchHelper.querySingleResource(candidateResourceId);

                    if (candidateResourceUrl != null) {
                        newResourceUrls.put(candidateResourceId, candidateResourceUrl);
                    }

                    //ToDo: add it to another list if CRAM does not respond with a url
                    taskInfo.getStoredResourceIds().remove(0);
                }

                // ToDo: add it to another list, not just remove it
                taskInfo.deleteResourceIds(problematicResourcesInfo.getProblematicResourceIds());
                taskInfo.addResourceIds(newResourceUrls);

                if (newResourceUrls.size() == noNewResourcesNeeded) {
                    return ProblematicResourcesHandlerResult.resourcesReplacedSuccessfully(taskInfo);
                } else {
                    log.info("Not enough resources are available.");

                    taskInfo.setStatus(ResourceManagerTaskInfoResponseStatus.NOT_ENOUGH_RESOURCES);
                    NotEnoughResourcesAvailable notEnoughResourcesAvailable = new NotEnoughResourcesAvailable(taskInfo.getTaskId(),
                            taskInfo.getResourceIds().size());

                    return ProblematicResourcesHandlerResult.notEnoughResources(taskInfo,notEnoughResourcesAvailable);
                }
            } else {
                log.debug("Task " + taskInfo.getTaskId() + " has allowCaching = FALSE");

                // Perform the request
                QueryAndProcessSearchResponseResult newQueryAndProcessSearchResponseResult = searchHelper
                        .queryAndProcessSearchResponse(taskInfo);

                // Store the taskInfo
                TaskInfo newTaskInfo = newQueryAndProcessSearchResponseResult.getTaskInfo();
                if (newTaskInfo.getStatus() == ResourceManagerTaskInfoResponseStatus.SUCCESS) {
                    // Return the newTaskInfo
                    return ProblematicResourcesHandlerResult.resourcesReplacedSuccessfully(newTaskInfo);
                } else {
                    // Return the old taskInfo, after removing the problematic resources
                    taskInfo.deleteResourceIds(problematicResourcesInfo.getProblematicResourceIds());

                    // Make sure that storeResourceIds is empty
                    taskInfo.setStoredResourceIds(new ArrayList<>());

                    taskInfo.setStatus(ResourceManagerTaskInfoResponseStatus.NOT_ENOUGH_RESOURCES);

                    NotEnoughResourcesAvailable notEnoughResourcesAvailable = new NotEnoughResourcesAvailable(taskInfo.getTaskId(),
                            taskInfo.getResourceIds().size());

                    return ProblematicResourcesHandlerResult.notEnoughResources(taskInfo,notEnoughResourcesAvailable);
                }
            }
        } else {
            taskInfo.deleteResourceIds(problematicResourcesInfo.getProblematicResourceIds());

            log.info("The remaining resources of task " + taskId + " are enough: minNoResources = " +
                    taskInfo.getMinNoResources() + ", acquired resources = " + taskInfo.getResourceIds().size());
            return ProblematicResourcesHandlerResult.enoughResources(taskInfo);
        }


        // ToDo: Remove when implement behavior for allowCaching == false
        // return ProblematicResourcesHandlerResult.unknownMessage();
    }

    private void informComponents(TaskInfo taskInfo) {
        // Inform Platform Proxy
        if (taskInfo.getInformPlatformProxy()) {

            PlatformProxyUpdateRequest platformProxyUpdateRequest = new PlatformProxyUpdateRequest();
            platformProxyUpdateRequest.setTaskId(taskInfo.getTaskId());
            platformProxyUpdateRequest.setQueryInterval_ms(new IntervalFormatter(taskInfo
                    .getQueryInterval()).getMillis());
            platformProxyUpdateRequest.setEnablerLogicName(taskInfo.getEnablerLogicName());
            platformProxyUpdateRequest.setResources(taskInfo.createPlatformProxyResourceInfoList());


            // Sending requests to PlatformProxy about the new resource ids of the task
            rabbitTemplate.convertAndSend(platformProxyExchange, platformProxyTaskUpdatedKey,
                    platformProxyUpdateRequest);
        }

        // Inform Enabler Logic about the new resource ids of the task
        ResourcesUpdated resourcesUpdated = new ResourcesUpdated(taskInfo.getTaskId(),
                taskInfo.getResourceIds());
        String specificEnablerLogicResourcesUpdatedKey = genericEnablerLogicResourcesUpdatedKey + "." +
                taskInfo.getEnablerLogicName();
        rabbitTemplate.convertAndSend(enablerLogicExchange, specificEnablerLogicResourcesUpdatedKey,
                resourcesUpdated);
    }


}
