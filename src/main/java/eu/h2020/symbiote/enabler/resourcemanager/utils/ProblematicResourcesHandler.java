package eu.h2020.symbiote.enabler.resourcemanager.utils;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.h2020.symbiote.enabler.messaging.model.ProblematicResourcesInfo;
import eu.h2020.symbiote.enabler.messaging.model.ProblematicResourcesMessage;
import eu.h2020.symbiote.enabler.resourcemanager.model.TaskInfo;
import eu.h2020.symbiote.enabler.resourcemanager.model.ProblematicResourcesHandlerStatus;
import eu.h2020.symbiote.enabler.resourcemanager.model.ProblematicResourcesHandlerResult;
import eu.h2020.symbiote.enabler.resourcemanager.repository.TaskInfoRepository;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by vasgl on 7/20/2017.
 */
public final class ProblematicResourcesHandler {
    private static Log log = LogFactory.getLog(ProblematicResourcesHandler.class);

    private ProblematicResourcesHandler() {
        // empty constructor
    }

    public static void replaceProblematicResources(String requestInString,
                                                   TaskInfoRepository taskInfoRepository) throws IOException {

        ObjectMapper mapper = new ObjectMapper();

        try {
            ProblematicResourcesMessage problematicResourcesMessage =  mapper.readValue(requestInString, ProblematicResourcesMessage.class);

            for(ProblematicResourcesInfo problematicResourcesInfo : problematicResourcesMessage.getProblematicResourcesInfoList()) {
                TaskInfo taskInfo = taskInfoRepository.findByTaskId(problematicResourcesInfo.getTaskId());
                if (taskInfo == null) {
                    log.info("The task with id = " + problematicResourcesInfo.getTaskId() + " does not exist!");
                } else {
                    ProblematicResourcesHandlerResult problematicResourcesHandlerResult = ProblematicResourcesHandler.
                            replaceProblematicResourcesIfTaskExists(problematicResourcesInfo, taskInfo);
                    taskInfoRepository.save(problematicResourcesHandlerResult.getTaskInfo());

                    // ToDo: reply to PlatformProxy and EnablerLogic
                }
            }
        } catch (JsonParseException | JsonMappingException e) {
            log.error("Error occurred during deserializing ProblematicResourcesMessage", e);
        }

    }

    public static ProblematicResourcesHandlerResult replaceProblematicResourcesIfTaskExists(ProblematicResourcesInfo problematicResourcesInfo,
                                               TaskInfo taskInfo) {

        // ToDo: Implement behavior when resourcesIds are cached and not enough resources are available
        // ToDo: Implement behavior when resourcesIds are not cached
        // ToDo: Distinguish between unavailable and wrong data resources

        log.info("problematicResourcesInfo = " + problematicResourcesInfo);
        String taskId = problematicResourcesInfo.getTaskId();

        log.info("taskInfo = " + taskInfo);
        log.info("taskInfo.getMinNoResources() = " + taskInfo.getMinNoResources());
        log.info("taskInfo.getResourceIds().size() = " + taskInfo.getResourceIds().size());
        log.info("problematicResourcesInfo.getProblematicResourceIds().size() = " + problematicResourcesInfo.getProblematicResourceIds().size());
        Integer noNewResourcesNeeded = taskInfo.getMinNoResources() - taskInfo.getResourceIds().size() +
                problematicResourcesInfo.getProblematicResourceIds().size();

        if (taskInfo.getAllowCaching()) {
            if (noNewResourcesNeeded <= taskInfo.getStoredResourceIds().size()) {
                log.info("Task with id = " + taskId + " has enough resources to replace the problematic ones.");

                List<String> newResourceIds = new ArrayList<>();

                for (Integer i = 0; i < noNewResourcesNeeded ; i++) {
                    newResourceIds.add(taskInfo.getStoredResourceIds().get(0));
                    taskInfo.getStoredResourceIds().remove(0);
                }

                taskInfo.getResourceIds().removeAll(problematicResourcesInfo.getProblematicResourceIds());
                taskInfo.getResourceIds().addAll(newResourceIds);

                return resourcesReplacedSuccessfully(taskInfo);
            }
        }

        return unknownMessage();
    }

    private static ProblematicResourcesHandlerResult resourcesReplacedSuccessfully(TaskInfo taskInfo) {
        return new ProblematicResourcesHandlerResult(ProblematicResourcesHandlerStatus.RESOURCES_REPLACED_SUCCESSFULLY, taskInfo);
    }

    private static ProblematicResourcesHandlerResult unknownMessage() {
        return new ProblematicResourcesHandlerResult(ProblematicResourcesHandlerStatus.UNKNOWN, null);
    }
}
