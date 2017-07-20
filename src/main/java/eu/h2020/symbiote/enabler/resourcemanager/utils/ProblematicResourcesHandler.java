package eu.h2020.symbiote.enabler.resourcemanager.utils;

import eu.h2020.symbiote.enabler.messaging.model.ProblematicResourcesInfo;
import eu.h2020.symbiote.enabler.resourcemanager.model.TaskInfo;
import eu.h2020.symbiote.enabler.resourcemanager.model.ProblematicResourcesHandlerStatus;
import eu.h2020.symbiote.enabler.resourcemanager.model.ProblematicResourcesHandlerResult;
import eu.h2020.symbiote.enabler.resourcemanager.repository.TaskInfoRepository;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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

    public static void replaceProblematicResources(ProblematicResourcesInfo problematicResourcesInfo,
                                                   TaskInfo taskInfo, TaskInfoRepository taskInfoRepository) {
        if (taskInfo == null) {
            log.info("The task with id = " + problematicResourcesInfo.getTaskId() + " does not exist!");
        } else {
            ProblematicResourcesHandlerResult problematicResourcesHandlerResult = ProblematicResourcesHandler.
                    replaceProblematicResourcesIfTaskExists(problematicResourcesInfo, taskInfo);
            taskInfoRepository.save(problematicResourcesHandlerResult.getTaskInfo());

            // ToDo: reply to PlatformProxy and EnablerLogic
        }
    }

    public static ProblematicResourcesHandlerResult replaceProblematicResourcesIfTaskExists(ProblematicResourcesInfo problematicResourcesInfo,
                                               TaskInfo taskInfo) {

        // ToDo: Implement behavior when resourcesIds are cached enough resources are available
        // ToDo: Implement behavior when resourcesIds are cached not enough resources are available
        // ToDo: Implement behavior when resourcesIds are not cached
        // ToDo: Distinguish between unavailable and wrong data resources

        String taskId = problematicResourcesInfo.getTaskId();
        Integer noNewResourcesNeeded = taskInfo.getCount() - taskInfo.getResourceIds().size() +
                problematicResourcesInfo.getProblematicResourceIds().size();

        if (taskInfo.getAllowCaching()) {

        }
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

        return unknownMessage();
    }

    private static ProblematicResourcesHandlerResult resourcesReplacedSuccessfully(TaskInfo taskInfo) {
        return new ProblematicResourcesHandlerResult(ProblematicResourcesHandlerStatus.RESOURCES_REPLACED_SUCCESSFULLY, taskInfo);
    }

    private static ProblematicResourcesHandlerResult unknownMessage() {
        return new ProblematicResourcesHandlerResult(ProblematicResourcesHandlerStatus.UNKNOWN, null);
    }
}
