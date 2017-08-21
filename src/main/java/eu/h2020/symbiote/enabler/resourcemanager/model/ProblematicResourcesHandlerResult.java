package eu.h2020.symbiote.enabler.resourcemanager.model;

import eu.h2020.symbiote.enabler.messaging.model.NotEnoughResourcesAvailable;
import eu.h2020.symbiote.enabler.messaging.model.PlatformProxyResourceInfo;

import java.util.List;

/**
 * Created by vasgl on 7/20/2017.
 */
public class ProblematicResourcesHandlerResult {
    private ProblematicResourcesHandlerStatus status;
    private TaskInfo taskInfo;
    private NotEnoughResourcesAvailable notEnoughResourcesAvailable;

    public ProblematicResourcesHandlerResult(ProblematicResourcesHandlerStatus status, TaskInfo taskInfo,
                                             NotEnoughResourcesAvailable notEnoughResourcesAvailable) {
        this.status = status;
        this.taskInfo = taskInfo;
        this.notEnoughResourcesAvailable = notEnoughResourcesAvailable;
    }

    public ProblematicResourcesHandlerStatus getStatus() { return status; }
    public void setStatus(ProblematicResourcesHandlerStatus status) { this.status = status; }

    public TaskInfo getTaskInfo() { return taskInfo; }
    public void setTaskInfo(TaskInfo taskInfo) { this.taskInfo = taskInfo; }

    public NotEnoughResourcesAvailable getNotEnoughResourcesAvailable() { return notEnoughResourcesAvailable; }
    public void setNotEnoughResourcesAvailable(NotEnoughResourcesAvailable notEnoughResourcesAvailable) {
        this.notEnoughResourcesAvailable = notEnoughResourcesAvailable;
    }

    public static ProblematicResourcesHandlerResult resourcesReplacedSuccessfully(TaskInfo taskInfo) {
        return new ProblematicResourcesHandlerResult(ProblematicResourcesHandlerStatus.RESOURCES_REPLACED_SUCCESSFULLY,
                taskInfo,null);
    }

    public static ProblematicResourcesHandlerResult enoughResources(TaskInfo taskInfo) {
        return new ProblematicResourcesHandlerResult(ProblematicResourcesHandlerStatus.ENOUGH_RESOURCES,
                taskInfo, null);
    }

    public static ProblematicResourcesHandlerResult notEnoughResources(TaskInfo taskInfo,
                                                                       NotEnoughResourcesAvailable notEnoughResourcesAvailable) {
        return new ProblematicResourcesHandlerResult(ProblematicResourcesHandlerStatus.NOT_ENOUGH_RESOURCES, taskInfo, notEnoughResourcesAvailable);
    }

    public static ProblematicResourcesHandlerResult unknownMessage() {
        return new ProblematicResourcesHandlerResult(ProblematicResourcesHandlerStatus.UNKNOWN, null, null);
    }

}
