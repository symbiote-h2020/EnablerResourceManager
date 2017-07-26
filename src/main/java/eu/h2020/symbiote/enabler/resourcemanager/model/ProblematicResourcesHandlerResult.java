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
    private List<PlatformProxyResourceInfo> platformProxyResourceInfoList;
    private List<String> newResources;
    private NotEnoughResourcesAvailable notEnoughResourcesAvailable;

    public ProblematicResourcesHandlerResult(ProblematicResourcesHandlerStatus status, TaskInfo taskInfo,
                                             List<PlatformProxyResourceInfo> platformProxyResourceInfoList,
                                             NotEnoughResourcesAvailable notEnoughResourcesAvailable,
                                             List<String> newResources) {
        this.status = status;
        this.taskInfo = taskInfo;
        this.platformProxyResourceInfoList = platformProxyResourceInfoList;
        this.notEnoughResourcesAvailable = notEnoughResourcesAvailable;
        this.newResources = newResources;
    }

    public ProblematicResourcesHandlerStatus getStatus() { return status; }
    public void setStatus(ProblematicResourcesHandlerStatus status) { this.status = status; }

    public TaskInfo getTaskInfo() { return taskInfo; }
    public void setTaskInfo(TaskInfo taskInfo) { this.taskInfo = taskInfo; }

    public List<PlatformProxyResourceInfo> getPlatformProxyResourceInfoList() {return platformProxyResourceInfoList; }
    public void setPlatformProxyResourceInfoList(List<PlatformProxyResourceInfo> platformProxyResourceInfoList) {
        this.platformProxyResourceInfoList = platformProxyResourceInfoList;
    }

    public List<String> getNewResources() { return newResources; }
    public void setNewResources(List<String> newResources) { this.newResources = newResources; }

    public NotEnoughResourcesAvailable getNotEnoughResourcesAvailable() { return notEnoughResourcesAvailable; }
    public void setNotEnoughResourcesAvailable(NotEnoughResourcesAvailable notEnoughResourcesAvailable) {
        this.notEnoughResourcesAvailable = notEnoughResourcesAvailable;
    }

    public static ProblematicResourcesHandlerResult resourcesReplacedSuccessfully(TaskInfo taskInfo,
                                                                            List<PlatformProxyResourceInfo> platformProxyResourceInfoList,
                                                                            List<String> newResourceIds) {
        return new ProblematicResourcesHandlerResult(ProblematicResourcesHandlerStatus.RESOURCES_REPLACED_SUCCESSFULLY,
                taskInfo, platformProxyResourceInfoList, null, newResourceIds);
    }

    public static ProblematicResourcesHandlerResult notEnoughResources(TaskInfo taskInfo,
                                                                       NotEnoughResourcesAvailable notEnoughResourcesAvailable) {
        return new ProblematicResourcesHandlerResult(ProblematicResourcesHandlerStatus.NOT_ENOUGH_RESOURCES, taskInfo, null, notEnoughResourcesAvailable, null);
    }

    public static ProblematicResourcesHandlerResult unknownMessage() {
        return new ProblematicResourcesHandlerResult(ProblematicResourcesHandlerStatus.UNKNOWN, null, null, null, null);
    }

}
