package eu.h2020.symbiote.enabler.resourcemanager.model;

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

    public ProblematicResourcesHandlerResult(ProblematicResourcesHandlerStatus status, TaskInfo taskInfo,
                                             List<PlatformProxyResourceInfo> platformProxyResourceInfoList,
                                             List<String> newResources) {
        this.status = status;
        this.taskInfo = taskInfo;
        this.platformProxyResourceInfoList = platformProxyResourceInfoList;
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

}
