package eu.h2020.symbiote.enabler.resourcemanager.model;

import eu.h2020.symbiote.enabler.messaging.model.PlatformProxyAcquisitionStartRequest;
import eu.h2020.symbiote.enabler.messaging.model.PlatformProxyTaskInfo;
import eu.h2020.symbiote.enabler.messaging.model.ResourceManagerTaskInfoResponse;

import java.util.ArrayList;

/**
 * Created by vasgl on 7/20/2017.
 */
public class QueryAndProcessSearchResponseResult {
    private ResourceManagerTaskInfoResponse resourceManagerTaskInfoResponse;
    private PlatformProxyTaskInfo platformProxyTaskInfo;
    private TaskInfo taskInfo;

    public QueryAndProcessSearchResponseResult() {
        // Empty constructor
    }

    public ResourceManagerTaskInfoResponse getResourceManagerTaskInfoResponse() { return this.resourceManagerTaskInfoResponse; }
    public void setResourceManagerTaskInfoResponse(ResourceManagerTaskInfoResponse resourceManagerTaskInfoResponse) {
        this.resourceManagerTaskInfoResponse = resourceManagerTaskInfoResponse;
    }

    public PlatformProxyTaskInfo getPlatformProxyTaskInfo() { return this.platformProxyTaskInfo; }
    public void setPlatformProxyTaskInfo(PlatformProxyTaskInfo platformProxyTaskInfo) {
        this.platformProxyTaskInfo = platformProxyTaskInfo;
    }

    public TaskInfo getTaskInfo() { return taskInfo; }
    public void setTaskInfo(TaskInfo taskInfo) { this.taskInfo = taskInfo; }

}
