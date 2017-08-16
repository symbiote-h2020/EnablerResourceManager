package eu.h2020.symbiote.enabler.resourcemanager.model;

import eu.h2020.symbiote.core.ci.QueryResourceResult;
import eu.h2020.symbiote.core.ci.QueryResponse;
import eu.h2020.symbiote.core.internal.CoreQueryRequest;
import eu.h2020.symbiote.enabler.messaging.model.ResourceManagerTaskInfoRequest;
import eu.h2020.symbiote.enabler.messaging.model.ResourceManagerTaskInfoResponse;
import org.springframework.data.annotation.Id;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Created by vasgl on 7/17/2017.
 */
public class TaskInfo extends ResourceManagerTaskInfoResponse {

    private List<String> storedResourceIds;

    public TaskInfo() {
        storedResourceIds = new ArrayList<>();
    }

    public TaskInfo (ResourceManagerTaskInfoRequest resourceManagerTaskInfoRequest) {
        super(resourceManagerTaskInfoRequest);
        setResourceIds(new ArrayList<>());
        storedResourceIds = new ArrayList<>();
    }

    public TaskInfo (ResourceManagerTaskInfoResponse resourceManagerTaskInfoResponse) {
        super(resourceManagerTaskInfoResponse);
        storedResourceIds = new ArrayList<>();
    }

    public TaskInfo (TaskInfo taskInfo) {
        this((ResourceManagerTaskInfoResponse) taskInfo);
        setStoredResourceIds(new ArrayList<>(taskInfo.getStoredResourceIds()));
    }

    public List<String> getStoredResourceIds() { return storedResourceIds; }
    public void setStoredResourceIds(List<String> list) { this.storedResourceIds = list; }

    public void calculateStoredResourceIds(QueryResponse queryResponse) {
        for (QueryResourceResult result : queryResponse.getResources()) {
            if (!getResourceIds().contains(result.getId())) {
                storedResourceIds.add(result.getId());
            }
        }
    }

    public void updateTaskInfo(ResourceManagerTaskInfoRequest request) {
        this.setTaskId(request.getTaskId());
        this.setMinNoResources(request.getMinNoResources());
        this.setCoreQueryRequest(request.getCoreQueryRequest());
        this.setQueryInterval(request.getQueryInterval());
        this.setAllowCaching(request.getAllowCaching());
        this.setCachingInterval(request.getCachingInterval());
        this.setInformPlatformProxy(request.getInformPlatformProxy());
        this.setEnablerLogicName(request.getEnablerLogicName());
    }

    @Override
    public boolean equals(Object o) {
        // self check
        if (this == o)
            return true;

        // null check
        if (o == null)
            return false;

        // type check and cast
        if (!(o instanceof TaskInfo))
            return false;

        TaskInfo taskInfo = (TaskInfo) o;
        // field comparison
        return Objects.equals(this.getTaskId(), taskInfo.getTaskId())
                && Objects.equals(this.getMinNoResources(), taskInfo.getMinNoResources())
                && Objects.equals(this.getCoreQueryRequest(), taskInfo.getCoreQueryRequest())
                && Objects.equals(this.getQueryInterval(), taskInfo.getQueryInterval())
                && Objects.equals(this.getAllowCaching(), taskInfo.getAllowCaching())
                && Objects.equals(this.getCachingInterval(), taskInfo.getCachingInterval())
                && Objects.equals(this.getInformPlatformProxy(), taskInfo.getInformPlatformProxy())
                && Objects.equals(this.getEnablerLogicName(), taskInfo.getEnablerLogicName())
                && Objects.equals(this.getResourceIds(), taskInfo.getResourceIds())
                && Objects.equals(this.getStoredResourceIds(), taskInfo.getStoredResourceIds());
    }

    //    Todo: Implement update in storedResourceIds

}
