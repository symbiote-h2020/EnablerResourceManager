package eu.h2020.symbiote.enabler.resourcemanager.model;

import eu.h2020.symbiote.core.ci.QueryResourceResult;
import eu.h2020.symbiote.core.ci.QueryResponse;
import eu.h2020.symbiote.enabler.messaging.model.ResourceManagerTaskInfoRequest;
import eu.h2020.symbiote.enabler.messaging.model.ResourceManagerTaskInfoResponse;
import eu.h2020.symbiote.enabler.messaging.model.ResourceManagerTaskInfoResponseStatus;

import java.util.*;

/**
 * Created by vasgl on 7/17/2017.
 */
public class TaskInfo extends ResourceManagerTaskInfoResponse {

    private List<String> storedResourceIds;
    private Map<String, String>
            resourceUrls;

    public TaskInfo() {
        setResourceIds(new ArrayList<>());
        storedResourceIds = new ArrayList<>();
        resourceUrls = new HashMap<>();
    }

    public TaskInfo (ResourceManagerTaskInfoRequest resourceManagerTaskInfoRequest) {
        super(resourceManagerTaskInfoRequest);
        setResourceIds(new ArrayList<>());
        setStatus(ResourceManagerTaskInfoResponseStatus.UNKNOWN);
        storedResourceIds = new ArrayList<>();
        resourceUrls = new HashMap<>();
    }

    public TaskInfo (ResourceManagerTaskInfoResponse resourceManagerTaskInfoResponse) {
        super(resourceManagerTaskInfoResponse);
        storedResourceIds = new ArrayList<>();
        resourceUrls = new HashMap<>();
    }

    public TaskInfo (TaskInfo taskInfo) {
        this((ResourceManagerTaskInfoResponse) taskInfo);
        setStoredResourceIds(new ArrayList<>(taskInfo.getStoredResourceIds()));
        setResourceUrls(new HashMap<String, String>(taskInfo.getResourceUrls()) {
        });
    }

    public List<String> getStoredResourceIds() { return storedResourceIds; }
    public void setStoredResourceIds(List<String> list) { this.storedResourceIds = list; }

    public Map<String, String> getResourceUrls() { return resourceUrls; }
    public void setResourceUrls(Map<String, String> resourceUrls) { this.resourceUrls = resourceUrls; }

    public void calculateStoredResourceIds(QueryResponse queryResponse) {
        for (QueryResourceResult result : queryResponse.getResources()) {
            if (!getResourceIds().contains(result.getId())) {
                storedResourceIds.add(result.getId());
            }
        }
    }

    public void addResourceIds(Map<String, String> resourcesMap) {
        for (Map.Entry<String, String> entry : resourcesMap.entrySet()) {
            if (!getResourceIds().contains(entry.getKey()))
                getResourceIds().add((entry.getKey()));
            resourceUrls.put(entry.getKey(), entry.getValue());
        }
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
                && Objects.equals(this.getStatus(), taskInfo.getStatus())
                && Objects.equals(this.getStoredResourceIds(), taskInfo.getStoredResourceIds())
                && Objects.equals(this.getResourceUrls(), taskInfo.getResourceUrls());
    }

    //    Todo: Implement update in storedResourceIds

}
