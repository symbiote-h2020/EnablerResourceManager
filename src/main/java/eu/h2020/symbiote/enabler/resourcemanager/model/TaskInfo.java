package eu.h2020.symbiote.enabler.resourcemanager.model;

import eu.h2020.symbiote.core.ci.QueryResourceResult;
import eu.h2020.symbiote.core.ci.QueryResponse;
import eu.h2020.symbiote.core.ci.SparqlQueryRequest;
import eu.h2020.symbiote.core.internal.CoreQueryRequest;
import eu.h2020.symbiote.enabler.messaging.model.PlatformProxyResourceInfo;
import eu.h2020.symbiote.enabler.messaging.model.ResourceManagerTaskInfoRequest;
import eu.h2020.symbiote.enabler.messaging.model.ResourceManagerTaskInfoResponse;
import eu.h2020.symbiote.enabler.messaging.model.ResourceManagerTaskInfoResponseStatus;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by vasgl on 7/17/2017.
 */
public class TaskInfo extends ResourceManagerTaskInfoResponse {

    private List<String> storedResourceIds;
    private Map<String, String> resourceUrls;

    public TaskInfo() {
        setResourceIds(new ArrayList<>());
        setResourceDescriptions(new ArrayList<>());
        storedResourceIds = new ArrayList<>();
        resourceUrls = new HashMap<>();
    }

    public TaskInfo(String taskId, Integer minNoResources, CoreQueryRequest coreQueryRequest,
                    String queryInterval, Boolean allowCaching, String cachingInterval,
                    Boolean informPlatformProxy, String enablerLogicName, SparqlQueryRequest sparqlQuery,
                    List<String> resourceIds, List<QueryResourceResult> resourceDescriptions,
                    ResourceManagerTaskInfoResponseStatus status,
                    List<String> storedResourceIds, Map<String, String>  resourceUrls, String message) {

        super(taskId, minNoResources, coreQueryRequest, queryInterval, allowCaching, cachingInterval,
                informPlatformProxy, enablerLogicName, sparqlQuery, resourceIds, resourceDescriptions,
                status, message);
        setStoredResourceIds(storedResourceIds);
        setResourceUrls(resourceUrls);
    }

    public TaskInfo (ResourceManagerTaskInfoRequest resourceManagerTaskInfoRequest) {
        super(resourceManagerTaskInfoRequest);
        setResourceIds(new ArrayList<>());
        setResourceDescriptions(new ArrayList<>());
        setStatus(ResourceManagerTaskInfoResponseStatus.UNKNOWN);
        setMessage("");
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

    public void addResourceIds(Map<String, String> resourcesMap, List<QueryResourceResult> results) {
        for (Map.Entry<String, String> entry : resourcesMap.entrySet()) {

            if (!getResourceIds().contains(entry.getKey()))
                getResourceIds().add((entry.getKey()));

            resourceUrls.put(entry.getKey(), entry.getValue());
        }

        for (QueryResourceResult result : results)
            if (!getResourceDescriptions().contains(result))
                getResourceDescriptions().add(result);
    }

    public void addResourceIds(List<PlatformProxyResourceInfo> platformProxyResourceInfoList) {
        for (PlatformProxyResourceInfo entry : platformProxyResourceInfoList) {
            if (!getResourceIds().contains(entry.getResourceId()))
                getResourceIds().add((entry.getResourceId()));
            resourceUrls.put(entry.getResourceId(), entry.getAccessURL());
        }
    }

    public void deleteResourceIds(List<String> resourceIds) {
        for (String entry : resourceIds) {
            getResourceIds().remove(entry);
            resourceUrls.remove(entry);
            setResourceDescriptions(getResourceDescriptions().stream()
                    .filter(elem -> !elem.getId().equals(entry)).collect(Collectors.toList()));
        }
    }

    public List<PlatformProxyResourceInfo> createPlatformProxyResourceInfoList() {
        List<PlatformProxyResourceInfo> platformProxyResourceInfoList = new ArrayList<>();

        for (Map.Entry<String, String> entry : getResourceUrls().entrySet()) {
            PlatformProxyResourceInfo platformProxyResourceInfo = new PlatformProxyResourceInfo();
            platformProxyResourceInfo.setResourceId(entry.getKey());
            platformProxyResourceInfo.setAccessURL(entry.getValue());
            platformProxyResourceInfoList.add(platformProxyResourceInfo);
        }
        return platformProxyResourceInfoList;
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

        ResourceManagerTaskInfoResponse response = (ResourceManagerTaskInfoResponse) o;
        TaskInfo taskInfo = (TaskInfo) o;
        // field comparison
        return super.equals(response)
                && Objects.equals(this.getStoredResourceIds(), taskInfo.getStoredResourceIds())
                && Objects.equals(this.getResourceUrls(), taskInfo.getResourceUrls());
    }

    //    Todo: Implement update in storedResourceIds

}
