package eu.h2020.symbiote.enabler.resourcemanager.model;

import eu.h2020.symbiote.enabler.messaging.model.PlatformProxyAcquisitionStartRequest;
import eu.h2020.symbiote.enabler.messaging.model.ResourceManagerTaskInfoResponse;

import java.util.ArrayList;

/**
 * Created by vasgl on 7/20/2017.
 */
public class QueryAndProcessSearchResponseResult {
    private ArrayList<ResourceManagerTaskInfoResponse> resourceManagerTaskInfoResponseList;
    private ArrayList<PlatformProxyAcquisitionStartRequest> platformProxyAcquisitionStartRequestList;

    public QueryAndProcessSearchResponseResult() {
        resourceManagerTaskInfoResponseList = new ArrayList<>();
        platformProxyAcquisitionStartRequestList = new ArrayList<>();
    }

    public ArrayList<ResourceManagerTaskInfoResponse> getResourceManagerTaskInfoResponseList() { return this.resourceManagerTaskInfoResponseList; }
    public ArrayList<PlatformProxyAcquisitionStartRequest> getPlatformProxyAcquisitionStartRequestList() { return this.platformProxyAcquisitionStartRequestList; }

    public void addToResourceManagerTaskInfoResponseList(ResourceManagerTaskInfoResponse item) {
        resourceManagerTaskInfoResponseList.add(item);
    }

    public void addToPlatformProxyAcquisitionStartRequestList(PlatformProxyAcquisitionStartRequest item) {
        platformProxyAcquisitionStartRequestList.add(item);
    }

    public void add(QueryAndProcessSearchResponseResult queryAndProcessSearchResponseResult) {
        this.resourceManagerTaskInfoResponseList.addAll(queryAndProcessSearchResponseResult.getResourceManagerTaskInfoResponseList());
        this.platformProxyAcquisitionStartRequestList.addAll(queryAndProcessSearchResponseResult.getPlatformProxyAcquisitionStartRequestList());
    }
}
