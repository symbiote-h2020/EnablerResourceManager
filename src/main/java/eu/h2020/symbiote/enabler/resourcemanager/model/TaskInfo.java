package eu.h2020.symbiote.enabler.resourcemanager.model;

import eu.h2020.symbiote.core.ci.QueryResourceResult;
import eu.h2020.symbiote.core.ci.QueryResponse;
import eu.h2020.symbiote.enabler.messaging.model.ResourceManagerTaskInfoResponse;
import org.springframework.data.annotation.Id;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by vasgl on 7/17/2017.
 */
public class TaskInfo extends ResourceManagerTaskInfoResponse {

    private List<String> storedResourceIds;

    public TaskInfo() {
        storedResourceIds = new ArrayList<>();
    }

    public TaskInfo (ResourceManagerTaskInfoResponse resourceManagerTaskInfoResponse) {
        super(resourceManagerTaskInfoResponse);
        setResourceIds(resourceManagerTaskInfoResponse.getResourceIds());
        storedResourceIds = new ArrayList<>();
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

//    Todo: Implement update in storedResourceIds
}
