package eu.h2020.symbiote.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;


public class EnablerLogicTaskInfoResponse extends EnablerLogicTaskInfoRequest{

    @JsonProperty("resourceIds")
    private List<String> resourceIds;

    public EnablerLogicTaskInfoResponse() {
    }

    public EnablerLogicTaskInfoResponse(EnablerLogicTaskInfoRequest enablerLogicTaskInfoRequest) {
        setTaskId(enablerLogicTaskInfoRequest.getTaskId());
        setCount(enablerLogicTaskInfoRequest.getCount());
        setLocation(enablerLogicTaskInfoRequest.getLocation());
        setObservesProperty(enablerLogicTaskInfoRequest.getObservesProperty());
        setInterval(enablerLogicTaskInfoRequest.getInterval());

    }

    public List<String> getResourceIds() {
        return resourceIds;
    }

    public void setResourceIds(List<String> resourceIds) {
        this.resourceIds = resourceIds;
    }

}
