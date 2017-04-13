package eu.h2020.symbiote.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;


public class EnablerLogicTaskInfoResponse extends EnablerLogicTaskInfoRequest{

    @JsonProperty("resourceIds")
    private List<String> resourceIds;

    public EnablerLogicTaskInfoResponse() {
    }

    public List<String> getObservesProperty() {
        return resourceIds;
    }

    public void setObservesProperty(List<String> resourceIds) {
        this.resourceIds = resourceIds;
    }

}
