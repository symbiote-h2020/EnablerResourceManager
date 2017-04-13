package eu.h2020.symbiote.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;


public class EnablerLogicAcquisitionStartRequest {

    @JsonProperty("resources")
    private List<EnablerLogicTaskInfoRequest> resources;


    public EnablerLogicAcquisitionStartRequest() {
    }

    public List<EnablerLogicTaskInfoRequest> getResources() {
        return resources;
    }

    public void setResources(List<EnablerLogicTaskInfoRequest> resources) {
        this.resources = resources;
    }

}
