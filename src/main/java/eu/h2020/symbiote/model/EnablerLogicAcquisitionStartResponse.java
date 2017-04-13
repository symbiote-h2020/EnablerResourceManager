package eu.h2020.symbiote.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;


public class EnablerLogicAcquisitionStartResponse {

    @JsonProperty("resources")
    private List<EnablerLogicTaskInfoResponse> resources;


    public EnablerLogicAcquisitionStartResponse() {
    }

    public List<EnablerLogicTaskInfoResponse> getResources() {
        return resources;
    }

    public void setResources(List<EnablerLogicTaskInfoResponse> resources) {
        this.resources = resources;
    }

}
