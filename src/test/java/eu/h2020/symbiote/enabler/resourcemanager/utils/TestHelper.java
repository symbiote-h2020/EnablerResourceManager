package eu.h2020.symbiote.enabler.resourcemanager.utils;


import eu.h2020.symbiote.core.internal.CoreQueryRequest;
import eu.h2020.symbiote.enabler.messaging.model.ResourceManagerAcquisitionStartRequest;
import eu.h2020.symbiote.enabler.messaging.model.ResourceManagerTaskInfoRequest;
import eu.h2020.symbiote.enabler.messaging.model.ResourceManagerUpdateRequest;

import java.util.ArrayList;
import java.util.Arrays;


/**
 * Created by vasgl on 7/26/2017.
 */
public final class TestHelper {

    private TestHelper() {
        // empty constructor
    }

    public static ResourceManagerAcquisitionStartRequest createValidQueryToResourceManager(int noTasks) {
        ArrayList<ResourceManagerTaskInfoRequest> resources = new ArrayList<>();
        ResourceManagerAcquisitionStartRequest request = new ResourceManagerAcquisitionStartRequest();

        CoreQueryRequest coreQueryRequest1 = new CoreQueryRequest.Builder()
                .locationName("Paris")
                .observedProperty(Arrays.asList("temperature", "humidity"))
                .build();

        ResourceManagerTaskInfoRequest request1 = new ResourceManagerTaskInfoRequest("1", 2,
                coreQueryRequest1, "P0-0-0T0:0:0.06", false, "P0-0-0T0:0:1",
                true, "enablerLogicName", null);

        resources.add(request1);

        if (noTasks > 1) {
            CoreQueryRequest coreQueryRequest2 = new CoreQueryRequest.Builder()
                    .locationName("Athens")
                    .observedProperty(Arrays.asList("air quality"))
                    .build();

            ResourceManagerTaskInfoRequest request2 = new ResourceManagerTaskInfoRequest("2", 1,
                    coreQueryRequest2, "P0-0-0T0:0:0.06", false, "P0-0-0T0:0:1",
                    true, "enablerLogicName2", null);

            resources.add(request2);
        }

        request.setResources(resources);
        return request;
    }

    public static ResourceManagerUpdateRequest createValidUpdateQueryToResourceManager(int noTasks) {
        ResourceManagerUpdateRequest updateRequest = new ResourceManagerUpdateRequest();
        updateRequest.setResources(createValidQueryToResourceManager(noTasks).getResources());
        return updateRequest;
    }

    public static ResourceManagerAcquisitionStartRequest createBadQueryToResourceManager() {
        ArrayList<ResourceManagerTaskInfoRequest> resources = new ArrayList<>();
        ResourceManagerAcquisitionStartRequest request = new ResourceManagerAcquisitionStartRequest();

        CoreQueryRequest coreQueryRequest1 = new CoreQueryRequest.Builder()
                .locationName("Zurich")
                .observedProperty(Arrays.asList("temperature", "humidity"))
                .build();

        ResourceManagerTaskInfoRequest request1 = new ResourceManagerTaskInfoRequest("1", 2,
                coreQueryRequest1, "P0-0-0T0:0:0.06", false, "P0-0-0T0:0:1",
                true, "enablerLogicName", null);

        resources.add(request1);
        request.setResources(resources);
        return request;
    }

}
