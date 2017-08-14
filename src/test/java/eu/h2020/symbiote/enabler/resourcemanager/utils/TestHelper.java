package eu.h2020.symbiote.enabler.resourcemanager.utils;


import eu.h2020.symbiote.core.internal.CoreQueryRequest;
import eu.h2020.symbiote.enabler.messaging.model.ResourceManagerAcquisitionStartRequest;
import eu.h2020.symbiote.enabler.messaging.model.ResourceManagerTaskInfoRequest;

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

        ResourceManagerTaskInfoRequest request1 = new ResourceManagerTaskInfoRequest();
        CoreQueryRequest coreQueryRequest1 = new CoreQueryRequest.Builder()
                .locationName("Paris")
                .observedProperty(Arrays.asList("temperature", "humidity"))
                .build();

        request1.setTaskId("1");
        request1.setMinNoResources(2);
        request1.setCoreQueryRequest(coreQueryRequest1);
        request1.setQueryInterval("P0-0-0T0:0:0.06");
        request1.setInformPlatformProxy(true);
        request1.setAllowCaching(false);
        request1.setCachingInterval("P0-0-0T0:0:1");
        request1.setEnablerLogicName("enablerLogicName");
        resources.add(request1);

        if (noTasks > 1) {
            ResourceManagerTaskInfoRequest request2 = new ResourceManagerTaskInfoRequest();
            CoreQueryRequest coreQueryRequest2 = new CoreQueryRequest.Builder()
                    .locationName("Athens")
                    .observedProperty(Arrays.asList("air quality"))
                    .build();

            request2.setTaskId("2");
            request2.setMinNoResources(1);
            request2.setCoreQueryRequest(coreQueryRequest2);
            request2.setQueryInterval("P0-0-0T0:0:0.06");
            request2.setInformPlatformProxy(true);
            request2.setAllowCaching(false);
            request2.setCachingInterval("P0-0-0T0:0:1");
            request2.setEnablerLogicName("enablerLogicName2");
            resources.add(request2);
        }

        request.setResources(resources);
        return request;
    }

    public static ResourceManagerAcquisitionStartRequest createBadQueryToResourceManager() {
        ArrayList<ResourceManagerTaskInfoRequest> resources = new ArrayList<>();
        ResourceManagerAcquisitionStartRequest request = new ResourceManagerAcquisitionStartRequest();
        ResourceManagerTaskInfoRequest request1 = new ResourceManagerTaskInfoRequest();
        CoreQueryRequest coreQueryRequest1 = new CoreQueryRequest.Builder()
                .locationName("Zurich")
                .observedProperty(Arrays.asList("temperature", "humidity"))
                .build();

        request1.setTaskId("1");
        request1.setMinNoResources(2);
        request1.setCoreQueryRequest(coreQueryRequest1);
        request1.setQueryInterval("P0-0-0T0:0:0.06");
        request1.setInformPlatformProxy(true);
        request1.setAllowCaching(false);
        request1.setCachingInterval("P0-0-0T0:0:1");
        request1.setEnablerLogicName("enablerLogicName");
        resources.add(request1);
        request.setResources(resources);

        return request;
    }

}
