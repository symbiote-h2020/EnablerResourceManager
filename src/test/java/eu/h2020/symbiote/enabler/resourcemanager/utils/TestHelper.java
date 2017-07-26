package eu.h2020.symbiote.enabler.resourcemanager.utils;

import com.fasterxml.jackson.core.JsonProcessingException;

import eu.h2020.symbiote.core.internal.CoreQueryRequest;
import eu.h2020.symbiote.enabler.messaging.model.ResourceManagerAcquisitionStartRequest;
import eu.h2020.symbiote.enabler.messaging.model.ResourceManagerAcquisitionStartResponse;
import eu.h2020.symbiote.enabler.messaging.model.ResourceManagerTaskInfoRequest;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.fail;

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
        request1.setQueryInterval_ms(60);
        request1.setInformPlatformProxy(true);
        request1.setAllowCaching(false);
        request1.setCachingInterval_ms(new Long(1000));
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
            request2.setQueryInterval_ms(60);
            request2.setInformPlatformProxy(true);
            request2.setAllowCaching(false);
            request2.setCachingInterval_ms(new Long(1000));
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
        request1.setQueryInterval_ms(60);
        request1.setInformPlatformProxy(true);
        request1.setAllowCaching(false);
        request1.setCachingInterval_ms(new Long(1000));
        request1.setEnablerLogicName("enablerLogicName");
        resources.add(request1);
        request.setResources(resources);

        return request;
    }

}
