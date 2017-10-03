package eu.h2020.symbiote.enabler.resourcemanager.utils;


import eu.h2020.symbiote.core.ci.QueryResponse;
import eu.h2020.symbiote.core.internal.CoreQueryRequest;
import eu.h2020.symbiote.enabler.messaging.model.ResourceManagerAcquisitionStartRequest;
import eu.h2020.symbiote.enabler.messaging.model.ResourceManagerTaskInfoRequest;
import eu.h2020.symbiote.enabler.messaging.model.ResourceManagerUpdateRequest;
import eu.h2020.symbiote.enabler.resourcemanager.dummyListeners.DummyEnablerLogicListener;
import eu.h2020.symbiote.enabler.resourcemanager.dummyListeners.DummyPlatformProxyListener;
import eu.h2020.symbiote.enabler.resourcemanager.integration.RestTemplateAnswer;
import eu.h2020.symbiote.enabler.resourcemanager.repository.TaskInfoRepository;
import eu.h2020.symbiote.enabler.resourcemanager.utils.AuthorizationManager;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;


public class TestHelper {

    public static void setUp(DummyPlatformProxyListener dummyPlatformProxyListener,
                      DummyEnablerLogicListener dummyEnablerLogicListener,
                      AuthorizationManager authorizationManager,
                      String symbIoTeCoreUrl, SearchHelper searchHelper,
                      RestTemplate restTemplate) throws Exception {

        searchHelper.restartTimer();
        dummyPlatformProxyListener.clearRequestsReceivedByListener();
        dummyEnablerLogicListener.clearRequestsReceivedByListener();

        doReturn(new HashMap<>()).when(authorizationManager).requestHomeToken(any());

        doAnswer(new RestTemplateAnswer(symbIoTeCoreUrl)).when(restTemplate)
                .exchange(any(String.class), any(HttpMethod.class), any(HttpEntity.class), any(Class.class));
        doAnswer(new RestTemplateAnswer(symbIoTeCoreUrl)).when(restTemplate)
                .exchange(any(String.class), any(HttpMethod.class), any(HttpEntity.class), any(ParameterizedTypeReference.class));
        doThrow(new HttpServerErrorException(HttpStatus.INTERNAL_SERVER_ERROR)).when(restTemplate)
                .exchange(eq(symbIoTeCoreUrl + "/resourceUrls?id=badCRAMrespose"), any(HttpMethod.class),
                        any(HttpEntity.class), any(ParameterizedTypeReference.class));
        doThrow(new HttpServerErrorException(HttpStatus.BAD_REQUEST)).when(restTemplate)
                .exchange(eq(symbIoTeCoreUrl + "/query?location_name=Zurich&observed_property=temperature,humidity&should_rank=true"),
                        any(HttpMethod.class), any(HttpEntity.class), eq(QueryResponse.class));

    }

    public static void clearSetup(TaskInfoRepository taskInfoRepository) throws Exception {
        taskInfoRepository.deleteAll();
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

        request.setTasks(resources);
        return request;
    }

    public static ResourceManagerUpdateRequest createValidUpdateQueryToResourceManager(int noTasks) {
        ResourceManagerUpdateRequest updateRequest = new ResourceManagerUpdateRequest();
        updateRequest.setTasks(createValidQueryToResourceManager(noTasks).getTasks());
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
        request.setTasks(resources);
        return request;
    }

}