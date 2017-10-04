package eu.h2020.symbiote.enabler.resourcemanager.integration;


import com.fasterxml.jackson.databind.ObjectMapper;

import eu.h2020.symbiote.core.ci.QueryResponse;
import eu.h2020.symbiote.core.internal.CoreQueryRequest;
import eu.h2020.symbiote.enabler.messaging.model.ResourceManagerAcquisitionStartRequest;
import eu.h2020.symbiote.enabler.messaging.model.ResourceManagerTaskInfoRequest;
import eu.h2020.symbiote.enabler.messaging.model.ResourceManagerUpdateRequest;
import eu.h2020.symbiote.enabler.resourcemanager.dummyListeners.DummyEnablerLogicListener;
import eu.h2020.symbiote.enabler.resourcemanager.dummyListeners.DummyPlatformProxyListener;
import eu.h2020.symbiote.enabler.resourcemanager.repository.TaskInfoRepository;
import eu.h2020.symbiote.enabler.resourcemanager.utils.AuthorizationManager;
import eu.h2020.symbiote.enabler.resourcemanager.utils.ProblematicResourcesHandler;
import eu.h2020.symbiote.enabler.resourcemanager.utils.SearchHelper;

import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;

import org.springframework.amqp.rabbit.AsyncRabbitTemplate;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ContextConfiguration
@Configuration
@ComponentScan
@ActiveProfiles("test")
public abstract class AbstractTestClass {

    @Autowired
    protected AsyncRabbitTemplate asyncRabbitTemplate;

    @Autowired
    protected TaskInfoRepository taskInfoRepository;

    @Autowired
    protected DummyPlatformProxyListener dummyPlatformProxyListener;

    @Autowired
    protected DummyEnablerLogicListener dummyEnablerLogicListener;

    @Autowired
    protected AuthorizationManager authorizationManager;

    @Autowired
    protected SearchHelper searchHelper;

    @Autowired
    protected ProblematicResourcesHandler problematicResourcesHandler;

    @Autowired
    protected RestTemplate restTemplate;

    @Autowired
    protected RabbitTemplate rabbitTemplate;

    @Autowired
    @Qualifier("symbIoTeCoreUrl")
    protected String symbIoTeCoreUrl;

    @Value("${rabbit.exchange.resourceManager.name}")
    protected String resourceManagerExchangeName;

    @Value("${rabbit.routingKey.resourceManager.cancelTask}")
    protected String cancelTaskRoutingKey;

    @Value("${rabbit.routingKey.resourceManager.startDataAcquisition}")
    protected String startDataAcquisitionRoutingKey;

    @Value("${rabbit.routingKey.resourceManager.updateTask}")
    protected String updateTaskRoutingKey;

    @Value("${rabbit.routingKey.resourceManager.wrongData}")
    protected String wrongDataRoutingKey;

    @Value("${rabbit.routingKey.resourceManager.unavailableResources}")
    protected String unavailableResourcesRoutingKey;

    protected ObjectMapper mapper = new ObjectMapper();

    // Execute the Setup method before the test.
    @Before
    public void setUp() throws Exception {

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

    @After
    public void clearSetup() throws Exception {
        taskInfoRepository.deleteAll();
    }

    ResourceManagerAcquisitionStartRequest createValidQueryToResourceManager(int noTasks) {
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

    ResourceManagerUpdateRequest createValidUpdateQueryToResourceManager(int noTasks) {
        ResourceManagerUpdateRequest updateRequest = new ResourceManagerUpdateRequest();
        updateRequest.setTasks(createValidQueryToResourceManager(noTasks).getTasks());
        return updateRequest;
    }

    ResourceManagerAcquisitionStartRequest createBadQueryToResourceManager() {
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