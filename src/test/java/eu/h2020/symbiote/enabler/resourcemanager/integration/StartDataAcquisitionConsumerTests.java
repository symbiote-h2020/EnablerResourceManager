package eu.h2020.symbiote.enabler.resourcemanager.integration;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.h2020.symbiote.core.ci.SparqlQueryOutputFormat;
import eu.h2020.symbiote.core.ci.SparqlQueryRequest;
import eu.h2020.symbiote.core.internal.CoreQueryRequest;
import eu.h2020.symbiote.enabler.messaging.model.*;
import eu.h2020.symbiote.enabler.resourcemanager.dummyListeners.DummyEnablerLogicListener;
import eu.h2020.symbiote.enabler.resourcemanager.dummyListeners.DummyPlatformProxyListener;
import eu.h2020.symbiote.enabler.resourcemanager.model.TaskInfo;
import eu.h2020.symbiote.enabler.resourcemanager.repository.TaskInfoRepository;
import eu.h2020.symbiote.enabler.resourcemanager.utils.AuthorizationManager;
import eu.h2020.symbiote.enabler.resourcemanager.utils.ListenableFutureAcquisitionStartCallback;
import eu.h2020.symbiote.enabler.resourcemanager.utils.TestHelper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.amqp.rabbit.AsyncRabbitTemplate;
import org.springframework.amqp.rabbit.AsyncRabbitTemplate.RabbitConverterFuture;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;


@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(webEnvironment = WebEnvironment.DEFINED_PORT,
        properties = {"eureka.client.enabled=false",
                "spring.sleuth.enabled=false",
                "symbiote.core.url=http://localhost:8080",
                "symbiote.coreaam.url=http://localhost:8080"}
)
@ContextConfiguration
@Configuration
@ComponentScan
@EnableAutoConfiguration
@ActiveProfiles("test")
public class StartDataAcquisitionConsumerTests {

    private static Log log = LogFactory
            .getLog(StartDataAcquisitionConsumerTests.class);

    @Autowired
    private AsyncRabbitTemplate asyncRabbitTemplate;

    @Autowired
    private TaskInfoRepository taskInfoRepository;

    @Autowired
    private DummyPlatformProxyListener dummyPlatformProxyListener;

    @Autowired
    private DummyEnablerLogicListener dummyEnablerLogicListener;

    @Autowired
    private AuthorizationManager authorizationManager;

    @Autowired
    @Qualifier("symbIoTeCoreUrl")
    private String symbIoTeCoreUrl;

    @Value("${rabbit.exchange.resourceManager.name}")
    private String resourceManagerExchangeName;

    @Value("${rabbit.routingKey.resourceManager.startDataAcquisition}")
    private String startDataAcquisitionRoutingKey;

    private ObjectMapper mapper = new ObjectMapper();

    // Execute the Setup method before the test.
    @Before
    public void setUp() throws Exception {
        dummyPlatformProxyListener.clearRequestsReceivedByListener();
        dummyEnablerLogicListener.clearRequestsReceivedByListener();

        doReturn(new HashMap<>()).when(authorizationManager).requestHomeToken(any());

    }

    @After
    public void clearSetup() throws Exception {
        taskInfoRepository.deleteAll();
    }

    @Test
    public void resourceManagerGetResourceDetailsTest() throws Exception {
        log.info("resourceManagerGetResourceDetailsTest STARTED!");

        // ToDo: add default field value in TaskInfo

        final AtomicReference<ResourceManagerAcquisitionStartResponse> resultRef = new AtomicReference<>();
        ResourceManagerAcquisitionStartRequest query = TestHelper.createValidQueryToResourceManager(2);
        List<PlatformProxyAcquisitionStartRequest> startAcquisitionRequestsReceivedByListener;

        log.info("Before sending the message");
        RabbitConverterFuture<ResourceManagerAcquisitionStartResponse> future = asyncRabbitTemplate
                .convertSendAndReceive(resourceManagerExchangeName, startDataAcquisitionRoutingKey, query);
        log.info("After sending the message");

        future.addCallback(new ListenableFutureAcquisitionStartCallback("resourceManagerGetResourceDetailsTest", resultRef));

        while(!future.isDone()) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(100);

        String responseInString = mapper.writeValueAsString(resultRef.get().getTasks());
        log.info("Response String: " + responseInString);

        // Test what Enabler Logic receives
        assertEquals(ResourceManagerTasksStatus.SUCCESS, resultRef.get().getStatus());
        assertEquals("ALL the task requests were successful!", resultRef.get().getMessage());
        assertEquals(2, resultRef.get().getTasks().get(0).getResourceIds().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, resultRef.get().getTasks().get(0).getStatus());
        assertEquals(1, resultRef.get().getTasks().get(1).getResourceIds().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, resultRef.get().getTasks().get(1).getStatus());

        assertEquals("resource1", resultRef.get().getTasks().get(0).getResourceIds().get(0));
        assertEquals("resource2", resultRef.get().getTasks().get(0).getResourceIds().get(1));
        assertEquals("resource4", resultRef.get().getTasks().get(1).getResourceIds().get(0));

        while(dummyPlatformProxyListener.startAcquisitionRequestsReceived() < 2) {
            log.info("startAcquisitionRequestsReceivedByListener.size(): " + dummyPlatformProxyListener.startAcquisitionRequestsReceived());
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(100);

        // Test what Platform Proxy receives
        startAcquisitionRequestsReceivedByListener = dummyPlatformProxyListener.getStartAcquisitionRequestsReceivedByListener();

        assertEquals(2, dummyPlatformProxyListener.startAcquisitionRequestsReceived());
        assertEquals(0, dummyPlatformProxyListener.updateAcquisitionRequestsReceived());

        if (startAcquisitionRequestsReceivedByListener.get(0).getTaskId().equals("1")) {
            assertEquals("resource1", startAcquisitionRequestsReceivedByListener.get(0).getResources().get(0).getResourceId());
            assertEquals("resource2", startAcquisitionRequestsReceivedByListener.get(0).getResources().get(1).getResourceId());
            assertEquals("resource4", startAcquisitionRequestsReceivedByListener.get(1).getResources().get(0).getResourceId());
            assertEquals("enablerLogicName", startAcquisitionRequestsReceivedByListener.get(0).getEnablerLogicName());
            assertEquals("enablerLogicName2", startAcquisitionRequestsReceivedByListener.get(1).getEnablerLogicName());

        } else {
            assertEquals("resource1", startAcquisitionRequestsReceivedByListener.get(1).getResources().get(0).getResourceId());
            assertEquals("resource2", startAcquisitionRequestsReceivedByListener.get(1).getResources().get(1).getResourceId());
            assertEquals("resource4", startAcquisitionRequestsReceivedByListener.get(0).getResources().get(0).getResourceId());
            assertEquals("enablerLogicName", startAcquisitionRequestsReceivedByListener.get(1).getEnablerLogicName());
            assertEquals("enablerLogicName2", startAcquisitionRequestsReceivedByListener.get(0).getEnablerLogicName());
        }

        // Test what is stored in the database
        TaskInfo taskInfo = taskInfoRepository.findByTaskId("1");
        assertEquals(2, taskInfo.getResourceIds().size());
        assertEquals(0, taskInfo.getStoredResourceIds().size()); // allowCaching == false
        assertEquals(2, taskInfo.getResourceUrls().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, taskInfo.getStatus());
        assertEquals("resource1", taskInfo.getResourceIds().get(0));
        assertEquals("resource2", taskInfo.getResourceIds().get(1));
        assertEquals(symbIoTeCoreUrl + "/Sensors('resource1')", taskInfo.getResourceUrls().get("resource1"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('resource2')", taskInfo.getResourceUrls().get("resource2"));

        taskInfo = taskInfoRepository.findByTaskId("2");
        assertEquals(1, taskInfo.getResourceIds().size());
        assertEquals(0, taskInfo.getStoredResourceIds().size()); // allowCaching == false
        assertEquals(1, taskInfo.getResourceUrls().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, taskInfo.getStatus());
        assertEquals("resource4", taskInfo.getResourceIds().get(0));
        assertEquals(symbIoTeCoreUrl + "/Sensors('resource4')", taskInfo.getResourceUrls().get("resource4"));

        log.info("resourceManagerGetResourceDetailsTest FINISHED!");
    }

    @Test
    public void resourceManagerGetSparqlResourceDetailsTest() throws Exception {
        log.info("resourceManagerGetSparqlResourceDetailsTest STARTED!");

        // ToDo: add default field value in TaskInfo

        final AtomicReference<ResourceManagerAcquisitionStartResponse> resultRef = new AtomicReference<>();
        List<PlatformProxyAcquisitionStartRequest> startAcquisitionRequestsReceivedByListener;

        ResourceManagerAcquisitionStartRequest query = TestHelper.createValidQueryToResourceManager(2);
        SparqlQueryRequest sparqlQueryRequest1 = new SparqlQueryRequest("Paris",
                SparqlQueryOutputFormat.COUNT);
        SparqlQueryRequest sparqlQueryRequest2 = new SparqlQueryRequest("Athens",
                SparqlQueryOutputFormat.COUNT);
        query.getTasks().get(0).setSparqlQueryRequest(sparqlQueryRequest1);
        query.getTasks().get(1).setSparqlQueryRequest(sparqlQueryRequest2);

        log.info("Before sending the message");
        RabbitConverterFuture<ResourceManagerAcquisitionStartResponse> future = asyncRabbitTemplate
                .convertSendAndReceive(resourceManagerExchangeName, startDataAcquisitionRoutingKey, query);
        log.info("After sending the message");

        future.addCallback(new ListenableFutureAcquisitionStartCallback("resourceManagerGetSparqlResourceDetailsTest", resultRef));

        while(!future.isDone()) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(100);

        String responseInString = mapper.writeValueAsString(resultRef.get().getTasks());
        log.info("Response String: " + responseInString);

        // Test what Enabler Logic receives
        assertEquals(ResourceManagerTasksStatus.SUCCESS, resultRef.get().getStatus());
        assertEquals("ALL the task requests were successful!", resultRef.get().getMessage());
        assertEquals(2, resultRef.get().getTasks().get(0).getResourceIds().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, resultRef.get().getTasks().get(0).getStatus());
        assertEquals(1, resultRef.get().getTasks().get(1).getResourceIds().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, resultRef.get().getTasks().get(1).getStatus());

        assertEquals("sparqlResource1", resultRef.get().getTasks().get(0).getResourceIds().get(0));
        assertEquals("sparqlResource2", resultRef.get().getTasks().get(0).getResourceIds().get(1));
        assertEquals("sparqlResource4", resultRef.get().getTasks().get(1).getResourceIds().get(0));

        while(dummyPlatformProxyListener.startAcquisitionRequestsReceived() < 2) {
            log.info("startAcquisitionRequestsReceivedByListener.size(): " + dummyPlatformProxyListener.startAcquisitionRequestsReceived());
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(100);

        // Test what Platform Proxy receives
        startAcquisitionRequestsReceivedByListener = dummyPlatformProxyListener.getStartAcquisitionRequestsReceivedByListener();

        assertEquals(2, dummyPlatformProxyListener.startAcquisitionRequestsReceived());
        assertEquals(0, dummyPlatformProxyListener.updateAcquisitionRequestsReceived());

        if (startAcquisitionRequestsReceivedByListener.get(0).getTaskId().equals("1")) {
            assertEquals("sparqlResource1", startAcquisitionRequestsReceivedByListener.get(0).getResources().get(0).getResourceId());
            assertEquals("sparqlResource2", startAcquisitionRequestsReceivedByListener.get(0).getResources().get(1).getResourceId());
            assertEquals("sparqlResource4", startAcquisitionRequestsReceivedByListener.get(1).getResources().get(0).getResourceId());
            assertEquals("enablerLogicName", startAcquisitionRequestsReceivedByListener.get(0).getEnablerLogicName());
            assertEquals("enablerLogicName2", startAcquisitionRequestsReceivedByListener.get(1).getEnablerLogicName());

        } else {
            assertEquals("sparqlResource1", startAcquisitionRequestsReceivedByListener.get(1).getResources().get(0).getResourceId());
            assertEquals("sparqlResource2", startAcquisitionRequestsReceivedByListener.get(1).getResources().get(1).getResourceId());
            assertEquals("sparqlResource4", startAcquisitionRequestsReceivedByListener.get(0).getResources().get(0).getResourceId());
            assertEquals("enablerLogicName", startAcquisitionRequestsReceivedByListener.get(1).getEnablerLogicName());
            assertEquals("enablerLogicName2", startAcquisitionRequestsReceivedByListener.get(0).getEnablerLogicName());
        }

        // Test what is stored in the database
        TaskInfo taskInfo = taskInfoRepository.findByTaskId("1");
        assertEquals(2, taskInfo.getResourceIds().size());
        assertEquals(0, taskInfo.getStoredResourceIds().size()); // allowCaching == false
        assertEquals(2, taskInfo.getResourceUrls().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, taskInfo.getStatus());
        assertEquals("sparqlResource1", taskInfo.getResourceIds().get(0));
        assertEquals("sparqlResource2", taskInfo.getResourceIds().get(1));
        assertEquals(symbIoTeCoreUrl + "/Sensors('sparqlResource1')", taskInfo.getResourceUrls().get("sparqlResource1"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('sparqlResource2')", taskInfo.getResourceUrls().get("sparqlResource2"));

        taskInfo = taskInfoRepository.findByTaskId("2");
        assertEquals(1, taskInfo.getResourceIds().size());
        assertEquals(0, taskInfo.getStoredResourceIds().size()); // allowCaching == false
        assertEquals(1, taskInfo.getResourceUrls().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, taskInfo.getStatus());
        assertEquals("sparqlResource4", taskInfo.getResourceIds().get(0));
        assertEquals(symbIoTeCoreUrl + "/Sensors('sparqlResource4')", taskInfo.getResourceUrls().get("sparqlResource4"));

        log.info("resourceManagerGetSparqlResourceDetailsTest FINISHED!");
    }

    // @Test
    public void resourceManagerGetResourceDetailsNoResponseTest() throws Exception {
        log.info("resourceManagerGetResourceDetailsNoResponseTest STARTED!");

        final AtomicReference<ResourceManagerAcquisitionStartResponse> resultRef = new AtomicReference<>();
        ResourceManagerAcquisitionStartRequest query = new ResourceManagerAcquisitionStartRequest();
        ArrayList<ResourceManagerTaskInfoRequest> resources = new ArrayList<>();


        ResourceManagerTaskInfoRequest request1 = new ResourceManagerTaskInfoRequest();
        CoreQueryRequest coreQueryRequest1 = new CoreQueryRequest.Builder()
                .locationName("Paris")
                .observedProperty(Arrays.asList("temperature", "humidity"))
                .build();

        request1.setTaskId("1");
        request1.setMinNoResources(2);
        request1.setCoreQueryRequest(coreQueryRequest1);
        request1.setQueryInterval("P0-0-0T0:0:0.06");
        resources.add(request1);

        query.setTasks(resources);

        log.info("Before sending the message");
        RabbitConverterFuture<ResourceManagerAcquisitionStartResponse> future = asyncRabbitTemplate
                .convertSendAndReceive(resourceManagerExchangeName, startDataAcquisitionRoutingKey, query);
        log.info("After sending the message");

        future.addCallback(new ListenableFutureAcquisitionStartCallback("resourceManagerGetResourceDetailsNoResponseTest", resultRef));

        while(!future.isDone()) {
            log.info("Sleeping!!!!!!");
            TimeUnit.MILLISECONDS.sleep(100);
        }

        log.info("resourceManagerGetResourceDetailsNoResponseTest FINISHED!");
    }

    @Test
    public void resourceManagerGetResourceDetailsBadRequestTest() throws Exception {
        log.info("resourceManagerGetResourceDetailsBadRequestTest STARTED!");

        final AtomicReference<ResourceManagerAcquisitionStartResponse> resultRef = new AtomicReference<>();
        ResourceManagerAcquisitionStartRequest query = TestHelper.createBadQueryToResourceManager();

        log.info("Before sending the message");
        RabbitConverterFuture<ResourceManagerAcquisitionStartResponse> future = asyncRabbitTemplate
                .convertSendAndReceive(resourceManagerExchangeName, startDataAcquisitionRoutingKey, query);
        log.info("After sending the message");

        future.addCallback(new ListenableFutureAcquisitionStartCallback("resourceManagerGetResourceDetailsBadRequestTest", resultRef));

        while(!future.isDone()) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(100);

        // Test what Enabler Logic receives
        assertEquals(ResourceManagerTasksStatus.FAILED, resultRef.get().getStatus());
        assertEquals("NONE of the task requests were successful", resultRef.get().getMessage());
        assertEquals(0, resultRef.get().getTasks().get(0).getResourceIds().size());

        // Test what Platform Proxy receives
        TimeUnit.MILLISECONDS.sleep(500);
        assertEquals(0, dummyPlatformProxyListener.startAcquisitionRequestsReceived());

        // Test what is stored in the database
        TaskInfo taskInfo = taskInfoRepository.findByTaskId("1");
        assertEquals(null, taskInfo);

        log.info("resourceManagerGetResourceDetailsBadRequestTest FINISHED!");
    }

    @Test
    public void notSendingToPlatformProxyTest() throws Exception {
        log.info("notSendingToPlatformProxyTest STARTED!");

        final AtomicReference<ResourceManagerAcquisitionStartResponse> resultRef = new AtomicReference<>();
        ResourceManagerAcquisitionStartRequest query = TestHelper.createValidQueryToResourceManager(2);
        List<PlatformProxyAcquisitionStartRequest> startAcquisitionRequestsReceivedByListener;

        // Forward to PlatformProxy only the 2nd task
        query.getTasks().get(0).setInformPlatformProxy(false);

        log.info("Before sending the message");
        RabbitConverterFuture<ResourceManagerAcquisitionStartResponse> future = asyncRabbitTemplate
                .convertSendAndReceive(resourceManagerExchangeName, startDataAcquisitionRoutingKey, query);
        log.info("After sending the message");

        future.addCallback(new ListenableFutureAcquisitionStartCallback("notSendingToPlatformProxyTest", resultRef));

        while(!future.isDone()) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(100);

        // Test what Enabler Logic receives
        assertEquals(ResourceManagerTasksStatus.SUCCESS, resultRef.get().getStatus());
        assertEquals("ALL the task requests were successful!", resultRef.get().getMessage());
        assertEquals(2, resultRef.get().getTasks().get(0).getResourceIds().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, resultRef.get().getTasks().get(0).getStatus());
        assertEquals(1, resultRef.get().getTasks().get(1).getResourceIds().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, resultRef.get().getTasks().get(1).getStatus());

        assertEquals("resource1", resultRef.get().getTasks().get(0).getResourceIds().get(0));
        assertEquals("resource2", resultRef.get().getTasks().get(0).getResourceIds().get(1));
        assertEquals("resource4", resultRef.get().getTasks().get(1).getResourceIds().get(0));

        while(dummyPlatformProxyListener.startAcquisitionRequestsReceived() < 1) {
            TimeUnit.MILLISECONDS.sleep(100);
        }

        // Test what Platform Proxy receives
        startAcquisitionRequestsReceivedByListener = dummyPlatformProxyListener.getStartAcquisitionRequestsReceivedByListener();
        assertEquals(1, dummyPlatformProxyListener.startAcquisitionRequestsReceived());
        assertEquals(0, dummyPlatformProxyListener.updateAcquisitionRequestsReceived());
        assertEquals("resource4", startAcquisitionRequestsReceivedByListener.get(0).getResources().get(0).getResourceId());
        assertEquals("enablerLogicName2", startAcquisitionRequestsReceivedByListener.get(0).getEnablerLogicName());

        // Test what is stored in the database
        TaskInfo taskInfo = taskInfoRepository.findByTaskId("1");
        assertEquals(2, taskInfo.getResourceIds().size());
        assertEquals(0, taskInfo.getStoredResourceIds().size());
        assertEquals(2, taskInfo.getResourceUrls().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, taskInfo.getStatus());
        assertEquals("resource1", taskInfo.getResourceIds().get(0));
        assertEquals("resource2", taskInfo.getResourceIds().get(1));
        assertEquals(symbIoTeCoreUrl + "/Sensors('resource1')", taskInfo.getResourceUrls().get("resource1"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('resource2')", taskInfo.getResourceUrls().get("resource2"));

        taskInfo = taskInfoRepository.findByTaskId("2");
        assertEquals(1, taskInfo.getResourceIds().size());
        assertEquals(0, taskInfo.getStoredResourceIds().size());
        assertEquals(1, taskInfo.getResourceUrls().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, taskInfo.getStatus());
        assertEquals("resource4", taskInfo.getResourceIds().get(0));
        assertEquals(symbIoTeCoreUrl + "/Sensors('resource4')", taskInfo.getResourceUrls().get("resource4"));

        log.info("notSendingToPlatformProxyTest FINISHED!");
    }

    @Test
    public void allowCachingTest() throws Exception {
        log.info("allowCachingTest STARTED!");

        final AtomicReference<ResourceManagerAcquisitionStartResponse> resultRef = new AtomicReference<>();
        ResourceManagerAcquisitionStartRequest query = TestHelper.createValidQueryToResourceManager(2);

        // Cache only the 2nd task
        query.getTasks().get(0).setAllowCaching(true);

        log.info("Before sending the message");
        RabbitConverterFuture<ResourceManagerAcquisitionStartResponse> future = asyncRabbitTemplate
                .convertSendAndReceive(resourceManagerExchangeName, startDataAcquisitionRoutingKey, query);
        log.info("After sending the message");

        future.addCallback(new ListenableFutureAcquisitionStartCallback("allowCachingTest", resultRef));

        while(!future.isDone()) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(100);

        // Test what Enabler Logic receives
        assertEquals(ResourceManagerTasksStatus.SUCCESS, resultRef.get().getStatus());
        assertEquals("ALL the task requests were successful!", resultRef.get().getMessage());
        assertEquals(2, resultRef.get().getTasks().get(0).getResourceIds().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, resultRef.get().getTasks().get(0).getStatus());
        assertEquals(1, resultRef.get().getTasks().get(1).getResourceIds().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, resultRef.get().getTasks().get(1).getStatus());

        assertEquals("resource1", resultRef.get().getTasks().get(0).getResourceIds().get(0));
        assertEquals("resource2", resultRef.get().getTasks().get(0).getResourceIds().get(1));
        assertEquals("resource4", resultRef.get().getTasks().get(1).getResourceIds().get(0));

        while(dummyPlatformProxyListener.startAcquisitionRequestsReceived() < 2) {
            TimeUnit.MILLISECONDS.sleep(100);
        }

        // Test what is stored in the database
        TaskInfo taskInfo = taskInfoRepository.findByTaskId("1");
        assertEquals(2, taskInfo.getResourceIds().size());
        assertEquals(1, taskInfo.getStoredResourceIds().size());
        assertEquals(2, taskInfo.getResourceUrls().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, taskInfo.getStatus());
        assertEquals("resource1", taskInfo.getResourceIds().get(0));
        assertEquals("resource2", taskInfo.getResourceIds().get(1));
        assertEquals("resource3", taskInfo.getStoredResourceIds().get(0));
        assertEquals(symbIoTeCoreUrl + "/Sensors('resource1')", taskInfo.getResourceUrls().get("resource1"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('resource2')", taskInfo.getResourceUrls().get("resource2"));

        taskInfo = taskInfoRepository.findByTaskId("2");
        assertEquals(1, taskInfo.getResourceIds().size());
        assertEquals(0, taskInfo.getStoredResourceIds().size());
        assertEquals(1, taskInfo.getResourceUrls().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, taskInfo.getStatus());
        assertEquals("resource4", taskInfo.getResourceIds().get(0));
        assertEquals(symbIoTeCoreUrl + "/Sensors('resource4')", taskInfo.getResourceUrls().get("resource4"));

        log.info("allowCachingTest FINISHED!");
    }

    @Test
    public void notEnoughResourcesTest() throws Exception {
        log.info("notEnoughResourcesTest STARTED!");

        final AtomicReference<ResourceManagerAcquisitionStartResponse> resultRef = new AtomicReference<>();
        List<PlatformProxyAcquisitionStartRequest> startAcquisitionRequestsReceivedByListener;

        ResourceManagerAcquisitionStartRequest query = TestHelper.createValidQueryToResourceManager(2);
        query.getTasks().get(0).setAllowCaching(true);
        query.getTasks().get(1).setMinNoResources(3);
        query.getTasks().get(1).setAllowCaching(true);

        log.info("Before sending the message");
        RabbitConverterFuture<ResourceManagerAcquisitionStartResponse> future = asyncRabbitTemplate
                .convertSendAndReceive(resourceManagerExchangeName, startDataAcquisitionRoutingKey, query);
        log.info("After sending the message");

        future.addCallback(new ListenableFutureAcquisitionStartCallback("allowCachingTest", resultRef));

        while(!future.isDone()) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(100);

        // Test what Enabler Logic receives
        assertEquals(ResourceManagerTasksStatus.PARTIAL_SUCCESS, resultRef.get().getStatus());
        assertEquals("Failed tasks id : [2]", resultRef.get().getMessage());
        assertEquals(2, resultRef.get().getTasks().get(0).getResourceIds().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, resultRef.get().getTasks().get(0).getStatus());
        assertEquals(2, resultRef.get().getTasks().get(1).getResourceIds().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.NOT_ENOUGH_RESOURCES, resultRef.get().getTasks().get(1).getStatus());

        assertEquals("resource1", resultRef.get().getTasks().get(0).getResourceIds().get(0));
        assertEquals("resource2", resultRef.get().getTasks().get(0).getResourceIds().get(1));
        assertEquals("resource4", resultRef.get().getTasks().get(1).getResourceIds().get(0));
        assertEquals("resource5", resultRef.get().getTasks().get(1).getResourceIds().get(1));

        while(dummyPlatformProxyListener.startAcquisitionRequestsReceived() < 1) {
            TimeUnit.MILLISECONDS.sleep(100);
        }

        // Test what Platform Proxy receives
        startAcquisitionRequestsReceivedByListener = dummyPlatformProxyListener.getStartAcquisitionRequestsReceivedByListener();
        assertEquals(1, dummyPlatformProxyListener.startAcquisitionRequestsReceived());
        assertEquals(0, dummyPlatformProxyListener.updateAcquisitionRequestsReceived());
        assertEquals(2, startAcquisitionRequestsReceivedByListener.get(0).getResources().size());
        assertEquals("resource1", startAcquisitionRequestsReceivedByListener.get(0).getResources().get(0).getResourceId());
        assertEquals("resource2", startAcquisitionRequestsReceivedByListener.get(0).getResources().get(1).getResourceId());

        // Test what is stored in the database
        TaskInfo taskInfo = taskInfoRepository.findByTaskId("1");
        assertEquals(2, taskInfo.getResourceIds().size());
        assertEquals(1, taskInfo.getStoredResourceIds().size());
        assertEquals(2, taskInfo.getResourceUrls().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, taskInfo.getStatus());
        assertEquals("resource1", taskInfo.getResourceIds().get(0));
        assertEquals("resource2", taskInfo.getResourceIds().get(1));
        assertEquals("resource3", taskInfo.getStoredResourceIds().get(0));
        assertEquals(symbIoTeCoreUrl + "/Sensors('resource1')", taskInfo.getResourceUrls().get("resource1"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('resource2')", taskInfo.getResourceUrls().get("resource2"));

        taskInfo = taskInfoRepository.findByTaskId("2");
        assertEquals(2, taskInfo.getResourceIds().size());
        assertEquals(0, taskInfo.getStoredResourceIds().size());
        assertEquals(2, taskInfo.getResourceUrls().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.NOT_ENOUGH_RESOURCES, taskInfo.getStatus());
        assertEquals("resource4", taskInfo.getResourceIds().get(0));
        assertEquals("resource5", taskInfo.getResourceIds().get(1));
        assertEquals(symbIoTeCoreUrl + "/Sensors('resource4')", taskInfo.getResourceUrls().get("resource4"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('resource5')", taskInfo.getResourceUrls().get("resource5"));

        log.info("notEnoughResourcesTest FINISHED!");
    }

    @Test
    public void wrongQueryIntervalFormatTest() throws Exception {
        log.info("wrongQueryIntervalFormatTest STARTED!");

        final AtomicReference<ResourceManagerAcquisitionStartResponse> resultRef = new AtomicReference<>();

        ResourceManagerAcquisitionStartRequest query = TestHelper.createValidQueryToResourceManager(2);
        Field queryIntervalField = query.getTasks().get(1).getClass().getDeclaredField("queryInterval");
        queryIntervalField.setAccessible(true);
        queryIntervalField.set(query.getTasks().get(1), "10s");

        log.info("Before sending the message");
        RabbitConverterFuture<ResourceManagerAcquisitionStartResponse> future = asyncRabbitTemplate
                .convertSendAndReceive(resourceManagerExchangeName, startDataAcquisitionRoutingKey, query);
        log.info("After sending the message");

        future.addCallback(new ListenableFutureAcquisitionStartCallback("allowCachingTest", resultRef));

        while(!future.isDone()) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(100);

        // Test what Enabler Logic receives
        assertEquals(ResourceManagerTasksStatus.FAILED_WRONG_FORMAT_INTERVAL, resultRef.get().getStatus());
        assertEquals(true,
                resultRef.get().getMessage().contains(IllegalArgumentException.class.getName() + ": Invalid format:"));

        TimeUnit.MILLISECONDS.sleep(500);

        // Test what Platform Proxy receives
        assertEquals(0, dummyPlatformProxyListener.startAcquisitionRequestsReceived());
        assertEquals(0, dummyPlatformProxyListener.updateAcquisitionRequestsReceived());

        // Test what is stored in the database
        TaskInfo taskInfo = taskInfoRepository.findByTaskId("1");
        assertEquals(null, taskInfo);

        taskInfo = taskInfoRepository.findByTaskId("2");
        assertEquals(null, taskInfo);

        log.info("wrongQueryIntervalFormatTest FINISHED!");
    }

    @Test
    public void wrongCacheIntervalFormatTest() throws Exception {
        log.info("wrongCacheIntervalFormatTest STARTED!");

        final AtomicReference<ResourceManagerAcquisitionStartResponse> resultRef = new AtomicReference<>();

        ResourceManagerAcquisitionStartRequest query = TestHelper.createValidQueryToResourceManager(2);
        Field cachingIntervalField = query.getTasks().get(1).getClass().getDeclaredField("cachingInterval");
        cachingIntervalField.setAccessible(true);
        cachingIntervalField.set(query.getTasks().get(1), "10s");

        log.info("Before sending the message");
        RabbitConverterFuture<ResourceManagerAcquisitionStartResponse> future = asyncRabbitTemplate
                .convertSendAndReceive(resourceManagerExchangeName, startDataAcquisitionRoutingKey, query);
        log.info("After sending the message");

        future.addCallback(new ListenableFutureAcquisitionStartCallback("allowCachingTest", resultRef));

        while(!future.isDone()) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(100);

        // Test what Enabler Logic receives
        assertEquals(ResourceManagerTasksStatus.FAILED_WRONG_FORMAT_INTERVAL, resultRef.get().getStatus());
        assertEquals(true,
                resultRef.get().getMessage().contains(IllegalArgumentException.class.getName() + ": Invalid format:"));

        TimeUnit.MILLISECONDS.sleep(500);

        // Test what Platform Proxy receives
        assertEquals(0, dummyPlatformProxyListener.startAcquisitionRequestsReceived());
        assertEquals(0, dummyPlatformProxyListener.updateAcquisitionRequestsReceived());

        // Test what is stored in the database
        TaskInfo taskInfo = taskInfoRepository.findByTaskId("1");
        assertEquals(null, taskInfo);

        taskInfo = taskInfoRepository.findByTaskId("2");
        assertEquals(null, taskInfo);

        log.info("wrongCacheIntervalFormatTest FINISHED!");
    }

}