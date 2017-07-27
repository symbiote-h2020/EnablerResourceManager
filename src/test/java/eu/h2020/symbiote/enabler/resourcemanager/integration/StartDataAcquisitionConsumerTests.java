package eu.h2020.symbiote.enabler.resourcemanager.integration;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.h2020.symbiote.core.internal.CoreQueryRequest;
import eu.h2020.symbiote.enabler.messaging.model.*;
import eu.h2020.symbiote.enabler.resourcemanager.dummyListeners.DummyEnablerLogicListener;
import eu.h2020.symbiote.enabler.resourcemanager.dummyListeners.DummyPlatformProxyListener;
import eu.h2020.symbiote.enabler.resourcemanager.model.TaskInfo;
import eu.h2020.symbiote.enabler.resourcemanager.repository.TaskInfoRepository;
import eu.h2020.symbiote.enabler.resourcemanager.utils.ListenableFutureCallbackCustom;
import eu.h2020.symbiote.enabler.resourcemanager.utils.TestHelper;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.amqp.rabbit.AsyncRabbitTemplate;
import org.springframework.amqp.rabbit.AsyncRabbitTemplate.RabbitConverterFuture;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;


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
public class StartDataAcquisitionConsumerTests {

    private static Logger log = LoggerFactory
            .getLogger(StartDataAcquisitionConsumerTests.class);

    @Autowired
    private AsyncRabbitTemplate asyncRabbitTemplate;

    @Autowired
    private TaskInfoRepository taskInfoRepository;

    @Autowired
    private DummyPlatformProxyListener dummyPlatformProxyListener;

    @Autowired
    private DummyEnablerLogicListener dummyEnablerLogicListener;

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
    }

    @After
    public void clearSetup() throws Exception {
        taskInfoRepository.deleteAll();
    }

    @Test
    public void resourceManagerGetResourceDetailsTest() throws Exception {
        log.info("resourceManagerGetResourceDetailsTest STARTED!");

        final AtomicReference<ResourceManagerAcquisitionStartResponse> resultRef = new AtomicReference<>();
        ResourceManagerAcquisitionStartRequest query = TestHelper.createValidQueryToResourceManager(2);
        List<PlatformProxyAcquisitionStartRequest> startAcquisitionRequestsReceivedByListener;

        log.info("Before sending the message");
        RabbitConverterFuture<ResourceManagerAcquisitionStartResponse> future = asyncRabbitTemplate
                .convertSendAndReceive(resourceManagerExchangeName, startDataAcquisitionRoutingKey, query);
        log.info("After sending the message");

        future.addCallback(new ListenableFutureCallbackCustom("resourceManagerGetResourceDetailsTest", resultRef));

        while(!future.isDone()) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(100);

        String responseInString = mapper.writeValueAsString(resultRef.get().getResources());
        log.info("Response String: " + responseInString);

        // Test what Enabler Logic receives
        assertEquals(2, resultRef.get().getResources().get(0).getResourceIds().size());
        assertEquals(1, resultRef.get().getResources().get(1).getResourceIds().size());

        assertEquals("resource1", resultRef.get().getResources().get(0).getResourceIds().get(0));
        assertEquals("resource2", resultRef.get().getResources().get(0).getResourceIds().get(1));
        assertEquals("resource4", resultRef.get().getResources().get(1).getResourceIds().get(0));

        while(dummyPlatformProxyListener.startAcquisitionRequestsReceived() < 2) {
            log.info("startAcquisitionRequestsReceivedByListener.size(): " + dummyPlatformProxyListener.startAcquisitionRequestsReceived());
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(100);

        // Test what Platform Proxy receives
        startAcquisitionRequestsReceivedByListener = dummyPlatformProxyListener.getStartAcquisitionRequestsReceivedByListener();

        if (startAcquisitionRequestsReceivedByListener.get(0).getResources().size() == 2) {
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

        log.info("resourceManagerGetResourceDetailsTest FINISHED!");
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
        request1.setQueryInterval_ms(60);
        resources.add(request1);

        query.setResources(resources);

        log.info("Before sending the message");
        RabbitConverterFuture<ResourceManagerAcquisitionStartResponse> future = asyncRabbitTemplate
                .convertSendAndReceive(resourceManagerExchangeName, startDataAcquisitionRoutingKey, query);
        log.info("After sending the message");

        future.addCallback(new ListenableFutureCallbackCustom("resourceManagerGetResourceDetailsNoResponseTest", resultRef));

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

        future.addCallback(new ListenableFutureCallbackCustom("resourceManagerGetResourceDetailsBadRequestTest", resultRef));

        while(!future.isDone()) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(100);

        // Test what Enabler Logic receives
        assertEquals(0, resultRef.get().getResources().get(0).getResourceIds().size());

        // Test what Platform Proxy receives
        TimeUnit.MILLISECONDS.sleep(500);
        assertEquals(0, dummyPlatformProxyListener.startAcquisitionRequestsReceived());

        log.info("resourceManagerGetResourceDetailsBadRequestTest FINISHED!");
    }

    @Test
    public void notSendingToPlatformProxyTest() throws Exception {
        log.info("notSendingToPlatformProxyTest STARTED!");

        final AtomicReference<ResourceManagerAcquisitionStartResponse> resultRef = new AtomicReference<>();
        ResourceManagerAcquisitionStartRequest query = TestHelper.createValidQueryToResourceManager(2);
        List<PlatformProxyAcquisitionStartRequest> startAcquisitionRequestsReceivedByListener;

        // Forward to PlatformProxy only the 2nd task
        query.getResources().get(0).setInformPlatformProxy(false);

        log.info("Before sending the message");
        RabbitConverterFuture<ResourceManagerAcquisitionStartResponse> future = asyncRabbitTemplate
                .convertSendAndReceive(resourceManagerExchangeName, startDataAcquisitionRoutingKey, query);
        log.info("After sending the message");

        future.addCallback(new ListenableFutureCallbackCustom("notSendingToPlatformProxyTest", resultRef));

        while(!future.isDone()) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(100);

        // Test what Enabler Logic receives
        assertEquals(2, resultRef.get().getResources().get(0).getResourceIds().size());
        assertEquals(1, resultRef.get().getResources().get(1).getResourceIds().size());

        assertEquals("resource1", resultRef.get().getResources().get(0).getResourceIds().get(0));
        assertEquals("resource2", resultRef.get().getResources().get(0).getResourceIds().get(1));
        assertEquals("resource4", resultRef.get().getResources().get(1).getResourceIds().get(0));

        while(dummyPlatformProxyListener.startAcquisitionRequestsReceived() < 1) {
            TimeUnit.MILLISECONDS.sleep(100);
        }

        // Test what Platform Proxy receives
        startAcquisitionRequestsReceivedByListener = dummyPlatformProxyListener.getStartAcquisitionRequestsReceivedByListener();
        assertEquals(1, dummyPlatformProxyListener.startAcquisitionRequestsReceived());
        assertEquals("resource4", startAcquisitionRequestsReceivedByListener.get(0).getResources().get(0).getResourceId());
        assertEquals("enablerLogicName2", startAcquisitionRequestsReceivedByListener.get(0).getEnablerLogicName());

        log.info("notSendingToPlatformProxyTest FINISHED!");
    }

    @Test
    public void allowCachingTest() throws Exception {
        log.info("allowCachingTest STARTED!");

        final AtomicReference<ResourceManagerAcquisitionStartResponse> resultRef = new AtomicReference<>();
        ResourceManagerAcquisitionStartRequest query = TestHelper.createValidQueryToResourceManager(2);

        // Forward to PlatformProxy only the 2nd task
        query.getResources().get(0).setAllowCaching(true);

        log.info("Before sending the message");
        RabbitConverterFuture<ResourceManagerAcquisitionStartResponse> future = asyncRabbitTemplate
                .convertSendAndReceive(resourceManagerExchangeName, startDataAcquisitionRoutingKey, query);
        log.info("After sending the message");

        future.addCallback(new ListenableFutureCallbackCustom("allowCachingTest", resultRef));

        while(!future.isDone()) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(100);

        // Test what Enabler Logic receives
        assertEquals(2, resultRef.get().getResources().get(0).getResourceIds().size());
        assertEquals(1, resultRef.get().getResources().get(1).getResourceIds().size());

        assertEquals("resource1", resultRef.get().getResources().get(0).getResourceIds().get(0));
        assertEquals("resource2", resultRef.get().getResources().get(0).getResourceIds().get(1));
        assertEquals("resource4", resultRef.get().getResources().get(1).getResourceIds().get(0));

        while(dummyPlatformProxyListener.startAcquisitionRequestsReceived() < 2) {
            TimeUnit.MILLISECONDS.sleep(100);
        }

        // Test what is stored in the database
        TaskInfo taskInfo = taskInfoRepository.findByTaskId("1");
        assertEquals(2, taskInfo.getResourceIds().size());
        assertEquals(1, taskInfo.getStoredResourceIds().size());
        assertEquals("resource1", taskInfo.getResourceIds().get(0));
        assertEquals("resource2", taskInfo.getResourceIds().get(1));
        assertEquals("resource3", taskInfo.getStoredResourceIds().get(0));

        taskInfo = taskInfoRepository.findByTaskId("2");
        assertEquals(1, taskInfo.getResourceIds().size());
        assertEquals(0, taskInfo.getStoredResourceIds().size());
        assertEquals("resource4", taskInfo.getResourceIds().get(0));

        log.info("allowCachingTest FINISHED!");

    }

}