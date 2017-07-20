package eu.h2020.symbiote.enabler.resourcemanager.integration;

import eu.h2020.symbiote.enabler.resourcemanager.messaging.RabbitManager;
import eu.h2020.symbiote.enabler.resourcemanager.model.TaskInfo;
import org.junit.After;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.Before;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.concurrent.ListenableFutureCallback;

import org.springframework.amqp.rabbit.AsyncRabbitTemplate;
import org.springframework.amqp.rabbit.AsyncRabbitTemplate.RabbitConverterFuture;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.ArrayList;

import eu.h2020.symbiote.enabler.resourcemanager.dummyListeners.DummyPlatformProxyListener;
import eu.h2020.symbiote.enabler.resourcemanager.repository.TaskInfoRepository;
import eu.h2020.symbiote.enabler.messaging.model.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


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
public class EnablerResourceManagerTests {

    private static Logger log = LoggerFactory
            .getLogger(EnablerResourceManagerTests.class);

    @Autowired
    private AsyncRabbitTemplate asyncRabbitTemplate;

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private TaskInfoRepository taskInfoRepository;

    @Autowired
    private DummyPlatformProxyListener dummyPlatformProxyListener;

    @Value("${rabbit.exchange.resourceManager.name}")
    private String resourceManagerExchangeName;
    @Value("${rabbit.exchange.resourceManager.type}")
    private String resourceManagerExchangeType;
    @Value("${rabbit.exchange.resourceManager.durable}")
    private boolean resourceManagerExchangeDurable;
    @Value("${rabbit.exchange.resourceManager.autodelete}")
    private boolean resourceManagerExchangeAutodelete;
    @Value("${rabbit.exchange.resourceManager.internal}")
    private boolean resourceManagerExchangeInternal;
    @Value("${rabbit.routingKey.resourceManager.startDataAcquisition}")
    private String startDataAcquisitionRoutingKey;
    @Value("${rabbit.routingKey.resourceManager.cancelTask}")
    private String cancelTaskRoutingKey;
    @Value("${rabbit.routingKey.resourceManager.unavailableResources}")
    private String unavailableResourcesRoutingKey;

    private ObjectMapper mapper = new ObjectMapper();

    // Execute the Setup method before the test.
    @Before
    public void setUp() throws Exception {
        dummyPlatformProxyListener.clearRequestReceivedByListener();
    }

    @After
    public void clearSetup() throws Exception {
        taskInfoRepository.deleteAll();
    }

    @Test
    public void resourceManagerGetResourceDetailsTest() throws Exception {

        final AtomicReference<ResourceManagerAcquisitionStartResponse> resultRef = new AtomicReference<>();
        ResourceManagerAcquisitionStartRequest query = createValidQueryToResourceManager(2);
        ArrayList<PlatformProxyAcquisitionStartRequest> requestReceivedByListener;

        log.info("Before sending the message");
        RabbitConverterFuture<ResourceManagerAcquisitionStartResponse> future = asyncRabbitTemplate.convertSendAndReceive(resourceManagerExchangeName, startDataAcquisitionRoutingKey, query);
        log.info("After sending the message");

        future.addCallback(new ListenableFutureCallbackCustom(resultRef));

        while(!future.isDone()) {
            TimeUnit.MILLISECONDS.sleep(100);
        }

        String responseInString = mapper.writeValueAsString(resultRef.get().getResources());
        log.info("Response String: " + responseInString);

        // Test what Enabler Logic receives
        assertEquals(2, resultRef.get().getResources().get(0).getResourceIds().size());
        assertEquals(1, resultRef.get().getResources().get(1).getResourceIds().size());

        assertEquals("resource1", resultRef.get().getResources().get(0).getResourceIds().get(0));
        assertEquals("resource2", resultRef.get().getResources().get(0).getResourceIds().get(1));
        assertEquals("resource4", resultRef.get().getResources().get(1).getResourceIds().get(0));

        while(dummyPlatformProxyListener.messagesReceived() < 2) {
            log.info("requestReceivedByListener.size(): " + dummyPlatformProxyListener.messagesReceived());
            TimeUnit.MILLISECONDS.sleep(100);
        }

        // Test what Platform Proxy receives
        requestReceivedByListener = dummyPlatformProxyListener.getRequestReceivedByListener();

        if (requestReceivedByListener.get(0).getResources().size() == 2) {
            assertEquals("resource1", requestReceivedByListener.get(0).getResources().get(0).getResourceId());
            assertEquals("resource2", requestReceivedByListener.get(0).getResources().get(1).getResourceId());
            assertEquals("resource4", requestReceivedByListener.get(1).getResources().get(0).getResourceId());
        } else {
            assertEquals("resource1", requestReceivedByListener.get(1).getResources().get(0).getResourceId());
            assertEquals("resource2", requestReceivedByListener.get(1).getResources().get(1).getResourceId());
            assertEquals("resource4", requestReceivedByListener.get(0).getResources().get(0).getResourceId());
        }

    }

//    @Test
    public void resourceManagerGetResourceDetailsNoResponseTest() throws Exception {

        final AtomicReference<ResourceManagerAcquisitionStartResponse> resultRef = new AtomicReference<>();
        ResourceManagerAcquisitionStartRequest query = new ResourceManagerAcquisitionStartRequest();
        ArrayList<ResourceManagerTaskInfoRequest> resources = new ArrayList<>();


        ResourceManagerTaskInfoRequest request1 = new ResourceManagerTaskInfoRequest();
        request1.setTaskId("1");
        request1.setCount(2);
        request1.setLocation("Paris");
        request1.setObservesProperty(Arrays.asList("temperature", "humidity"));
        request1.setInterval(60);
        resources.add(request1);

        query.setResources(resources);

        log.info("Before sending the message");
        RabbitConverterFuture<ResourceManagerAcquisitionStartResponse> future = asyncRabbitTemplate.convertSendAndReceive(resourceManagerExchangeName, startDataAcquisitionRoutingKey, query);
        log.info("After sending the message");

        future.addCallback(new ListenableFutureCallbackCustom(resultRef));

        while(!future.isDone()) {
            log.info("Sleeping!!!!!!");
            TimeUnit.MILLISECONDS.sleep(100);
        }

    }

    @Test
    public void resourceManagerGetResourceDetailsBadRequestTest() throws Exception {

        final AtomicReference<ResourceManagerAcquisitionStartResponse> resultRef = new AtomicReference<>();
        ResourceManagerAcquisitionStartRequest query = createBadQueryToResourceManager();

        log.info("Before sending the message");
        RabbitConverterFuture<ResourceManagerAcquisitionStartResponse> future = asyncRabbitTemplate.convertSendAndReceive(resourceManagerExchangeName, startDataAcquisitionRoutingKey, query);
        log.info("After sending the message");

        future.addCallback(new ListenableFutureCallbackCustom(resultRef));

        while(!future.isDone()) {
            TimeUnit.MILLISECONDS.sleep(100);
        }

        // Test what Enabler Logic receives
        assertEquals(null, resultRef.get().getResources().get(0).getResourceIds());

        // Test what Platform Proxy receives
        TimeUnit.MILLISECONDS.sleep(500);
        assertEquals(0, dummyPlatformProxyListener.messagesReceived());

    }

    @Test
    public void notSendingToPlatformProxyTest() throws Exception {

        final AtomicReference<ResourceManagerAcquisitionStartResponse> resultRef = new AtomicReference<>();
        ResourceManagerAcquisitionStartRequest query = createValidQueryToResourceManager(2);
        ArrayList<PlatformProxyAcquisitionStartRequest> requestReceivedByListener;

        // Forward to PlatformProxy only the 2nd task
        query.getResources().get(0).setInformPlatformProxy(false);

        log.info("Before sending the message");
        RabbitConverterFuture<ResourceManagerAcquisitionStartResponse> future = asyncRabbitTemplate.convertSendAndReceive(resourceManagerExchangeName, startDataAcquisitionRoutingKey, query);
        log.info("After sending the message");

        future.addCallback(new ListenableFutureCallbackCustom(resultRef));

        while(!future.isDone()) {
            TimeUnit.MILLISECONDS.sleep(100);
        }

        // Test what Enabler Logic receives
        assertEquals(2, resultRef.get().getResources().get(0).getResourceIds().size());
        assertEquals(1, resultRef.get().getResources().get(1).getResourceIds().size());

        assertEquals("resource1", resultRef.get().getResources().get(0).getResourceIds().get(0));
        assertEquals("resource2", resultRef.get().getResources().get(0).getResourceIds().get(1));
        assertEquals("resource4", resultRef.get().getResources().get(1).getResourceIds().get(0));

        while(dummyPlatformProxyListener.messagesReceived() < 1) {
            TimeUnit.MILLISECONDS.sleep(100);
        }

        // Test what Platform Proxy receives
        requestReceivedByListener = dummyPlatformProxyListener.getRequestReceivedByListener();
        assertEquals(1, dummyPlatformProxyListener.messagesReceived());
        assertEquals("resource4", requestReceivedByListener.get(0).getResources().get(0).getResourceId());

    }

    @Test
    public void allowCachingTest() throws Exception {

        final AtomicReference<ResourceManagerAcquisitionStartResponse> resultRef = new AtomicReference<>();
        ResourceManagerAcquisitionStartRequest query = createValidQueryToResourceManager(2);

        // Forward to PlatformProxy only the 2nd task
        query.getResources().get(0).setAllowCaching(true);

        log.info("Before sending the message");
        RabbitConverterFuture<ResourceManagerAcquisitionStartResponse> future = asyncRabbitTemplate.convertSendAndReceive(resourceManagerExchangeName, startDataAcquisitionRoutingKey, query);
        log.info("After sending the message");

        future.addCallback(new ListenableFutureCallbackCustom(resultRef));

        while(!future.isDone()) {
            TimeUnit.MILLISECONDS.sleep(100);
        }

        // Test what Enabler Logic receives
        assertEquals(2, resultRef.get().getResources().get(0).getResourceIds().size());
        assertEquals(1, resultRef.get().getResources().get(1).getResourceIds().size());

        assertEquals("resource1", resultRef.get().getResources().get(0).getResourceIds().get(0));
        assertEquals("resource2", resultRef.get().getResources().get(0).getResourceIds().get(1));
        assertEquals("resource4", resultRef.get().getResources().get(1).getResourceIds().get(0));

        while(dummyPlatformProxyListener.messagesReceived() < 2) {
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

    }

    @Test
    public void cancelTask() throws Exception {

        final AtomicReference<ResourceManagerAcquisitionStartResponse> resultRef = new AtomicReference<>();
        ResourceManagerAcquisitionStartRequest query = createValidQueryToResourceManager(2);

        log.info("Before sending the message");
        RabbitConverterFuture<ResourceManagerAcquisitionStartResponse> future = asyncRabbitTemplate.convertSendAndReceive(resourceManagerExchangeName, startDataAcquisitionRoutingKey, query);
        log.info("After sending the message");

        future.addCallback(new ListenableFutureCallbackCustom(resultRef));

        while(!future.isDone()) {
            TimeUnit.MILLISECONDS.sleep(100);
        }

        // Test what is stored in the database
        TaskInfo taskInfo = taskInfoRepository.findByTaskId("1");
        assertEquals(2, taskInfo.getResourceIds().size());

        taskInfo = taskInfoRepository.findByTaskId("2");
        assertEquals(1, taskInfo.getResourceIds().size());

        CancelTaskRequest cancelTaskRequest = new CancelTaskRequest();
        cancelTaskRequest.setTaskIdList(Arrays.asList("1", "2"));

        log.info("Before sending the message");
        rabbitTemplate.convertAndSend(resourceManagerExchangeName, cancelTaskRoutingKey, cancelTaskRequest);
        log.info("After sending the message");

        TimeUnit.MILLISECONDS.sleep(500);

        taskInfo = taskInfoRepository.findByTaskId("1");
        assertEquals(null, taskInfo);

        taskInfo = taskInfoRepository.findByTaskId("2");
        assertEquals(null, taskInfo);
    }

    @Test
    public void unavailableResourcesWithEnoughResourcesTest() throws Exception {
        ProblematicResourcesInfo unavailableResourcesInfo1 = new ProblematicResourcesInfo();
        unavailableResourcesInfo1.setTaskId("task1");
        unavailableResourcesInfo1.setProblematicResourceIds(Arrays.asList("1", "3"));

        ProblematicResourcesInfo unavailableResourcesInfo2 = new ProblematicResourcesInfo();
        unavailableResourcesInfo2.setTaskId("task2");
        unavailableResourcesInfo2.setProblematicResourceIds(Arrays.asList("resource4"));

        ProblematicResourcesMessage unavailableResourcesMessage = new ProblematicResourcesMessage();
        unavailableResourcesMessage.setProblematicResourcesInfoList(Arrays.asList(unavailableResourcesInfo1, unavailableResourcesInfo2));

        TaskInfo taskInfo = new TaskInfo();
        taskInfo.setTaskId("task1");
        taskInfo.setCount(5);
        taskInfo.setResourceIds(new ArrayList(Arrays.asList("1", "2", "3")));
        taskInfo.setStoredResourceIds(new ArrayList(Arrays.asList("4", "5", "6", "7", "8", "9")));
        taskInfoRepository.save(taskInfo);

        log.info("Before sending the message");
        rabbitTemplate.convertAndSend(resourceManagerExchangeName, unavailableResourcesRoutingKey, unavailableResourcesMessage);
        log.info("After sending the message");

        TimeUnit.MILLISECONDS.sleep(500);

        taskInfo = taskInfoRepository.findByTaskId("task2");
        assertEquals(null, taskInfo);

        taskInfo = taskInfoRepository.findByTaskId("task1");
        assertEquals(5, taskInfo.getResourceIds().size());
        assertEquals(2, taskInfo.getStoredResourceIds().size());

        assertEquals("2", taskInfo.getResourceIds().get(0));
        assertEquals("4", taskInfo.getResourceIds().get(1));
        assertEquals("5", taskInfo.getResourceIds().get(2));
        assertEquals("6", taskInfo.getResourceIds().get(3));
        assertEquals("7", taskInfo.getResourceIds().get(4));

        assertEquals("8", taskInfo.getStoredResourceIds().get(0));
        assertEquals("9", taskInfo.getStoredResourceIds().get(1));
    }

    private ResourceManagerAcquisitionStartRequest createValidQueryToResourceManager(int noTasks) {
        ArrayList<ResourceManagerTaskInfoRequest> resources = new ArrayList<>();
        ResourceManagerAcquisitionStartRequest request = new ResourceManagerAcquisitionStartRequest();

        ResourceManagerTaskInfoRequest request1 = new ResourceManagerTaskInfoRequest();

        request1.setTaskId("1");
        request1.setCount(2);
        request1.setLocation("Paris");
        request1.setObservesProperty(Arrays.asList("temperature", "humidity"));
        request1.setInterval(60);
        request1.setInformPlatformProxy(true);
        request1.setAllowCaching(false);
        request1.setCachingInterval(new Long(1000));
        resources.add(request1);

        if (noTasks > 1) {
            ResourceManagerTaskInfoRequest request2 = new ResourceManagerTaskInfoRequest();

            request2.setTaskId("2");
            request2.setCount(1);
            request2.setLocation("Athens");
            request2.setObservesProperty(Arrays.asList("air quality"));
            request2.setInterval(60);
            request2.setInformPlatformProxy(true);
            request2.setAllowCaching(false);
            request2.setCachingInterval(new Long(1000));
            resources.add(request2);
        }

        request.setResources(resources);
        return request;
    }

    private ResourceManagerAcquisitionStartRequest createBadQueryToResourceManager() {
        ArrayList<ResourceManagerTaskInfoRequest> resources = new ArrayList<>();
        ResourceManagerAcquisitionStartRequest request = new ResourceManagerAcquisitionStartRequest();
        ResourceManagerTaskInfoRequest request1 = new ResourceManagerTaskInfoRequest();

        request1.setTaskId("1");
        request1.setCount(2);
        request1.setLocation("Zurich");
        request1.setObservesProperty(Arrays.asList("temperature", "humidity"));
        request1.setInterval(60);
        request1.setInformPlatformProxy(true);
        request1.setAllowCaching(false);
        request1.setCachingInterval(new Long(1000));
        resources.add(request1);
        request.setResources(resources);

        return request;
    }

    private class ListenableFutureCallbackCustom implements ListenableFutureCallback<ResourceManagerAcquisitionStartResponse> {
        AtomicReference<ResourceManagerAcquisitionStartResponse> resultRef;

        ListenableFutureCallbackCustom(AtomicReference<ResourceManagerAcquisitionStartResponse> resultRef) {
            this.resultRef = resultRef;
        }

        public void onSuccess(ResourceManagerAcquisitionStartResponse result) {
            try {
                log.info("Successfully received response: " + mapper.writeValueAsString(result));
            } catch (JsonProcessingException e) {
                log.info(e.toString());
            }
            resultRef.set(result);

        }

        public void onFailure(Throwable ex) {
            fail("Accessed the element which does not exist");
        }

    }

}