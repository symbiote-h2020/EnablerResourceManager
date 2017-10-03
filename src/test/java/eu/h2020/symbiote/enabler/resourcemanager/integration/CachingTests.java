package eu.h2020.symbiote.enabler.resourcemanager.integration;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.h2020.symbiote.enabler.messaging.model.*;
import eu.h2020.symbiote.enabler.resourcemanager.dummyListeners.DummyEnablerLogicListener;
import eu.h2020.symbiote.enabler.resourcemanager.dummyListeners.DummyPlatformProxyListener;
import eu.h2020.symbiote.enabler.resourcemanager.model.TaskInfo;
import eu.h2020.symbiote.enabler.resourcemanager.repository.TaskInfoRepository;
import eu.h2020.symbiote.enabler.resourcemanager.utils.*;

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
import org.springframework.web.client.RestTemplate;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;


@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@ContextConfiguration
@Configuration
@EnableAutoConfiguration
@ComponentScan
@ActiveProfiles("test")
public class CachingTests {

    private static Log log = LogFactory
            .getLog(CachingTests.class);

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
    private SearchHelper searchHelper;

    @Autowired
    private RestTemplate restTemplate;

    @Autowired
    @Qualifier("symbIoTeCoreUrl")
    private String symbIoTeCoreUrl;

    @Value("${rabbit.exchange.resourceManager.name}")
    private String resourceManagerExchangeName;

    @Value("${rabbit.routingKey.resourceManager.cancelTask}")
    private String cancelTaskRoutingKey;

    @Value("${rabbit.routingKey.resourceManager.startDataAcquisition}")
    private String startDataAcquisitionRoutingKey;

    private ObjectMapper mapper = new ObjectMapper();


    // Execute the Setup method before the test.
    @Before
    public void setUp() throws Exception {
        TestHelper.setUp(dummyPlatformProxyListener, dummyEnablerLogicListener, authorizationManager, symbIoTeCoreUrl,
                searchHelper, restTemplate);
    }

    @After
    public void clearSetup() throws Exception {
        TestHelper.clearSetup(taskInfoRepository);
    }

    @Test
    public void createAndCancelCachingTasksTest() throws Exception {
        log.info("createAndCancelCachingTasksTest STARTED!");

        /**
         *  Create the task
         */

        final AtomicReference<ResourceManagerAcquisitionStartResponse> createResultRef = new AtomicReference<>();
        ResourceManagerAcquisitionStartRequest query = TestHelper.createValidQueryToResourceManager(2);
        List<PlatformProxyAcquisitionStartRequest> startAcquisitionRequestsReceivedByListener;

        query.getTasks().get(0).setAllowCaching(true);
        query.getTasks().get(0).setCachingInterval("P0-0-0T0:0:1");
        query.getTasks().get(1).setAllowCaching(true);
        query.getTasks().get(1).setCachingInterval("P0-0-0T0:0:1");

        log.info("Before sending the message");
        RabbitConverterFuture<ResourceManagerAcquisitionStartResponse> createFuture = asyncRabbitTemplate
                .convertSendAndReceive(resourceManagerExchangeName, startDataAcquisitionRoutingKey, query);
        log.info("After sending the message");

        createFuture.addCallback(new ListenableFutureAcquisitionStartCallback("resourceManagerGetResourceDetailsTest", createResultRef));

        while(!createFuture.isDone()) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(100);

        String responseInString = mapper.writeValueAsString(createResultRef.get().getTasks());
        log.info("Response String: " + responseInString);

        // Test what Enabler Logic receives
        assertEquals(ResourceManagerTasksStatus.SUCCESS, createResultRef.get().getStatus());
        assertEquals("ALL the task requests were successful!", createResultRef.get().getMessage());
        assertEquals(2, createResultRef.get().getTasks().get(0).getResourceIds().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, createResultRef.get().getTasks().get(0).getStatus());
        assertEquals(1, createResultRef.get().getTasks().get(1).getResourceIds().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, createResultRef.get().getTasks().get(1).getStatus());

        assertEquals("resource1", createResultRef.get().getTasks().get(0).getResourceIds().get(0));
        assertEquals("resource2", createResultRef.get().getTasks().get(0).getResourceIds().get(1));
        assertEquals("resource4", createResultRef.get().getTasks().get(1).getResourceIds().get(0));

        // Check the stored ScheduledTaskInfoUpdate
        assertNotNull(searchHelper.getScheduledTaskInfoUpdateMap().get("1"));
        assertNotNull(searchHelper.getScheduledTaskInfoUpdateMap().get("2"));

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
        assertEquals(1, taskInfo.getStoredResourceIds().size());
        assertEquals(2, taskInfo.getResourceUrls().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, taskInfo.getStatus());
        assertEquals("resource1", taskInfo.getResourceIds().get(0));
        assertEquals("resource2", taskInfo.getResourceIds().get(1));
        assertEquals(symbIoTeCoreUrl + "/Sensors('resource1')", taskInfo.getResourceUrls().get("resource1"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('resource2')", taskInfo.getResourceUrls().get("resource2"));

        taskInfo = taskInfoRepository.findByTaskId("2");
        assertEquals(1, taskInfo.getResourceIds().size());
        assertEquals(1, taskInfo.getStoredResourceIds().size());
        assertEquals(1, taskInfo.getResourceUrls().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, taskInfo.getStatus());
        assertEquals("resource4", taskInfo.getResourceIds().get(0));
        assertEquals(symbIoTeCoreUrl + "/Sensors('resource4')", taskInfo.getResourceUrls().get("resource4"));


        /**
         *  Check if timer is activated
         */

        taskInfo = taskInfoRepository.findByTaskId("1");
        taskInfo.getStoredResourceIds().clear();
        taskInfoRepository.save(taskInfo);

        taskInfo = taskInfoRepository.findByTaskId("2");
        taskInfo.getStoredResourceIds().clear();
        taskInfoRepository.save(taskInfo);

        taskInfo = taskInfoRepository.findByTaskId("1");
        assertEquals(0, taskInfo.getStoredResourceIds().size());
        taskInfo = taskInfoRepository.findByTaskId("2");
        assertEquals(0, taskInfo.getStoredResourceIds().size());

        // Wait a little more than 1 sec in order to check if stored resources are updated
        TimeUnit.MILLISECONDS.sleep(1200);

        // Test what is stored in the database
        taskInfo = taskInfoRepository.findByTaskId("1");
        assertEquals(2, taskInfo.getResourceIds().size());
        assertEquals(1, taskInfo.getStoredResourceIds().size());
        assertEquals(2, taskInfo.getResourceUrls().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, taskInfo.getStatus());
        assertEquals("resource1", taskInfo.getResourceIds().get(0));
        assertEquals("resource2", taskInfo.getResourceIds().get(1));
        assertEquals(symbIoTeCoreUrl + "/Sensors('resource1')", taskInfo.getResourceUrls().get("resource1"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('resource2')", taskInfo.getResourceUrls().get("resource2"));

        taskInfo = taskInfoRepository.findByTaskId("2");
        assertEquals(1, taskInfo.getResourceIds().size());
        assertEquals(1, taskInfo.getStoredResourceIds().size());
        assertEquals(1, taskInfo.getResourceUrls().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, taskInfo.getStatus());
        assertEquals("resource4", taskInfo.getResourceIds().get(0));
        assertEquals(symbIoTeCoreUrl + "/Sensors('resource4')", taskInfo.getResourceUrls().get("resource4"));

        // Check the stored ScheduledTaskInfoUpdate
        assertNotNull(searchHelper.getScheduledTaskInfoUpdateMap().get("1"));
        assertNotNull(searchHelper.getScheduledTaskInfoUpdateMap().get("2"));


        /**
         *  Cancel the Task
         */

        final AtomicReference<CancelTaskResponse> cancelResultRef = new AtomicReference<>();
        List<CancelTaskRequest> cancelTaskRequestArrayList;
        
        CancelTaskRequest cancelTaskRequest = new CancelTaskRequest();
        cancelTaskRequest.setTaskIdList(Arrays.asList("1", "2"));


        log.info("Before sending the message");
        RabbitConverterFuture<CancelTaskResponse> cancelFuture = asyncRabbitTemplate
                .convertSendAndReceive(resourceManagerExchangeName, cancelTaskRoutingKey, cancelTaskRequest);
        log.info("After sending the message");

        cancelFuture.addCallback(new ListenableFutureCancelCallback("successfulCancelTaskTest", cancelResultRef));

        while(!cancelFuture.isDone()) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(100);


        // Test what Enabler Logic receives
        assertEquals(CancelTaskResponseStatus.SUCCESS, cancelResultRef.get().getStatus());
        assertEquals("ALL tasks were deleted!", cancelResultRef.get().getMessage());

        // Test the database
        taskInfo = taskInfoRepository.findByTaskId("1");
        assertNull(taskInfo);

        taskInfo = taskInfoRepository.findByTaskId("2");
        assertNull(taskInfo);

        // Check the stored ScheduledTaskInfoUpdates
        assertNull(searchHelper.getScheduledTaskInfoUpdateMap().get("1"));
        assertNull(searchHelper.getScheduledTaskInfoUpdateMap().get("2"));

        while (dummyPlatformProxyListener.cancelTaskRequestsReceived() == 0) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(100);


        cancelTaskRequestArrayList = dummyPlatformProxyListener.getCancelTaskRequestsReceivedByListener();
        assertEquals(1, cancelTaskRequestArrayList.size());
        assertEquals(2, cancelTaskRequestArrayList.get(0).getTaskIdList().size());
        assertEquals("1", cancelTaskRequestArrayList.get(0).getTaskIdList().get(0));
        assertEquals("2", cancelTaskRequestArrayList.get(0).getTaskIdList().get(1));

        log.info("createAndCancelCachingTasksTest FINISHED!");
    }



}