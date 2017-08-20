package eu.h2020.symbiote.enabler.resourcemanager.integration;


import com.fasterxml.jackson.databind.ObjectMapper;
import eu.h2020.symbiote.core.internal.CoreQueryRequest;
import eu.h2020.symbiote.enabler.messaging.model.*;
import eu.h2020.symbiote.enabler.resourcemanager.dummyListeners.DummyEnablerLogicListener;
import eu.h2020.symbiote.enabler.resourcemanager.dummyListeners.DummyPlatformProxyListener;
import eu.h2020.symbiote.enabler.resourcemanager.messaging.consumers.CancelTaskConsumer;
import eu.h2020.symbiote.enabler.resourcemanager.model.TaskInfo;
import eu.h2020.symbiote.enabler.resourcemanager.repository.TaskInfoRepository;
import eu.h2020.symbiote.enabler.resourcemanager.utils.ListenableFutureCallbackCustom;
import eu.h2020.symbiote.enabler.resourcemanager.utils.TestHelper;
import javafx.concurrent.Task;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.AsyncRabbitTemplate;
import org.springframework.amqp.rabbit.AsyncRabbitTemplate.RabbitConverterFuture;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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
public class UpdateTaskConsumerTests {

    private static Logger log = LoggerFactory
            .getLogger(UpdateTaskConsumerTests.class);

    @Autowired
    private AsyncRabbitTemplate asyncRabbitTemplate;

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private TaskInfoRepository taskInfoRepository;

    @Autowired
    private DummyPlatformProxyListener dummyPlatformProxyListener;

    @Autowired
    private DummyEnablerLogicListener dummyEnablerLogicListener;

    @Autowired
    @Qualifier("symbIoTeCoreUrl")
    private String symbIoTeCoreUrl;

    @Value("${rabbit.exchange.resourceManager.name}")
    private String resourceManagerExchangeName;

    @Value("${rabbit.routingKey.resourceManager.updateTask}")
    private String updateTaskRoutingKey;

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
    public void updateTaskTest() throws Exception {
        // In this test, the value of informPlatformProxy remains the same in all the tasks

        log.info("updateTaskTest STARTED!");

        final AtomicReference<ResourceManagerAcquisitionStartResponse> resultRef = new AtomicReference<>();
        List<PlatformProxyUpdateRequest> taskUpdateRequestsReceivedByListener;

        TaskInfo task1 = new TaskInfo();
        CoreQueryRequest coreQueryRequest = new CoreQueryRequest.Builder()
                .locationName("Zurich")
                .observedProperty(Arrays.asList("temperature", "humidity"))
                .shouldRank(true)
                .build();

        Map<String, String> resourceUrls1 = new HashMap<>();
        resourceUrls1.put("resource1", symbIoTeCoreUrl + "/Sensors('resource1')");
        resourceUrls1.put("resource2", symbIoTeCoreUrl + "/Sensors('resource2')");

        task1.setTaskId("1");
        task1.setMinNoResources(2);
        task1.setCoreQueryRequest(coreQueryRequest);
        task1.setResourceIds(Arrays.asList("resource1", "resource2"));
        task1.setQueryInterval("P0-0-0T0:0:0.06");
        task1.setAllowCaching(true);
        task1.setCachingInterval("P0-0-0T0:0:1");
        task1.setInformPlatformProxy(true);
        task1.setStoredResourceIds(Arrays.asList("3", "4"));
        task1.setEnablerLogicName("enablerLogic");
        task1.setStatus(ResourceManagerTaskInfoResponseStatus.SUCCESS);
        task1.setResourceUrls(resourceUrls1);
        taskInfoRepository.save(task1);

        Map<String, String> resourceUrls2 = new HashMap<>();
        resourceUrls2.put("21", symbIoTeCoreUrl + "/Sensors('21')");
        resourceUrls2.put("22", symbIoTeCoreUrl + "/Sensors('22')");
        TaskInfo task2 = new TaskInfo(task1);
        task2.setTaskId("2");
        task2.setInformPlatformProxy(false); // Because we want to keep the same value in the updatedTask2
        task2.setResourceIds(Arrays.asList("21", "22"));
        task2.setResourceUrls(resourceUrls2);
        taskInfoRepository.save(task2);

        Map<String, String> resourceUrls3 = new HashMap<>();
        resourceUrls3.put("31", symbIoTeCoreUrl + "/Sensors('31')");
        resourceUrls3.put("32", symbIoTeCoreUrl + "/Sensors('32')");
        TaskInfo task3 = new TaskInfo(task1);
        task3.setTaskId("3");
        task3.setResourceIds(Arrays.asList("31", "32"));
        task3.setResourceUrls(resourceUrls3);
        taskInfoRepository.save(task3);

        Map<String, String> resourceUrls4 = new HashMap<>();
        resourceUrls4.put("41", symbIoTeCoreUrl + "/Sensors('41')");
        resourceUrls4.put("42", symbIoTeCoreUrl + "/Sensors('42')");
        TaskInfo task4 = new TaskInfo(task1);
        task4.setTaskId("4");
        task4.setInformPlatformProxy(false); // Because we want to keep the same value in the updatedTask4
        task4.setResourceIds(Arrays.asList("41", "42"));
        task4.setResourceUrls(resourceUrls4);
        taskInfoRepository.save(task4);

        Map<String, String> resourceUrls5 = new HashMap<>();
        resourceUrls5.put("51", symbIoTeCoreUrl + "/Sensors('51')");
        resourceUrls5.put("52", symbIoTeCoreUrl + "/Sensors('52')");
        TaskInfo task5 = new TaskInfo(task1);
        task5.setTaskId("5");
        task5.setResourceIds(Arrays.asList("51", "52"));
        task5.setResourceUrls(resourceUrls5);
        taskInfoRepository.save(task5);

        Map<String, String> resourceUrls6 = new HashMap<>();
        resourceUrls6.put("61", symbIoTeCoreUrl + "/Sensors('61')");
        resourceUrls6.put("62", symbIoTeCoreUrl + "/Sensors('62')");
        TaskInfo task6 = new TaskInfo(task1);
        task6.setTaskId("6");
        task6.setResourceIds(Arrays.asList("61", "62"));
        task6.setResourceUrls(resourceUrls6);
        taskInfoRepository.save(task6);

        // This task should reach Platform Proxy and inform it for new resources
        TaskInfo updatedTask1 = new TaskInfo(task1);
        updatedTask1.getCoreQueryRequest().setLocation_name("Paris");

        // This task should not reach Platform Proxy, because InformPlatformProxy == false
        TaskInfo updatedTask2 = new TaskInfo(task2);

        // This task should not reach Platform Proxy, because there is nothing new to report
        TaskInfo updatedTask3 = new TaskInfo();
        updatedTask3.setTaskId("3");
        updatedTask3.setMinNoResources(null);
        updatedTask3.setCoreQueryRequest(null);
        updatedTask3.setAllowCaching(null);
        updatedTask3.setCachingInterval(null);
        updatedTask3.setInformPlatformProxy(null);
        updatedTask3.setQueryInterval(null);
        updatedTask3.setEnablerLogicName(null);
        updatedTask3.setResourceIds(Arrays.asList("31", "32"));
        updatedTask3.setStoredResourceIds(Arrays.asList("3", "4"));
        updatedTask3.setStatus(ResourceManagerTaskInfoResponseStatus.SUCCESS);
        updatedTask3.setResourceUrls(resourceUrls3);

        // This task should not reach Platform Proxy, because InformPlatformProxy == false
        TaskInfo updatedTask4 = new TaskInfo(task4);
        updatedTask4.setInformPlatformProxy(false);
        updatedTask4.setEnablerLogicName("updatedTask4");
        updatedTask4.setQueryInterval("P0-0-0T0:0:0.1");

        // This task should reach Platform Proxy, because InformPlatformProxy == true and the enablerLogic changed
        TaskInfo updatedTask5 = new TaskInfo(task5);
        updatedTask5.setEnablerLogicName("updatedTask5");

        // This task should reach Platform Proxy, because InformPlatformProxy == true and the query interval changed
        TaskInfo updatedTask6 = new TaskInfo(task6);
        updatedTask6.setQueryInterval("P0-0-0T0:0:0.1");

        ResourceManagerAcquisitionStartRequest req = new ResourceManagerAcquisitionStartRequest();
        req.setResources(Arrays.asList(new ResourceManagerTaskInfoRequest(updatedTask1),
                new ResourceManagerTaskInfoRequest(updatedTask2),
                new ResourceManagerTaskInfoRequest(updatedTask3),
                new ResourceManagerTaskInfoRequest(updatedTask4),
                new ResourceManagerTaskInfoRequest(updatedTask5),
                new ResourceManagerTaskInfoRequest(updatedTask6)));


        log.info("Before sending the message");
        RabbitConverterFuture<ResourceManagerAcquisitionStartResponse> future = asyncRabbitTemplate
                .convertSendAndReceive(resourceManagerExchangeName, updateTaskRoutingKey, req);
        log.info("After sending the message");

        future.addCallback(new ListenableFutureCallbackCustom("updateTaskTest", resultRef));

        while(!future.isDone()) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(100);

        TaskInfo storedTaskInfo1 = taskInfoRepository.findByTaskId("1");
        TaskInfo storedTaskInfo2 = taskInfoRepository.findByTaskId("2");
        TaskInfo storedTaskInfo3 = taskInfoRepository.findByTaskId("3");
        TaskInfo storedTaskInfo4 = taskInfoRepository.findByTaskId("4");
        TaskInfo storedTaskInfo5 = taskInfoRepository.findByTaskId("5");
        TaskInfo storedTaskInfo6 = taskInfoRepository.findByTaskId("6");

        // Only task3.equals(storedTaskInfo3) should be true, because updateTask3 is the only one where nothing changes
        assertEquals(false, task1.equals(storedTaskInfo1));
        assertEquals(true, task2.equals(storedTaskInfo2));
        assertEquals(true, task3.equals(storedTaskInfo3));
        assertEquals(false, task4.equals(storedTaskInfo4));
        assertEquals(false, task5.equals(storedTaskInfo5));
        assertEquals(false, task6.equals(storedTaskInfo6));

        // Only updatedTask1.equals(storedTaskInfo1) and updatedTask3.equals(storedTaskInfo3) should be false, because
        // storedTaskInfo1 has modified resources and storedTaskInfo3 has the initial fields, not null fields
        assertEquals(false, updatedTask1.equals(storedTaskInfo1));
        assertEquals(true, updatedTask2.equals(storedTaskInfo2));
        assertEquals(false, updatedTask3.equals(storedTaskInfo3));
        assertEquals(true, updatedTask4.equals(storedTaskInfo4));
        assertEquals(true, updatedTask5.equals(storedTaskInfo5));
        assertEquals(true, updatedTask6.equals(storedTaskInfo6));

        // Test what is stored in the database
        assertEquals(2, storedTaskInfo1.getResourceIds().size());
        assertEquals(1, storedTaskInfo1.getStoredResourceIds().size());
        assertEquals(2, storedTaskInfo1.getResourceUrls().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, storedTaskInfo1.getStatus());
        assertEquals("resource1", storedTaskInfo1.getResourceIds().get(0));
        assertEquals("resource2", storedTaskInfo1.getResourceIds().get(1));
        assertEquals("resource3", storedTaskInfo1.getStoredResourceIds().get(0));
        assertEquals(symbIoTeCoreUrl + "/Sensors('resource1')", storedTaskInfo1.getResourceUrls().get("resource1"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('resource2')", storedTaskInfo1.getResourceUrls().get("resource2"));

        assertEquals(2, storedTaskInfo2.getResourceIds().size());
        assertEquals(2, storedTaskInfo2.getStoredResourceIds().size());
        assertEquals(2, storedTaskInfo2.getResourceUrls().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, storedTaskInfo2.getStatus());
        assertEquals("21", storedTaskInfo2.getResourceIds().get(0));
        assertEquals("22", storedTaskInfo2.getResourceIds().get(1));
        assertEquals("3", storedTaskInfo2.getStoredResourceIds().get(0));
        assertEquals("4", storedTaskInfo2.getStoredResourceIds().get(1));
        assertEquals(symbIoTeCoreUrl + "/Sensors('21')", storedTaskInfo2.getResourceUrls().get("21"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('22')", storedTaskInfo2.getResourceUrls().get("22"));

        assertEquals(2, storedTaskInfo3.getResourceIds().size());
        assertEquals(2, storedTaskInfo3.getStoredResourceIds().size());
        assertEquals(2, storedTaskInfo3.getResourceUrls().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, storedTaskInfo3.getStatus());
        assertEquals("31", storedTaskInfo3.getResourceIds().get(0));
        assertEquals("32", storedTaskInfo3.getResourceIds().get(1));
        assertEquals("3", storedTaskInfo3.getStoredResourceIds().get(0));
        assertEquals("4", storedTaskInfo3.getStoredResourceIds().get(1));
        assertEquals(symbIoTeCoreUrl + "/Sensors('31')", storedTaskInfo3.getResourceUrls().get("31"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('32')", storedTaskInfo3.getResourceUrls().get("32"));

        assertEquals(2, storedTaskInfo4.getResourceIds().size());
        assertEquals(2, storedTaskInfo4.getStoredResourceIds().size());
        assertEquals(2, storedTaskInfo4.getResourceUrls().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, storedTaskInfo4.getStatus());
        assertEquals("41", storedTaskInfo4.getResourceIds().get(0));
        assertEquals("42", storedTaskInfo4.getResourceIds().get(1));
        assertEquals("3", storedTaskInfo4.getStoredResourceIds().get(0));
        assertEquals("4", storedTaskInfo4.getStoredResourceIds().get(1));
        assertEquals(symbIoTeCoreUrl + "/Sensors('41')", storedTaskInfo4.getResourceUrls().get("41"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('42')", storedTaskInfo4.getResourceUrls().get("42"));

        assertEquals(2, storedTaskInfo5.getResourceIds().size());
        assertEquals(2, storedTaskInfo5.getStoredResourceIds().size());
        assertEquals(2, storedTaskInfo5.getResourceUrls().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, storedTaskInfo5.getStatus());
        assertEquals("51", storedTaskInfo5.getResourceIds().get(0));
        assertEquals("52", storedTaskInfo5.getResourceIds().get(1));
        assertEquals("3", storedTaskInfo5.getStoredResourceIds().get(0));
        assertEquals("4", storedTaskInfo5.getStoredResourceIds().get(1));
        assertEquals(symbIoTeCoreUrl + "/Sensors('51')", storedTaskInfo5.getResourceUrls().get("51"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('52')", storedTaskInfo5.getResourceUrls().get("52"));

        assertEquals(2, storedTaskInfo6.getResourceIds().size());
        assertEquals(2, storedTaskInfo6.getStoredResourceIds().size());
        assertEquals(2, storedTaskInfo6.getResourceUrls().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, storedTaskInfo6.getStatus());
        assertEquals("61", storedTaskInfo6.getResourceIds().get(0));
        assertEquals("62", storedTaskInfo6.getResourceIds().get(1));
        assertEquals("3", storedTaskInfo6.getStoredResourceIds().get(0));
        assertEquals("4", storedTaskInfo6.getStoredResourceIds().get(1));
        assertEquals(symbIoTeCoreUrl + "/Sensors('61')", storedTaskInfo6.getResourceUrls().get("61"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('62')", storedTaskInfo6.getResourceUrls().get("62"));

        // Test what Enabler Logic receives
        assertEquals(6, resultRef.get().getResources().size());
        assertEquals(2, resultRef.get().getResources().get(0).getResourceIds().size());
        assertEquals(2, resultRef.get().getResources().get(1).getResourceIds().size());
        assertEquals(2, resultRef.get().getResources().get(2).getResourceIds().size());
        assertEquals(2, resultRef.get().getResources().get(3).getResourceIds().size());
        assertEquals(2, resultRef.get().getResources().get(4).getResourceIds().size());
        assertEquals(2, resultRef.get().getResources().get(5).getResourceIds().size());

        assertEquals("resource1", resultRef.get().getResources().get(0).getResourceIds().get(0));
        assertEquals("resource2", resultRef.get().getResources().get(0).getResourceIds().get(1));
        assertEquals("21", resultRef.get().getResources().get(1).getResourceIds().get(0));
        assertEquals("22", resultRef.get().getResources().get(1).getResourceIds().get(1));
        assertEquals("31", resultRef.get().getResources().get(2).getResourceIds().get(0));
        assertEquals("32", resultRef.get().getResources().get(2).getResourceIds().get(1));
        assertEquals("41", resultRef.get().getResources().get(3).getResourceIds().get(0));
        assertEquals("42", resultRef.get().getResources().get(3).getResourceIds().get(1));
        assertEquals("51", resultRef.get().getResources().get(4).getResourceIds().get(0));
        assertEquals("52", resultRef.get().getResources().get(4).getResourceIds().get(1));
        assertEquals("61", resultRef.get().getResources().get(5).getResourceIds().get(0));
        assertEquals("62", resultRef.get().getResources().get(5).getResourceIds().get(1));

        while(dummyPlatformProxyListener.updateAcquisitionRequestsReceived() < 3) {
            log.info("updateAcquisitionRequestsReceived.size(): " + dummyPlatformProxyListener.updateAcquisitionRequestsReceived());
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(300);

        // Test what Platform Proxy receives
        taskUpdateRequestsReceivedByListener = dummyPlatformProxyListener.getUpdateAcquisitionRequestsReceivedByListener();

        assertEquals(3, dummyPlatformProxyListener.updateAcquisitionRequestsReceived());
        assertEquals(0, dummyPlatformProxyListener.startAcquisitionRequestsReceived());
        assertEquals(0, dummyPlatformProxyListener.cancelTaskRequestsReceived());

        for (PlatformProxyUpdateRequest request : taskUpdateRequestsReceivedByListener) {

            log.info("id = " + request.getTaskId());

            if (request.getTaskId().equals("1")) {
                assertEquals("resource1", request.getResources().get(0).getResourceId());
                assertEquals("resource2", request.getResources().get(1).getResourceId());
                assertEquals("enablerLogic", request.getEnablerLogicName());
                assertEquals(60, (long) request.getQueryInterval_ms());
                continue;
            }

            if (request.getTaskId().equals("5")) {
                Map<String, String> resourcesMap = new HashMap<>();
                resourcesMap.put("51", symbIoTeCoreUrl + "/Sensors('51')");
                resourcesMap.put("52", symbIoTeCoreUrl + "/Sensors('52')");

                assertEquals(2, request.getResources().size());
                assertEquals(resourcesMap.get(request.getResources().get(0).getResourceId()),
                        request.getResources().get(0).getAccessURL());
                assertEquals(resourcesMap.get(request.getResources().get(1).getResourceId()),
                        request.getResources().get(1).getAccessURL());
                assertEquals(60, (long) request.getQueryInterval_ms());
                continue;
            }

            if (request.getTaskId().equals("6")) {
                Map<String, String> resourcesMap = new HashMap<>();
                resourcesMap.put("61", symbIoTeCoreUrl + "/Sensors('61')");
                resourcesMap.put("62", symbIoTeCoreUrl + "/Sensors('62')");

                assertEquals(2, request.getResources().size());
                assertEquals(resourcesMap.get(request.getResources().get(0).getResourceId()),
                        request.getResources().get(0).getAccessURL());
                assertEquals(resourcesMap.get(request.getResources().get(1).getResourceId()),
                        request.getResources().get(1).getAccessURL());
                assertEquals(100, (long) request.getQueryInterval_ms());
                continue;
            }

            fail("The code should not reach here, because no other tasks should be received by the platform proxy");
        }

        log.info("updateTaskTest FINISHED!");
    }

    @Test
    public void wrongQueryIntervalFormatUpdateTest() throws Exception {
        log.info("wrongQueryIntervalFormatUpdateTest STARTED!");

        final AtomicReference<ResourceManagerAcquisitionStartResponse> resultRef = new AtomicReference<>();

        ResourceManagerAcquisitionStartRequest query = TestHelper.createValidQueryToResourceManager(2);
        Field queryIntervalField = query.getResources().get(1).getClass().getDeclaredField("queryInterval");
        queryIntervalField.setAccessible(true);
        queryIntervalField.set(query.getResources().get(1), "10s");

        log.info("Before sending the message");
        RabbitConverterFuture<ResourceManagerAcquisitionStartResponse> future = asyncRabbitTemplate
                .convertSendAndReceive(resourceManagerExchangeName, updateTaskRoutingKey, query);
        log.info("After sending the message");

        future.addCallback(new ListenableFutureCallbackCustom("wrongQueryIntervalFormatUpdateTest", resultRef));

        while(!future.isDone()) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(100);

        // Test what Enabler Logic receives
        assertEquals(ResourceManagerAcquisitionStartResponseStatus.FAILED_WRONG_FORMAT_INTERVAL, resultRef.get().getStatus());

        TimeUnit.MILLISECONDS.sleep(500);

        // Test what Platform Proxy receives
        assertEquals(0, dummyPlatformProxyListener.startAcquisitionRequestsReceived());
        assertEquals(0, dummyPlatformProxyListener.updateAcquisitionRequestsReceived());

        // Test what is stored in the database
        TaskInfo taskInfo = taskInfoRepository.findByTaskId("1");
        assertEquals(null, taskInfo);

        taskInfo = taskInfoRepository.findByTaskId("2");
        assertEquals(null, taskInfo);

        log.info("wrongQueryIntervalFormatUpdateTest FINISHED!");

    }

    @Test
    public void wrongCacheIntervalFormatUpdateTest() throws Exception {
        log.info("wrongCacheIntervalFormatUpdateTest STARTED!");

        final AtomicReference<ResourceManagerAcquisitionStartResponse> resultRef = new AtomicReference<>();

        ResourceManagerAcquisitionStartRequest query = TestHelper.createValidQueryToResourceManager(2);
        Field cachingIntervalField = query.getResources().get(1).getClass().getDeclaredField("cachingInterval");
        cachingIntervalField.setAccessible(true);
        cachingIntervalField.set(query.getResources().get(1), "10s");

        log.info("Before sending the message");
        RabbitConverterFuture<ResourceManagerAcquisitionStartResponse> future = asyncRabbitTemplate
                .convertSendAndReceive(resourceManagerExchangeName, updateTaskRoutingKey, query);
        log.info("After sending the message");

        future.addCallback(new ListenableFutureCallbackCustom("wrongCacheIntervalFormatUpdateTest", resultRef));

        while(!future.isDone()) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(100);

        // Test what Enabler Logic receives
        assertEquals(ResourceManagerAcquisitionStartResponseStatus.FAILED_WRONG_FORMAT_INTERVAL, resultRef.get().getStatus());

        TimeUnit.MILLISECONDS.sleep(500);

        // Test what Platform Proxy receives
        assertEquals(0, dummyPlatformProxyListener.startAcquisitionRequestsReceived());
        assertEquals(0, dummyPlatformProxyListener.updateAcquisitionRequestsReceived());

        // Test what is stored in the database
        TaskInfo taskInfo = taskInfoRepository.findByTaskId("1");
        assertEquals(null, taskInfo);

        taskInfo = taskInfoRepository.findByTaskId("2");
        assertEquals(null, taskInfo);

        log.info("wrongCacheIntervalFormatUpdateTest FINISHED!");

    }

    @Test
    public void updateTaskWithInformPlatformProxyBecomingFalseTest() throws Exception {
        // In this test, the value of informPlatformProxy changes from true to false

        log.info("updateTaskWithInformPlatformProxyBecomingFalseTest STARTED!");

        final AtomicReference<ResourceManagerAcquisitionStartResponse> resultRef = new AtomicReference<>();
        List<CancelTaskRequest> cancelTaskRequestsReceivedByListener;

        TaskInfo task1 = new TaskInfo();
        CoreQueryRequest coreQueryRequest = new CoreQueryRequest.Builder()
                .locationName("Zurich")
                .observedProperty(Arrays.asList("temperature", "humidity"))
                .shouldRank(true)
                .build();

        Map<String, String> resourceUrls1 = new HashMap<>();
        resourceUrls1.put("resource1", symbIoTeCoreUrl + "/Sensors('resource1')");
        resourceUrls1.put("resource2", symbIoTeCoreUrl + "/Sensors('resource2')");

        task1.setTaskId("1");
        task1.setMinNoResources(2);
        task1.setCoreQueryRequest(coreQueryRequest);
        task1.setResourceIds(Arrays.asList("resource1", "resource2"));
        task1.setQueryInterval("P0-0-0T0:0:0.06");
        task1.setAllowCaching(true);
        task1.setCachingInterval("P0-0-0T0:0:1");
        task1.setInformPlatformProxy(true);
        task1.setStoredResourceIds(Arrays.asList("3", "4"));
        task1.setEnablerLogicName("enablerLogic");
        task1.setStatus(ResourceManagerTaskInfoResponseStatus.SUCCESS);
        task1.setResourceUrls(resourceUrls1);
        taskInfoRepository.save(task1);

        Map<String, String> resourceUrls2 = new HashMap<>();
        resourceUrls2.put("21", symbIoTeCoreUrl + "/Sensors('21')");
        resourceUrls2.put("22", symbIoTeCoreUrl + "/Sensors('22')");
        TaskInfo task2 = new TaskInfo(task1);
        task2.setTaskId("2");
        task2.setResourceIds(Arrays.asList("21", "22"));
        task2.setResourceUrls(resourceUrls2);
        taskInfoRepository.save(task2);

         // This task should reach Platform Proxy and inform it for new resources
        TaskInfo updatedTask1 = new TaskInfo(task1);
        updatedTask1.getCoreQueryRequest().setLocation_name("Paris");
        updatedTask1.setInformPlatformProxy(false);

        // This task should not reach Platform Proxy, because InformPlatformProxy == false
        TaskInfo updatedTask2 = new TaskInfo(task2);
        updatedTask2.setInformPlatformProxy(false);

        ResourceManagerAcquisitionStartRequest req = new ResourceManagerAcquisitionStartRequest();
        req.setResources(Arrays.asList(new ResourceManagerTaskInfoRequest(updatedTask1),
                new ResourceManagerTaskInfoRequest(updatedTask2)));


        log.info("Before sending the message");
        RabbitConverterFuture<ResourceManagerAcquisitionStartResponse> future = asyncRabbitTemplate
                .convertSendAndReceive(resourceManagerExchangeName, updateTaskRoutingKey, req);
        log.info("After sending the message");

        future.addCallback(new ListenableFutureCallbackCustom("updateTaskWithInformPlatformProxyBecomingFalseTest", resultRef));

        while(!future.isDone()) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(100);

        TaskInfo storedTaskInfo1 = taskInfoRepository.findByTaskId("1");
        TaskInfo storedTaskInfo2 = taskInfoRepository.findByTaskId("2");

        // Both tasks change their informPlatformProxy field
        assertEquals(false, task1.equals(storedTaskInfo1));
        assertEquals(false, task2.equals(storedTaskInfo2));

        // Only updatedTask1.equals(storedTaskInfo1) should be false, because it has modified resources
        assertEquals(false, updatedTask1.equals(storedTaskInfo1));
        assertEquals(true, updatedTask2.equals(storedTaskInfo2));

        // Test what is stored in the database
        assertEquals(2, storedTaskInfo1.getResourceIds().size());
        assertEquals(1, storedTaskInfo1.getStoredResourceIds().size());
        assertEquals(2, storedTaskInfo1.getResourceUrls().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, storedTaskInfo1.getStatus());
        assertEquals("resource1", storedTaskInfo1.getResourceIds().get(0));
        assertEquals("resource2", storedTaskInfo1.getResourceIds().get(1));
        assertEquals("resource3", storedTaskInfo1.getStoredResourceIds().get(0));
        assertEquals(symbIoTeCoreUrl + "/Sensors('resource1')", storedTaskInfo1.getResourceUrls().get("resource1"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('resource2')", storedTaskInfo1.getResourceUrls().get("resource2"));

        assertEquals(2, storedTaskInfo2.getResourceIds().size());
        assertEquals(2, storedTaskInfo2.getStoredResourceIds().size());
        assertEquals(2, storedTaskInfo2.getResourceUrls().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, storedTaskInfo2.getStatus());
        assertEquals("21", storedTaskInfo2.getResourceIds().get(0));
        assertEquals("22", storedTaskInfo2.getResourceIds().get(1));
        assertEquals("3", storedTaskInfo2.getStoredResourceIds().get(0));
        assertEquals("4", storedTaskInfo2.getStoredResourceIds().get(1));
        assertEquals(symbIoTeCoreUrl + "/Sensors('21')", storedTaskInfo2.getResourceUrls().get("21"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('22')", storedTaskInfo2.getResourceUrls().get("22"));

        // Test what Enabler Logic receives
        assertEquals(2, resultRef.get().getResources().size());
        assertEquals(2, resultRef.get().getResources().get(0).getResourceIds().size());
        assertEquals(2, resultRef.get().getResources().get(1).getResourceIds().size());

        assertEquals("resource1", resultRef.get().getResources().get(0).getResourceIds().get(0));
        assertEquals("resource2", resultRef.get().getResources().get(0).getResourceIds().get(1));
        assertEquals("21", resultRef.get().getResources().get(1).getResourceIds().get(0));
        assertEquals("22", resultRef.get().getResources().get(1).getResourceIds().get(1));

        while(dummyPlatformProxyListener.cancelTaskRequestsReceived() < 1) {
            log.info("updateAcquisitionRequestsReceived.size(): " + dummyPlatformProxyListener.updateAcquisitionRequestsReceived());
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(300);

        // Test what Platform Proxy receives
        cancelTaskRequestsReceivedByListener = dummyPlatformProxyListener.getCancelTaskRequestsReceivedByListener();

        assertEquals(0, dummyPlatformProxyListener.updateAcquisitionRequestsReceived());
        assertEquals(0, dummyPlatformProxyListener.startAcquisitionRequestsReceived());
        assertEquals(1, dummyPlatformProxyListener.cancelTaskRequestsReceived());

        assertEquals(2, cancelTaskRequestsReceivedByListener.get(0).getTaskIdList().size());
        assertEquals("1", cancelTaskRequestsReceivedByListener.get(0).getTaskIdList().get(0));
        assertEquals("2", cancelTaskRequestsReceivedByListener.get(0).getTaskIdList().get(1));

        log.info("updateTaskWithInformPlatformProxyBecomingFalseTest FINISHED!");
    }

    @Test
    public void updateTaskWithAllowCachingTest() throws Exception {

        log.info("updateTaskWithAllowCachingBecomingFalseTest STARTED!");

        final AtomicReference<ResourceManagerAcquisitionStartResponse> resultRef = new AtomicReference<>();
        List<PlatformProxyUpdateRequest> taskUpdateRequestsReceivedByListener;

        TaskInfo task1 = new TaskInfo();
        CoreQueryRequest coreQueryRequest = new CoreQueryRequest.Builder()
                .locationName("Zurich")
                .observedProperty(Arrays.asList("temperature", "humidity"))
                .shouldRank(true)
                .build();

        Map<String, String> resourceUrls1 = new HashMap<>();
        resourceUrls1.put("resource1", symbIoTeCoreUrl + "/Sensors('resource1')");
        resourceUrls1.put("resource2", symbIoTeCoreUrl + "/Sensors('resource2')");

        task1.setTaskId("1");
        task1.setMinNoResources(2);
        task1.setCoreQueryRequest(coreQueryRequest);
        task1.setResourceIds(Arrays.asList("resource1", "resource2"));
        task1.setQueryInterval("P0-0-0T0:0:0.06");
        task1.setAllowCaching(true);
        task1.setCachingInterval("P0-0-0T0:0:1");
        task1.setInformPlatformProxy(true);
        task1.setStoredResourceIds(Arrays.asList("3", "4"));
        task1.setEnablerLogicName("enablerLogic");
        task1.setStatus(ResourceManagerTaskInfoResponseStatus.SUCCESS);
        task1.setResourceUrls(resourceUrls1);
        taskInfoRepository.save(task1);

        Map<String, String> resourceUrls2 = new HashMap<>();
        resourceUrls2.put("21", symbIoTeCoreUrl + "/Sensors('21')");
        resourceUrls2.put("22", symbIoTeCoreUrl + "/Sensors('22')");
        TaskInfo task2 = new TaskInfo(task1);
        task2.setTaskId("2");
        task2.setResourceIds(Arrays.asList("21", "22"));
        task2.setResourceUrls(resourceUrls2);
        taskInfoRepository.save(task2);

        // resource1 should not be in the storedResourceIds after the update
        Map<String, String> resourceUrls3 = new HashMap<>();
        resourceUrls3.put("31", symbIoTeCoreUrl + "/Sensors('31')");
        resourceUrls3.put("32", symbIoTeCoreUrl + "/Sensors('32')");
        resourceUrls3.put("resource1", symbIoTeCoreUrl + "/Sensors('resource1')");
        TaskInfo task3 = new TaskInfo(task1);
        task3.setTaskId("3");
        task3.getCoreQueryRequest().setLocation_name("Paris");
        task3.setResourceIds(Arrays.asList("31", "32", "resource1"));
        task3.setAllowCaching(false);
        task3.setResourceUrls(resourceUrls3);
        taskInfoRepository.save(task3);

        Map<String, String> resourceUrls4 = new HashMap<>();
        resourceUrls4.put("41", symbIoTeCoreUrl + "/Sensors('41')");
        resourceUrls4.put("42", symbIoTeCoreUrl + "/Sensors('42')");
        TaskInfo task4 = new TaskInfo(task1);
        task4.setTaskId("4");
        task4.setResourceIds(Arrays.asList("41", "42"));
        task4.setAllowCaching(false);
        task4.setResourceUrls(resourceUrls4);
        taskInfoRepository.save(task4);

        // This task should reach Platform Proxy and inform it for new resources
        // true -> false transition
        // CoreQueryRequest changed
        TaskInfo updatedTask1 = new TaskInfo(task1);
        updatedTask1.getCoreQueryRequest().setLocation_name("Paris");
        updatedTask1.setAllowCaching(false);

        // This task should not reach Platform Proxy, because nothing that concerns the Platform Proxy changes
        // true -> false transition
        // CoreQueryRequest did not change
        TaskInfo updatedTask2 = new TaskInfo(task2);
        updatedTask2.setAllowCaching(false);

        // This task should reach Platform Proxy, because informPlatformProxy == true
        // false -> true transition
        // CoreQueryRequest did not change
        TaskInfo updatedTask3 = new TaskInfo(task3);
        updatedTask3.setAllowCaching(true);

        // This task should reach Platform Proxy, because informPlatformProxy == true
        // false -> true transition
        // CoreQueryRequest changed
        TaskInfo updatedTask4 = new TaskInfo(task4);
        updatedTask4.setAllowCaching(true);
        updatedTask4.getCoreQueryRequest().setLocation_name("Paris");

        ResourceManagerAcquisitionStartRequest req = new ResourceManagerAcquisitionStartRequest();
        req.setResources(Arrays.asList(new ResourceManagerTaskInfoRequest(updatedTask1),
                new ResourceManagerTaskInfoRequest(updatedTask2),
                new ResourceManagerTaskInfoRequest(updatedTask3),
                new ResourceManagerTaskInfoRequest(updatedTask4)));


        log.info("Before sending the message");
        RabbitConverterFuture<ResourceManagerAcquisitionStartResponse> future = asyncRabbitTemplate
                .convertSendAndReceive(resourceManagerExchangeName, updateTaskRoutingKey, req);
        log.info("After sending the message");

        future.addCallback(new ListenableFutureCallbackCustom("updateTaskWithAllowCachingBecomingFalseTest", resultRef));

        while(!future.isDone()) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(100);

        TaskInfo storedTaskInfo1 = taskInfoRepository.findByTaskId("1");
        TaskInfo storedTaskInfo2 = taskInfoRepository.findByTaskId("2");
        TaskInfo storedTaskInfo3 = taskInfoRepository.findByTaskId("3");
        TaskInfo storedTaskInfo4 = taskInfoRepository.findByTaskId("4");

        // All tasks change their allowCaching field
        assertEquals(false, task1.equals(storedTaskInfo1));
        assertEquals(false, task2.equals(storedTaskInfo2));
        assertEquals(false, task3.equals(storedTaskInfo3));
        assertEquals(false, task4.equals(storedTaskInfo4));

        // Both should be false, because the stored resources are cleared
        assertEquals(false, updatedTask1.equals(storedTaskInfo1));
        assertEquals(false, updatedTask2.equals(storedTaskInfo2));

        // Because the storedResources are updated
        assertEquals(false, updatedTask3.equals(storedTaskInfo3));
        assertEquals(false, updatedTask4.equals(storedTaskInfo4));


        // Test if the stored resources were cleared in the first 2 tasks
        assertEquals(2, storedTaskInfo1.getResourceIds().size());
        assertEquals(0, storedTaskInfo1.getStoredResourceIds().size());
        assertEquals(2, storedTaskInfo1.getResourceUrls().size());
        assertEquals(2, storedTaskInfo2.getResourceIds().size());
        assertEquals(0, storedTaskInfo2.getStoredResourceIds().size());
        assertEquals(2, storedTaskInfo2.getResourceUrls().size());

        // Test the statuses
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, storedTaskInfo1.getStatus());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, storedTaskInfo2.getStatus());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, storedTaskInfo3.getStatus());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, storedTaskInfo4.getStatus());

        // Test if stored resources were added in the last 2 tasks
        assertEquals(3, storedTaskInfo3.getResourceIds().size());
        assertEquals(2, storedTaskInfo3.getStoredResourceIds().size());
        assertEquals(3, storedTaskInfo3.getResourceUrls().size());
        assertEquals(2, storedTaskInfo4.getResourceIds().size());
        assertEquals(1, storedTaskInfo4.getStoredResourceIds().size());
        assertEquals(2, storedTaskInfo4.getResourceUrls().size());

        // Check the resourceIds
        assertEquals("resource1", storedTaskInfo1.getResourceIds().get(0));
        assertEquals("resource2", storedTaskInfo1.getResourceIds().get(1));
        assertEquals("21", storedTaskInfo2.getResourceIds().get(0));
        assertEquals("22", storedTaskInfo2.getResourceIds().get(1));
        assertEquals("31", storedTaskInfo3.getResourceIds().get(0));
        assertEquals("32", storedTaskInfo3.getResourceIds().get(1));
        assertEquals("resource1", storedTaskInfo3.getResourceIds().get(2));
        assertEquals("resource1", storedTaskInfo4.getResourceIds().get(0));
        assertEquals("resource2", storedTaskInfo4.getResourceIds().get(1));

        // Check the storedResourceIds
        assertEquals("resource2", storedTaskInfo3.getStoredResourceIds().get(0));
        assertEquals("resource3", storedTaskInfo3.getStoredResourceIds().get(1));
        assertEquals("resource3", storedTaskInfo4.getStoredResourceIds().get(0));

        // Check the resourceUrls
        assertEquals(symbIoTeCoreUrl + "/Sensors('resource1')", storedTaskInfo1.getResourceUrls().get("resource1"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('resource2')", storedTaskInfo1.getResourceUrls().get("resource2"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('21')", storedTaskInfo2.getResourceUrls().get("21"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('22')", storedTaskInfo2.getResourceUrls().get("22"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('31')", storedTaskInfo3.getResourceUrls().get("31"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('32')", storedTaskInfo3.getResourceUrls().get("32"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('resource1')", storedTaskInfo3.getResourceUrls().get("resource1"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('resource1')", storedTaskInfo4.getResourceUrls().get("resource1"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('resource2')", storedTaskInfo4.getResourceUrls().get("resource2"));

        // Test what Enabler Logic receives
        assertEquals(4, resultRef.get().getResources().size());
        assertEquals(2, resultRef.get().getResources().get(0).getResourceIds().size());
        assertEquals(2, resultRef.get().getResources().get(1).getResourceIds().size());
        assertEquals(3, resultRef.get().getResources().get(2).getResourceIds().size());
        assertEquals(2, resultRef.get().getResources().get(3).getResourceIds().size());

        assertEquals("resource1", resultRef.get().getResources().get(0).getResourceIds().get(0));
        assertEquals("resource2", resultRef.get().getResources().get(0).getResourceIds().get(1));
        assertEquals("21", resultRef.get().getResources().get(1).getResourceIds().get(0));
        assertEquals("22", resultRef.get().getResources().get(1).getResourceIds().get(1));
        assertEquals("31", resultRef.get().getResources().get(2).getResourceIds().get(0));
        assertEquals("32", resultRef.get().getResources().get(2).getResourceIds().get(1));
        assertEquals("resource1", resultRef.get().getResources().get(2).getResourceIds().get(2));
        assertEquals("resource1", resultRef.get().getResources().get(3).getResourceIds().get(0));
        assertEquals("resource2", resultRef.get().getResources().get(3).getResourceIds().get(1));

        while (dummyPlatformProxyListener.updateAcquisitionRequestsReceived() < 2) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(300);

        // Test what Platform Proxy receives

        taskUpdateRequestsReceivedByListener = dummyPlatformProxyListener.getUpdateAcquisitionRequestsReceivedByListener();

        assertEquals(2, dummyPlatformProxyListener.updateAcquisitionRequestsReceived());
        assertEquals(0, dummyPlatformProxyListener.startAcquisitionRequestsReceived());
        assertEquals(0, dummyPlatformProxyListener.cancelTaskRequestsReceived());

        for (PlatformProxyUpdateRequest request : taskUpdateRequestsReceivedByListener) {

            log.info("id = " + request.getTaskId());

            if (request.getTaskId().equals("1")) {
                assertEquals("resource1", request.getResources().get(0).getResourceId());
                assertEquals("resource2", request.getResources().get(1).getResourceId());
                continue;
            }

            if (request.getTaskId().equals("4")) {
                assertEquals("resource1", request.getResources().get(0).getResourceId());
                assertEquals("resource2", request.getResources().get(1).getResourceId());
                continue;
            }

            fail("The code should not reach here, because no other tasks should be received by the platform proxy");
        }

        log.info("updateTaskWithAllowCachingBecomingFalseTest FINISHED!");
    }

    @Test
    public void changeInMinNoResourcesTest() throws Exception {

        log.info("changeInMinNoResourcesTest STARTED!");

        final AtomicReference<ResourceManagerAcquisitionStartResponse> resultRef = new AtomicReference<>();
        List<PlatformProxyUpdateRequest> taskUpdateRequestsReceivedByListener;

        TaskInfo task1 = new TaskInfo();
        CoreQueryRequest coreQueryRequest = new CoreQueryRequest.Builder()
                .locationName("Paris")
                .observedProperty(Arrays.asList("temperature", "humidity"))
                .shouldRank(true)
                .build();

        Map<String, String> resourceUrls1 = new HashMap<>();
        resourceUrls1.put("resource1", symbIoTeCoreUrl + "/Sensors('resource1')");
        resourceUrls1.put("resource2", symbIoTeCoreUrl + "/Sensors('resource2')");

        task1.setTaskId("1");
        task1.setMinNoResources(2);
        task1.setCoreQueryRequest(coreQueryRequest);
        task1.setResourceIds(Arrays.asList("resource1", "resource2"));
        task1.setQueryInterval("P0-0-0T0:0:0.06");
        task1.setAllowCaching(true);
        task1.setCachingInterval("P0-0-0T0:0:1");
        task1.setInformPlatformProxy(true);
        task1.setStoredResourceIds(Arrays.asList("3", "4"));
        task1.setEnablerLogicName("enablerLogic");
        task1.setStatus(ResourceManagerTaskInfoResponseStatus.SUCCESS);
        task1.setResourceUrls(resourceUrls1);
        taskInfoRepository.save(task1);

        Map<String, String> resourceUrls2 = new HashMap<>();
        resourceUrls2.put("21", symbIoTeCoreUrl + "/Sensors('21')");
        resourceUrls2.put("22", symbIoTeCoreUrl + "/Sensors('22')");
        TaskInfo task2 = new TaskInfo(task1);
        task2.setTaskId("2");
        task2.setResourceIds(Arrays.asList("21", "22"));
        task2.setResourceUrls(resourceUrls2);
        taskInfoRepository.save(task2);

        Map<String, String> resourceUrls3 = new HashMap<>();
        resourceUrls3.put("31", symbIoTeCoreUrl + "/Sensors('31')");
        resourceUrls3.put("32", symbIoTeCoreUrl + "/Sensors('32')");
        TaskInfo task3 = new TaskInfo(task1);
        task3.setTaskId("3");
        task3.setResourceIds(Arrays.asList("31", "32"));
        task3.setResourceUrls(resourceUrls3);
        taskInfoRepository.save(task3);

        Map<String, String> resourceUrls4 = new HashMap<>();
        resourceUrls4.put("41", symbIoTeCoreUrl + "/Sensors('41')");
        resourceUrls4.put("42", symbIoTeCoreUrl + "/Sensors('42')");
        TaskInfo task4 = new TaskInfo(task1);
        task4.setTaskId("4");
        task4.setResourceIds(Arrays.asList("41", "42"));
        task4.setStoredResourceIds(Arrays.asList("3", "4", "badCRAMrespose"));
        task4.setResourceUrls(resourceUrls4);
        taskInfoRepository.save(task4);

        // ToDo: Add a test when a task is saved with NOT_ENOUGH_RESOURCES STATUS

        // This task should not reach Platform Proxy, since minNoResources decreased
        TaskInfo updatedTask1 = new TaskInfo(task1);
        updatedTask1.setMinNoResources(1);

        // This task should reach Platform Proxy, since minNoResources increased and there were enough resources
        TaskInfo updatedTask2 = new TaskInfo(task2);
        updatedTask2.setMinNoResources(3);

        // This task should not reach Platform Proxy, since minNoResources increased and there were not enough storedResources
        TaskInfo updatedTask3 = new TaskInfo(task3);
        updatedTask3.setMinNoResources(5);

        // This task should not reach Platform Proxy, because there are not enough available resources
        TaskInfo updatedTask4 = new TaskInfo(task4);
        updatedTask4.setMinNoResources(5);


        ResourceManagerAcquisitionStartRequest req = new ResourceManagerAcquisitionStartRequest();
        req.setResources(Arrays.asList(new ResourceManagerTaskInfoRequest(updatedTask1),
                new ResourceManagerTaskInfoRequest(updatedTask2),
                new ResourceManagerTaskInfoRequest(updatedTask3),
                new ResourceManagerTaskInfoRequest(updatedTask4)));


        log.info("Before sending the message");
        RabbitConverterFuture<ResourceManagerAcquisitionStartResponse> future = asyncRabbitTemplate
                .convertSendAndReceive(resourceManagerExchangeName, updateTaskRoutingKey, req);
        log.info("After sending the message");

        future.addCallback(new ListenableFutureCallbackCustom("changeInMinNoResourcesTest", resultRef));

        while(!future.isDone()) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(100);

        TaskInfo storedTaskInfo1 = taskInfoRepository.findByTaskId("1");
        TaskInfo storedTaskInfo2 = taskInfoRepository.findByTaskId("2");
        TaskInfo storedTaskInfo3 = taskInfoRepository.findByTaskId("3");
        TaskInfo storedTaskInfo4 = taskInfoRepository.findByTaskId("4");

        // All tasks minNoResources field has been changed
        assertEquals(false, task1.equals(storedTaskInfo1));
        assertEquals(false, task2.equals(storedTaskInfo2));
        assertEquals(false, task3.equals(storedTaskInfo3));
        assertEquals(false, task4.equals(storedTaskInfo4));

        assertEquals(true, updatedTask1.equals(storedTaskInfo1)); // Nothing changes
        assertEquals(false, updatedTask2.equals(storedTaskInfo2)); // The storedResources are updated
        assertEquals(false, updatedTask3.equals(storedTaskInfo3)); // The status changed to NOT_ENOUGH_RESOURCES
        assertEquals(false, updatedTask4.equals(storedTaskInfo4)); // The status changed to NOT_ENOUGH_RESOURCES

        // Check the statuses
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, storedTaskInfo1.getStatus());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, storedTaskInfo2.getStatus());
        assertEquals(ResourceManagerTaskInfoResponseStatus.NOT_ENOUGH_RESOURCES, storedTaskInfo3.getStatus());
        assertEquals(ResourceManagerTaskInfoResponseStatus.NOT_ENOUGH_RESOURCES, storedTaskInfo4.getStatus());

        // Check the sizes of resourceIds, storedResourceIds and resourceUrls
        assertEquals(2, storedTaskInfo1.getResourceIds().size());
        assertEquals(2, storedTaskInfo1.getStoredResourceIds().size());
        assertEquals(2, storedTaskInfo1.getResourceUrls().size());
        assertEquals(3, storedTaskInfo2.getResourceIds().size());
        assertEquals(1, storedTaskInfo2.getStoredResourceIds().size());
        assertEquals(3, storedTaskInfo2.getResourceUrls().size());
        assertEquals(4, storedTaskInfo3.getResourceIds().size());
        assertEquals(0, storedTaskInfo3.getStoredResourceIds().size());
        assertEquals(4, storedTaskInfo3.getResourceUrls().size());
        assertEquals(4, storedTaskInfo4.getResourceIds().size());
        assertEquals(0, storedTaskInfo4.getStoredResourceIds().size());
        assertEquals(4, storedTaskInfo4.getResourceUrls().size());

        // Check the resourceIds
        assertEquals("resource1", storedTaskInfo1.getResourceIds().get(0));
        assertEquals("resource2", storedTaskInfo1.getResourceIds().get(1));
        assertEquals("21", storedTaskInfo2.getResourceIds().get(0));
        assertEquals("22", storedTaskInfo2.getResourceIds().get(1));
        assertEquals("3", storedTaskInfo2.getResourceIds().get(2));
        assertEquals("31", storedTaskInfo3.getResourceIds().get(0));
        assertEquals("32", storedTaskInfo3.getResourceIds().get(1));
        assertEquals("3", storedTaskInfo3.getResourceIds().get(2));
        assertEquals("4", storedTaskInfo3.getResourceIds().get(3));
        assertEquals("41", storedTaskInfo4.getResourceIds().get(0));
        assertEquals("42", storedTaskInfo4.getResourceIds().get(1));
        assertEquals("3", storedTaskInfo4.getResourceIds().get(2));
        assertEquals("4", storedTaskInfo4.getResourceIds().get(3));

        // Check the storedResourceIds
        assertEquals("4", storedTaskInfo2.getStoredResourceIds().get(0));

        // Check the resourceUrls
        assertEquals(symbIoTeCoreUrl + "/Sensors('resource1')", storedTaskInfo1.getResourceUrls().get("resource1"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('resource2')", storedTaskInfo1.getResourceUrls().get("resource2"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('21')", storedTaskInfo2.getResourceUrls().get("21"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('22')", storedTaskInfo2.getResourceUrls().get("22"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('3')", storedTaskInfo2.getResourceUrls().get("3"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('31')", storedTaskInfo3.getResourceUrls().get("31"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('32')", storedTaskInfo3.getResourceUrls().get("32"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('3')", storedTaskInfo3.getResourceUrls().get("3"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('4')", storedTaskInfo3.getResourceUrls().get("4"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('41')", storedTaskInfo4.getResourceUrls().get("41"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('42')", storedTaskInfo4.getResourceUrls().get("42"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('3')", storedTaskInfo4.getResourceUrls().get("3"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('4')", storedTaskInfo4.getResourceUrls().get("4"));

        // Test what Enabler Logic receives
        assertEquals(4, resultRef.get().getResources().size());
        assertEquals(2, resultRef.get().getResources().get(0).getResourceIds().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, resultRef.get().getResources().get(0).getStatus());
        assertEquals(3, resultRef.get().getResources().get(1).getResourceIds().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, resultRef.get().getResources().get(1).getStatus());
        assertEquals(4, resultRef.get().getResources().get(2).getResourceIds().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.NOT_ENOUGH_RESOURCES, resultRef.get().getResources().get(2).getStatus());
        assertEquals(4, resultRef.get().getResources().get(3).getResourceIds().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.NOT_ENOUGH_RESOURCES, resultRef.get().getResources().get(3).getStatus());

        assertEquals("resource1", resultRef.get().getResources().get(0).getResourceIds().get(0));
        assertEquals("resource2", resultRef.get().getResources().get(0).getResourceIds().get(1));
        assertEquals("21", resultRef.get().getResources().get(1).getResourceIds().get(0));
        assertEquals("22", resultRef.get().getResources().get(1).getResourceIds().get(1));
        assertEquals("3", resultRef.get().getResources().get(1).getResourceIds().get(2));
        assertEquals("31", resultRef.get().getResources().get(2).getResourceIds().get(0));
        assertEquals("32", resultRef.get().getResources().get(2).getResourceIds().get(1));
        assertEquals("3", resultRef.get().getResources().get(2).getResourceIds().get(2));
        assertEquals("4", resultRef.get().getResources().get(2).getResourceIds().get(3));
        assertEquals("41", resultRef.get().getResources().get(3).getResourceIds().get(0));
        assertEquals("42", resultRef.get().getResources().get(3).getResourceIds().get(1));
        assertEquals("3", resultRef.get().getResources().get(3).getResourceIds().get(2));
        assertEquals("4", resultRef.get().getResources().get(3).getResourceIds().get(3));

        while (dummyPlatformProxyListener.updateAcquisitionRequestsReceived() < 1) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(300);

        // Test what Platform Proxy receives

        taskUpdateRequestsReceivedByListener = dummyPlatformProxyListener.getUpdateAcquisitionRequestsReceivedByListener();

        assertEquals(1, dummyPlatformProxyListener.updateAcquisitionRequestsReceived());
        assertEquals(0, dummyPlatformProxyListener.startAcquisitionRequestsReceived());
        assertEquals(0, dummyPlatformProxyListener.cancelTaskRequestsReceived());

        for (PlatformProxyUpdateRequest request : taskUpdateRequestsReceivedByListener) {

            log.info("id = " + request.getTaskId());

            if (request.getTaskId().equals("2")) {
                Map<String, String> resourcesMap = new HashMap<>();
                resourcesMap.put("21", symbIoTeCoreUrl + "/Sensors('21')");
                resourcesMap.put("22", symbIoTeCoreUrl + "/Sensors('22')");
                resourcesMap.put("3", symbIoTeCoreUrl + "/Sensors('3')");

                assertEquals(3, request.getResources().size());
                assertEquals(resourcesMap.get(request.getResources().get(0).getResourceId()),
                        request.getResources().get(0).getAccessURL());
                assertEquals(resourcesMap.get(request.getResources().get(1).getResourceId()),
                        request.getResources().get(1).getAccessURL());
                assertEquals(resourcesMap.get(request.getResources().get(2).getResourceId()),
                        request.getResources().get(2).getAccessURL());
                continue;
            }

            fail("The code should not reach here, because no other tasks should be received by the platform proxy");
        }

        log.info("changeInMinNoResourcesTest FINISHED!");
    }
}