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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Arrays;
import java.util.List;
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
                .build();

        task1.setTaskId("1");
        task1.setMinNoResources(2);
        task1.setCoreQueryRequest(coreQueryRequest);
        task1.setResourceIds(Arrays.asList("resource1", "resource2"));
        task1.setQueryInterval_ms(60);
        task1.setAllowCaching(true);
        task1.setCachingInterval_ms(new Long(1000));
        task1.setInformPlatformProxy(true);
        task1.setStoredResourceIds(Arrays.asList("3", "4"));
        task1.setEnablerLogicName("enablerLogic");
        taskInfoRepository.save(task1);

        TaskInfo task2 = new TaskInfo(task1);
        task2.setTaskId("2");
        task2.setInformPlatformProxy(false); // Because we want to keep the same value in the updatedTask2
        task2.setResourceIds(Arrays.asList("21", "22"));
        taskInfoRepository.save(task2);

        TaskInfo task3 = new TaskInfo(task1);
        task3.setTaskId("3");
        task3.setResourceIds(Arrays.asList("31", "32"));
        taskInfoRepository.save(task3);

        TaskInfo task4 = new TaskInfo(task1);
        task4.setTaskId("4");
        task4.setInformPlatformProxy(false); // Because we want to keep the same value in the updatedTask4
        task4.setResourceIds(Arrays.asList("41", "42"));
        taskInfoRepository.save(task4);

        TaskInfo task5 = new TaskInfo(task1);
        task5.setTaskId("5");
        task5.setResourceIds(Arrays.asList("51", "52"));
        taskInfoRepository.save(task5);

        TaskInfo task6 = new TaskInfo(task1);
        task6.setTaskId("6");
        task6.setResourceIds(Arrays.asList("61", "62"));
        taskInfoRepository.save(task6);

        TaskInfo task7 = new TaskInfo(task1);
        task7.setTaskId("7");
        task7.setResourceIds(Arrays.asList("71", "72"));
        taskInfoRepository.save(task7);

        // This task should reach Platform Proxy and inform it for new resources
        TaskInfo updatedTask1 = new TaskInfo(task1);
        updatedTask1.getCoreQueryRequest().setLocation_name("Paris");

        // This task should not reach Platform Proxy, because InformPlatformProxy == false
        TaskInfo updatedTask2 = new TaskInfo(task2);
        updatedTask2.setInformPlatformProxy(false);

        // This task should not reach Platform Proxy, because there is nothing new to report
        TaskInfo updatedTask3 = new TaskInfo();
        updatedTask3.setTaskId("3");
        updatedTask3.setMinNoResources(null);
        updatedTask3.setCoreQueryRequest(null);
        updatedTask3.setAllowCaching(null);
        updatedTask3.setCachingInterval_ms(null);
        updatedTask3.setInformPlatformProxy(null);
        updatedTask3.setQueryInterval_ms(null);
        updatedTask3.setEnablerLogicName(null);
        updatedTask3.setResourceIds(Arrays.asList("31", "32"));
        updatedTask3.setStoredResourceIds(Arrays.asList("3", "4"));

        // This task should not reach Platform Proxy, because InformPlatformProxy == false
        TaskInfo updatedTask4 = new TaskInfo(task4);
        updatedTask4.setInformPlatformProxy(false);
        updatedTask4.setEnablerLogicName("updatedTask4");
        updatedTask4.setQueryInterval_ms(100);

        // This task should reach Platform Proxy, because InformPlatformProxy == true and the enablerLogic changed
        TaskInfo updatedTask5 = new TaskInfo(task5);
        updatedTask5.setEnablerLogicName("updatedTask5");

        // This task should reach Platform Proxy, because InformPlatformProxy == true and the query interval changed
        TaskInfo updatedTask6 = new TaskInfo(task6);
        updatedTask6.setQueryInterval_ms(100);

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

        future.addCallback(new ListenableFutureCallbackCustom("updateTask", resultRef));

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
                assertEquals(60, (int) request.getQueryInterval_ms());
                continue;
            }

            if (request.getTaskId().equals("5")) {
                assertEquals(null, request.getResources());
                assertEquals("updatedTask5", request.getEnablerLogicName());
                assertEquals(60, (int) request.getQueryInterval_ms());
                continue;
            }

            if (request.getTaskId().equals("6")) {
                assertEquals(null, request.getResources());
                assertEquals("enablerLogic", request.getEnablerLogicName());
                assertEquals(100, (int) request.getQueryInterval_ms());
                continue;
            }

            fail("The code should not reach here, because no other tasks should be received by the platform proxy");
        }

        log.info("updateTaskTest FINISHED!");
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
                .build();

        task1.setTaskId("1");
        task1.setMinNoResources(2);
        task1.setCoreQueryRequest(coreQueryRequest);
        task1.setResourceIds(Arrays.asList("resource1", "resource2"));
        task1.setQueryInterval_ms(60);
        task1.setAllowCaching(true);
        task1.setCachingInterval_ms(new Long(1000));
        task1.setInformPlatformProxy(true);
        task1.setStoredResourceIds(Arrays.asList("3", "4"));
        task1.setEnablerLogicName("enablerLogic");
        taskInfoRepository.save(task1);

        TaskInfo task2 = new TaskInfo(task1);
        task2.setTaskId("2");
        task2.setResourceIds(Arrays.asList("21", "22"));
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

        future.addCallback(new ListenableFutureCallbackCustom("updateTask", resultRef));

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

}