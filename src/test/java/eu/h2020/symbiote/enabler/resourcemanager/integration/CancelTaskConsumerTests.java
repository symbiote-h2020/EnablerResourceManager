package eu.h2020.symbiote.enabler.resourcemanager.integration;


import com.fasterxml.jackson.databind.ObjectMapper;

import eu.h2020.symbiote.core.internal.CoreQueryRequest;
import eu.h2020.symbiote.enabler.messaging.model.CancelTaskRequest;
import eu.h2020.symbiote.enabler.messaging.model.CancelTaskResponse;
import eu.h2020.symbiote.enabler.messaging.model.CancelTaskResponseStatus;
import eu.h2020.symbiote.enabler.messaging.model.ResourceManagerTaskInfoResponseStatus;
import eu.h2020.symbiote.enabler.resourcemanager.dummyListeners.DummyEnablerLogicListener;
import eu.h2020.symbiote.enabler.resourcemanager.dummyListeners.DummyPlatformProxyListener;
import eu.h2020.symbiote.enabler.resourcemanager.model.TaskInfo;
import eu.h2020.symbiote.enabler.resourcemanager.repository.TaskInfoRepository;
import eu.h2020.symbiote.enabler.resourcemanager.utils.ListenableFutureCancelCallback;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(webEnvironment = WebEnvironment.DEFINED_PORT,
        properties = {"eureka.client.enabled=false",
                "spring.sleuth.enabled=false",
                "symbiote.core.url=http://localhost:8080",
                "symbiote.coreaam.url=http://localhost:8080",
                "symbiote.enabler.rm.database=symbiote-enabler-rm-database-ctct",
                "rabbit.queueName.resourceManager.startDataAcquisition=symbIoTe-resourceManager-startDataAcquisition-ctct",
                "rabbit.queueName.resourceManager.cancelTask=symbIoTe-resourceManager-cancelTask-ctct",
                "rabbit.queueName.resourceManager.unavailableResources=symbIoTe-resourceManager-unavailableResources-ctct",
                "rabbit.queueName.resourceManager.wrongData=symbIoTe-resourceManager-wrongData-ctct",
                "rabbit.queueName.resourceManager.updateTask=symbIoTe-resourceManager-updateTask-ctct",
                "rabbit.queueName.pl.acquisitionStartRequested=symbIoTe-pl-acquisitionStartRequested-ctct",
                "rabbit.queueName.pl.taskUpdated=symbIoTe-pl-taskUpdated-ctct",
                "rabbit.queueName.pl.cancelTasks=symbIoTe-pl-cancelTasks-ctct",
                "rabbit.queueName.el.resourcesUpdated=symbIoTe-el-resourcesUpdated-ctct",
                "rabbit.queueName.el.notEnoughResources=symbIoTe-el-notEnoughResources-ctct"})
@ContextConfiguration
@Configuration
@ComponentScan
@EnableAutoConfiguration
@ActiveProfiles("test")
public class CancelTaskConsumerTests {

    private static Log log = LogFactory
            .getLog(CancelTaskConsumerTests.class);

    @Autowired
    private AsyncRabbitTemplate asyncRabbitTemplate;

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

    @Value("${rabbit.routingKey.resourceManager.cancelTask}")
    private String cancelTaskRoutingKey;

    @Value("${rabbit.routingKey.resourceManager.startDataAcquisition}")
    private String startDataAcquisitionRoutingKey;

    private ObjectMapper mapper = new ObjectMapper();

    // Execute the Setup method before the test.
    @Before
    public void setUp() throws Exception {
        dummyPlatformProxyListener.clearRequestsReceivedByListener();
        dummyEnablerLogicListener.clearRequestsReceivedByListener();
        taskInfoRepository.deleteAll();

    }

    @After
    public void clearSetup() throws Exception {
        taskInfoRepository.deleteAll();
    }

    @Test
    public void successfulCancelTaskTest() throws Exception {
        log.info("successfulCancelTaskTest STARTED!");

        final AtomicReference<CancelTaskResponse> resultRef = new AtomicReference<>();
        List<CancelTaskRequest> cancelTaskRequestArrayList = null;

        CoreQueryRequest coreQueryRequest = new CoreQueryRequest.Builder()
                .locationName("Zurich")
                .observedProperty(Arrays.asList("temperature", "humidity"))
                .build();

        List<String> resourceIds = Arrays.asList("1", "2");
        List<String> storedResourceIds = Arrays.asList("3", "4");

        Map<String, String> resourceUrls1 = new HashMap<>();
        resourceUrls1.put("resource1", symbIoTeCoreUrl + "/Sensors('resource1')");
        resourceUrls1.put("resource2", symbIoTeCoreUrl + "/Sensors('resource2')");

        TaskInfo task1 = new TaskInfo("1", 2, coreQueryRequest, "P0-0-0T0:0:0.06",
                true, "P0-0-0T0:0:1", true,
                "TestEnablerLogic", null, resourceIds,
                ResourceManagerTaskInfoResponseStatus.SUCCESS, storedResourceIds, resourceUrls1);
        taskInfoRepository.save(task1);

        TaskInfo task2 = new TaskInfo(task1);
        task2.setTaskId("2");
        task2.setInformPlatformProxy(false);
        taskInfoRepository.save(task2);

        TaskInfo task3 = new TaskInfo(task1);
        task3.setTaskId("3");
        taskInfoRepository.save(task3);


        // Test what is stored in the database
        TaskInfo taskInfo = taskInfoRepository.findByTaskId("1");
        assertEquals(2, taskInfo.getResourceIds().size());

        taskInfo = taskInfoRepository.findByTaskId("2");
        assertEquals(false, taskInfo.getInformPlatformProxy());

        taskInfo = taskInfoRepository.findByTaskId("3");
        assertEquals(true, taskInfo.getInformPlatformProxy());

        CancelTaskRequest cancelTaskRequest = new CancelTaskRequest();
        cancelTaskRequest.setTaskIdList(Arrays.asList("1", "2", "3"));

        log.info("Before sending the message");
        RabbitConverterFuture<CancelTaskResponse> future = asyncRabbitTemplate
                .convertSendAndReceive(resourceManagerExchangeName, cancelTaskRoutingKey, cancelTaskRequest);
        log.info("After sending the message");

        future.addCallback(new ListenableFutureCancelCallback("successfulCancelTaskTest", resultRef));

        while(!future.isDone()) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(100);

        // Test what Enabler Logic receives
        assertEquals(CancelTaskResponseStatus.SUCCESS, resultRef.get().getStatus());
        assertEquals("ALL tasks were deleted!", resultRef.get().getMessage());

        // Test the database
        taskInfo = taskInfoRepository.findByTaskId("1");
        assertEquals(null, taskInfo);

        taskInfo = taskInfoRepository.findByTaskId("2");
        assertEquals(null, taskInfo);

        taskInfo = taskInfoRepository.findByTaskId("3");
        assertEquals(null, taskInfo);

        taskInfo = taskInfoRepository.findByTaskId("4");
        assertEquals(null, taskInfo);

        while (dummyPlatformProxyListener.cancelTaskRequestsReceived() == 0) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(100);


        cancelTaskRequestArrayList = dummyPlatformProxyListener.getCancelTaskRequestsReceivedByListener();
        assertEquals(1, cancelTaskRequestArrayList.size());
        assertEquals(2, cancelTaskRequestArrayList.get(0).getTaskIdList().size());
        assertEquals("1", cancelTaskRequestArrayList.get(0).getTaskIdList().get(0));
        assertEquals("3", cancelTaskRequestArrayList.get(0).getTaskIdList().get(1));

        log.info("successfulCancelTaskTest FINISHED!");
    }

    @Test
    public void cancelTaskWithNonExistentTasksTest() throws Exception {
        log.info("cancelTaskWithNonExistentTasksTest STARTED!");

        final AtomicReference<CancelTaskResponse> resultRef = new AtomicReference<>();
        List<CancelTaskRequest> cancelTaskRequestArrayList = null;

        CoreQueryRequest coreQueryRequest = new CoreQueryRequest.Builder()
                .locationName("Zurich")
                .observedProperty(Arrays.asList("temperature", "humidity"))
                .build();

        List<String> resourceIds = Arrays.asList("1", "2");
        List<String> storedResourceIds = Arrays.asList("3", "4");

        Map<String, String> resourceUrls1 = new HashMap<>();
        resourceUrls1.put("resource1", symbIoTeCoreUrl + "/Sensors('resource1')");
        resourceUrls1.put("resource2", symbIoTeCoreUrl + "/Sensors('resource2')");

        TaskInfo task1 = new TaskInfo("1", 2, coreQueryRequest, "P0-0-0T0:0:0.06",
                true, "P0-0-0T0:0:1", true,
                "TestEnablerLogic", null, resourceIds,
                ResourceManagerTaskInfoResponseStatus.SUCCESS, storedResourceIds, resourceUrls1);
        taskInfoRepository.save(task1);

        TaskInfo task2 = new TaskInfo(task1);
        task2.setTaskId("2");
        task2.setInformPlatformProxy(false);
        taskInfoRepository.save(task2);

        TaskInfo task4 = new TaskInfo(task1);
        task4.setTaskId("4");
        taskInfoRepository.save(task4);


        // Test what is stored in the database
        TaskInfo taskInfo = taskInfoRepository.findByTaskId("1");
        assertEquals(2, taskInfo.getResourceIds().size());

        taskInfo = taskInfoRepository.findByTaskId("2");
        assertEquals(false, taskInfo.getInformPlatformProxy());

        taskInfo = taskInfoRepository.findByTaskId("3");
        assertEquals(null, taskInfo);

        taskInfo = taskInfoRepository.findByTaskId("4");
        assertEquals(true, taskInfo.getInformPlatformProxy());

        CancelTaskRequest cancelTaskRequest = new CancelTaskRequest();
        cancelTaskRequest.setTaskIdList(Arrays.asList("1", "2", "3", "4"));

        log.info("Before sending the message");
        RabbitConverterFuture<CancelTaskResponse> future = asyncRabbitTemplate
                .convertSendAndReceive(resourceManagerExchangeName, cancelTaskRoutingKey, cancelTaskRequest);
        log.info("After sending the message");

        future.addCallback(new ListenableFutureCancelCallback("cancelTaskWithNonExistentTasksTest", resultRef));

        while(!future.isDone()) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(100);

        // Test what Enabler Logic receives
        assertEquals(CancelTaskResponseStatus.NOT_ALL_TASKS_EXIST, resultRef.get().getStatus());
        assertEquals("Non-existent task ids : [3]", resultRef.get().getMessage());

        // Test the database
        taskInfo = taskInfoRepository.findByTaskId("1");
        assertEquals(null, taskInfo);

        taskInfo = taskInfoRepository.findByTaskId("2");
        assertEquals(null, taskInfo);

        taskInfo = taskInfoRepository.findByTaskId("3");
        assertEquals(null, taskInfo);

        taskInfo = taskInfoRepository.findByTaskId("4");
        assertEquals(null, taskInfo);

        while (dummyPlatformProxyListener.cancelTaskRequestsReceived() == 0) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(100);


        cancelTaskRequestArrayList = dummyPlatformProxyListener.getCancelTaskRequestsReceivedByListener();
        assertEquals(1, cancelTaskRequestArrayList.size());
        assertEquals(2, cancelTaskRequestArrayList.get(0).getTaskIdList().size());
        assertEquals("1", cancelTaskRequestArrayList.get(0).getTaskIdList().get(0));
        assertEquals("4", cancelTaskRequestArrayList.get(0).getTaskIdList().get(1));

        log.info("cancelTaskWithNonExistentTasksTest FINISHED!");
    }

    @Test
    public void cancelTaskEmptyResponseListTest() throws Exception {
        log.info("cancelTaskEmptyResponseListTest STARTED!");

        final AtomicReference<CancelTaskResponse> resultRef = new AtomicReference<>();
        List<CancelTaskRequest> cancelTaskRequestArrayList = null;

        CoreQueryRequest coreQueryRequest = new CoreQueryRequest.Builder()
                .locationName("Zurich")
                .observedProperty(Arrays.asList("temperature", "humidity"))
                .build();

        List<String> resourceIds = Arrays.asList("1", "2");
        List<String> storedResourceIds = Arrays.asList("3", "4");

        Map<String, String> resourceUrls1 = new HashMap<>();
        resourceUrls1.put("resource1", symbIoTeCoreUrl + "/Sensors('resource1')");
        resourceUrls1.put("resource2", symbIoTeCoreUrl + "/Sensors('resource2')");

        TaskInfo task1 = new TaskInfo("1", 2, coreQueryRequest, "P0-0-0T0:0:0.06",
                true, "P0-0-0T0:0:1", true,
                "TestEnablerLogic", null, resourceIds,
                ResourceManagerTaskInfoResponseStatus.SUCCESS, storedResourceIds, resourceUrls1);
        taskInfoRepository.save(task1);

        TaskInfo task2 = new TaskInfo(task1);
        task2.setTaskId("2");
        task2.setInformPlatformProxy(false);
        taskInfoRepository.save(task2);

        TaskInfo task4 = new TaskInfo(task1);
        task4.setTaskId("4");
        taskInfoRepository.save(task4);


        // Test what is stored in the database
        TaskInfo taskInfo = taskInfoRepository.findByTaskId("1");
        assertEquals(2, taskInfo.getResourceIds().size());

        taskInfo = taskInfoRepository.findByTaskId("2");
        assertEquals(false, taskInfo.getInformPlatformProxy());

        taskInfo = taskInfoRepository.findByTaskId("3");
        assertEquals(null, taskInfo);

        taskInfo = taskInfoRepository.findByTaskId("4");
        assertEquals(true, taskInfo.getInformPlatformProxy());

        CancelTaskRequest cancelTaskRequest = new CancelTaskRequest();
        cancelTaskRequest.setTaskIdList(Arrays.asList("2", "3"));

        log.info("Before sending the message");
        RabbitConverterFuture<CancelTaskResponse> future = asyncRabbitTemplate
                .convertSendAndReceive(resourceManagerExchangeName, cancelTaskRoutingKey, cancelTaskRequest);
        log.info("After sending the message");

        future.addCallback(new ListenableFutureCancelCallback("cancelTaskEmptyResponseListTest", resultRef));

        while(!future.isDone()) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(100);

        // Test what Enabler Logic receives
        assertEquals(CancelTaskResponseStatus.NOT_ALL_TASKS_EXIST, resultRef.get().getStatus());
        assertEquals("Non-existent task ids : [3]", resultRef.get().getMessage());

        // Test the database
        taskInfo = taskInfoRepository.findByTaskId("1");
        assertEquals(2, taskInfo.getResourceIds().size());

        taskInfo = taskInfoRepository.findByTaskId("2");
        assertEquals(null, taskInfo);

        taskInfo = taskInfoRepository.findByTaskId("3");
        assertEquals(null, taskInfo);

        taskInfo = taskInfoRepository.findByTaskId("4");
        assertEquals(true, taskInfo.getInformPlatformProxy());

        cancelTaskRequestArrayList = dummyPlatformProxyListener.getCancelTaskRequestsReceivedByListener();
        assertEquals(0, cancelTaskRequestArrayList.size());

        log.info("cancelTaskEmptyResponseListTest FINISHED!");
    }

    @Test
    public void deserializationErrorTest() throws Exception {
        log.info("deserializationErrorTest STARTED!");

        final AtomicReference<CancelTaskResponse> resultRef = new AtomicReference<>();

        log.info("Before sending the message");
        RabbitConverterFuture<CancelTaskResponse> future = asyncRabbitTemplate
                .convertSendAndReceive(resourceManagerExchangeName, cancelTaskRoutingKey, new CancelTaskResponse());
        log.info("After sending the message");

        future.addCallback(new ListenableFutureCancelCallback("deserializationErrorTest", resultRef));

        while(!future.isDone()) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(100);

        // Test what Enabler Logic receives
        assertEquals(CancelTaskResponseStatus.FAILED, resultRef.get().getStatus());

        log.info("deserializationErrorTest FINISHED!");
    }
}