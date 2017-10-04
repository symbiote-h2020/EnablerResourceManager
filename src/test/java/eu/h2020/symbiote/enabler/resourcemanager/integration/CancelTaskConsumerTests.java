package eu.h2020.symbiote.enabler.resourcemanager.integration;


import eu.h2020.symbiote.core.internal.CoreQueryRequest;
import eu.h2020.symbiote.enabler.messaging.model.CancelTaskRequest;
import eu.h2020.symbiote.enabler.messaging.model.CancelTaskResponse;
import eu.h2020.symbiote.enabler.messaging.model.CancelTaskResponseStatus;
import eu.h2020.symbiote.enabler.messaging.model.ResourceManagerTaskInfoResponseStatus;
import eu.h2020.symbiote.enabler.resourcemanager.model.ScheduledTaskInfoUpdate;
import eu.h2020.symbiote.enabler.resourcemanager.model.TaskInfo;
import eu.h2020.symbiote.enabler.resourcemanager.utils.ListenableFutureCancelCallback;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.junit.Test;
import org.springframework.amqp.rabbit.AsyncRabbitTemplate.RabbitConverterFuture;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;


@EnableAutoConfiguration
public class CancelTaskConsumerTests extends AbstractTestClass {

    private static Log log = LogFactory
            .getLog(CancelTaskConsumerTests.class);


    @Test
    public void successfulCancelTaskTest() throws Exception {
        log.info("successfulCancelTaskTest STARTED!");

        final AtomicReference<CancelTaskResponse> resultRef = new AtomicReference<>();
        List<CancelTaskRequest> cancelTaskRequestArrayList;

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
                true, "P0-0-0T1:0:1", true,
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

        searchHelper.getScheduledTaskInfoUpdateMap().put(task1.getTaskId(), new ScheduledTaskInfoUpdate(taskInfoRepository,
                searchHelper, task1));
        searchHelper.getScheduledTaskInfoUpdateMap().put(task2.getTaskId(), new ScheduledTaskInfoUpdate(taskInfoRepository,
                searchHelper, task2));
        searchHelper.getScheduledTaskInfoUpdateMap().put(task3.getTaskId(), new ScheduledTaskInfoUpdate(taskInfoRepository,
                searchHelper, task3));

        // Test what is stored in the database
        TaskInfo taskInfo = taskInfoRepository.findByTaskId("1");
        assertEquals(2, taskInfo.getResourceIds().size());

        taskInfo = taskInfoRepository.findByTaskId("2");
        assertEquals(false, taskInfo.getInformPlatformProxy());

        taskInfo = taskInfoRepository.findByTaskId("3");
        assertEquals(true, taskInfo.getInformPlatformProxy());

        CancelTaskRequest cancelTaskRequest = new CancelTaskRequest();
        cancelTaskRequest.setTaskIdList(Arrays.asList("1", "2", "3"));

        // Check the stored ScheduledTaskInfoUpdates
        assertNotNull(searchHelper.getScheduledTaskInfoUpdateMap().get(task1.getTaskId()));
        assertNotNull(searchHelper.getScheduledTaskInfoUpdateMap().get(task2.getTaskId()));
        assertNotNull(searchHelper.getScheduledTaskInfoUpdateMap().get(task3.getTaskId()));

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

        // Check the stored ScheduledTaskInfoUpdates
        assertNull(searchHelper.getScheduledTaskInfoUpdateMap().get(task1.getTaskId()));
        assertNull(searchHelper.getScheduledTaskInfoUpdateMap().get(task2.getTaskId()));
        assertNull(searchHelper.getScheduledTaskInfoUpdateMap().get(task3.getTaskId()));

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
        List<CancelTaskRequest> cancelTaskRequestArrayList;

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
        List<CancelTaskRequest> cancelTaskRequestArrayList;

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