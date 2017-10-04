package eu.h2020.symbiote.enabler.resourcemanager.integration;

import eu.h2020.symbiote.core.internal.CoreQueryRequest;
import eu.h2020.symbiote.enabler.messaging.model.*;
import eu.h2020.symbiote.enabler.resourcemanager.model.ScheduledTaskInfoUpdate;
import eu.h2020.symbiote.enabler.resourcemanager.model.TaskInfo;
import eu.h2020.symbiote.enabler.resourcemanager.utils.ListenableFutureAcquisitionStartCallback;
import eu.h2020.symbiote.enabler.resourcemanager.utils.ListenableFutureCancelCallback;
import eu.h2020.symbiote.enabler.resourcemanager.utils.ListenableFutureUpdateCallback;

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
public class CachingTests extends AbstractTestClass {

    private static Log log = LogFactory
            .getLog(CachingTests.class);

    @Test
    public void createAndCancelCachingTasksTest() throws Exception {
        log.info("createAndCancelCachingTasksTest STARTED!");

        // Create the task

        final AtomicReference<ResourceManagerAcquisitionStartResponse> createResultRef = new AtomicReference<>();
        ResourceManagerAcquisitionStartRequest query = createValidQueryToResourceManager(2);
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


        //  Check if timer is activated

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


        // Cancel the Task

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


    @Test
    public void updateTaskWithAllowCachingTest() throws Exception {

        log.info("updateTaskWithAllowCachingTest STARTED!");

        final AtomicReference<ResourceManagerUpdateResponse> resultRef = new AtomicReference<>();
        List<PlatformProxyUpdateRequest> taskUpdateRequestsReceivedByListener;

        CoreQueryRequest coreQueryRequest = new CoreQueryRequest.Builder()
                .locationName("Athens")
                .observedProperty(Arrays.asList("temperature", "humidity"))
                .shouldRank(true)
                .build();

        List<String> resourceIds = Arrays.asList("resource1", "resource2");
        List<String> storedResourceIds = Arrays.asList("3", "4");

        Map<String, String> resourceUrls1 = new HashMap<>();
        resourceUrls1.put("resource1", symbIoTeCoreUrl + "/Sensors('resource1')");
        resourceUrls1.put("resource2", symbIoTeCoreUrl + "/Sensors('resource2')");

        TaskInfo task1 = new TaskInfo("1", 2, coreQueryRequest, "P0-0-0T0:0:0.06",
                true, "P0-0-0T0:0:1", true,
                "enablerLogic", null, resourceIds,
                ResourceManagerTaskInfoResponseStatus.SUCCESS, storedResourceIds, resourceUrls1);
        taskInfoRepository.save(task1);
        searchHelper.getScheduledTaskInfoUpdateMap().put(task1.getTaskId(), new ScheduledTaskInfoUpdate(taskInfoRepository,
                searchHelper, task1));

        Map<String, String> resourceUrls2 = new HashMap<>();
        resourceUrls2.put("21", symbIoTeCoreUrl + "/Sensors('21')");
        resourceUrls2.put("22", symbIoTeCoreUrl + "/Sensors('22')");
        TaskInfo task2 = new TaskInfo(task1);
        task2.setTaskId("2");
        task2.setResourceIds(Arrays.asList("21", "22"));
        task2.setResourceUrls(resourceUrls2);
        taskInfoRepository.save(task2);
        searchHelper.getScheduledTaskInfoUpdateMap().put(task2.getTaskId(), new ScheduledTaskInfoUpdate(taskInfoRepository,
                searchHelper, task2));

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

        // Check the stored ScheduledTaskInfoUpdates
        assertNotNull(searchHelper.getScheduledTaskInfoUpdateMap().get(task1.getTaskId()));
        assertNotNull(searchHelper.getScheduledTaskInfoUpdateMap().get(task2.getTaskId()));
        assertNull(searchHelper.getScheduledTaskInfoUpdateMap().get(task3.getTaskId()));
        assertNull(searchHelper.getScheduledTaskInfoUpdateMap().get(task4.getTaskId()));

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

        ResourceManagerUpdateRequest req = new ResourceManagerUpdateRequest();
        req.setTasks(Arrays.asList(new ResourceManagerTaskInfoRequest(updatedTask1),
                new ResourceManagerTaskInfoRequest(updatedTask2),
                new ResourceManagerTaskInfoRequest(updatedTask3),
                new ResourceManagerTaskInfoRequest(updatedTask4)));


        log.info("Before sending the message");
        RabbitConverterFuture<ResourceManagerUpdateResponse> future = asyncRabbitTemplate
                .convertSendAndReceive(resourceManagerExchangeName, updateTaskRoutingKey, req);
        log.info("After sending the message");

        future.addCallback(new ListenableFutureUpdateCallback("updateTaskWithAllowCachingBecomingFalseTest", resultRef));

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

        // Check the stored ScheduledTaskInfoUpdates
        assertNull(searchHelper.getScheduledTaskInfoUpdateMap().get(task1.getTaskId()));
        assertNull(searchHelper.getScheduledTaskInfoUpdateMap().get(task2.getTaskId()));
        assertNotNull(searchHelper.getScheduledTaskInfoUpdateMap().get(task3.getTaskId()));
        assertNotNull(searchHelper.getScheduledTaskInfoUpdateMap().get(task4.getTaskId()));

        // Test what Enabler Logic receives
        assertEquals(4, resultRef.get().getTasks().size());
        assertEquals(2, resultRef.get().getTasks().get(0).getResourceIds().size());
        assertEquals(2, resultRef.get().getTasks().get(1).getResourceIds().size());
        assertEquals(3, resultRef.get().getTasks().get(2).getResourceIds().size());
        assertEquals(2, resultRef.get().getTasks().get(3).getResourceIds().size());

        assertEquals(ResourceManagerTasksStatus.SUCCESS, resultRef.get().getStatus());
        assertEquals("ALL the update task requests were successful!", resultRef.get().getMessage());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, resultRef.get().getTasks().get(0).getStatus());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, resultRef.get().getTasks().get(1).getStatus());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, resultRef.get().getTasks().get(2).getStatus());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, resultRef.get().getTasks().get(3).getStatus());

        assertEquals("resource1", resultRef.get().getTasks().get(0).getResourceIds().get(0));
        assertEquals("resource2", resultRef.get().getTasks().get(0).getResourceIds().get(1));
        assertEquals("21", resultRef.get().getTasks().get(1).getResourceIds().get(0));
        assertEquals("22", resultRef.get().getTasks().get(1).getResourceIds().get(1));
        assertEquals("31", resultRef.get().getTasks().get(2).getResourceIds().get(0));
        assertEquals("32", resultRef.get().getTasks().get(2).getResourceIds().get(1));
        assertEquals("resource1", resultRef.get().getTasks().get(2).getResourceIds().get(2));
        assertEquals("resource1", resultRef.get().getTasks().get(3).getResourceIds().get(0));
        assertEquals("resource2", resultRef.get().getTasks().get(3).getResourceIds().get(1));

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

        boolean foundTask1 = false;
        boolean foundTask4 = false;

        for (PlatformProxyUpdateRequest request : taskUpdateRequestsReceivedByListener) {

            log.info("Task id = " + request.getTaskId());

            if (request.getTaskId().equals("1")) {
                assertEquals("resource1", request.getResources().get(0).getResourceId());
                assertEquals("resource2", request.getResources().get(1).getResourceId());
                foundTask1 = true;
                continue;
            }

            if (request.getTaskId().equals("4")) {
                assertEquals("resource1", request.getResources().get(0).getResourceId());
                assertEquals("resource2", request.getResources().get(1).getResourceId());
                foundTask4 = true;
                continue;
            }

            fail("The code should not reach here, because no other tasks should be received by the platform proxy");
        }

        assertEquals(true, foundTask1);
        assertEquals(true, foundTask4);


        // Check if timer is activated

        // Clear the storedResourceIds from task3 and task4
        storedTaskInfo3.getStoredResourceIds().clear();
        taskInfoRepository.save(storedTaskInfo3);
        storedTaskInfo4.getStoredResourceIds().clear();
        taskInfoRepository.save(storedTaskInfo4);

        // Wait a little more than 1 sec in order to check if stored resources are updated
        TimeUnit.MILLISECONDS.sleep(1200);

        storedTaskInfo3 = taskInfoRepository.findByTaskId("3");
        storedTaskInfo4 = taskInfoRepository.findByTaskId("4");

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

        // Test if stored resources were updated in the last 2 tasks
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

        // Check the stored ScheduledTaskInfoUpdates
        assertNull(searchHelper.getScheduledTaskInfoUpdateMap().get(task1.getTaskId()));
        assertNull(searchHelper.getScheduledTaskInfoUpdateMap().get(task2.getTaskId()));
        assertNotNull(searchHelper.getScheduledTaskInfoUpdateMap().get(task3.getTaskId()));
        assertNotNull(searchHelper.getScheduledTaskInfoUpdateMap().get(task4.getTaskId()));

        log.info("updateTaskWithAllowCachingTest FINISHED!");
    }

}