package eu.h2020.symbiote.enabler.resourcemanager.integration.tests;


import eu.h2020.symbiote.core.ci.QueryResourceResult;
import eu.h2020.symbiote.core.ci.SparqlQueryOutputFormat;
import eu.h2020.symbiote.core.ci.SparqlQueryRequest;
import eu.h2020.symbiote.core.internal.CoreQueryRequest;
import eu.h2020.symbiote.enabler.messaging.model.*;
import eu.h2020.symbiote.enabler.resourcemanager.integration.AbstractTestClass;
import eu.h2020.symbiote.enabler.resourcemanager.model.ScheduledTaskInfoUpdate;
import eu.h2020.symbiote.enabler.resourcemanager.model.TaskInfo;
import eu.h2020.symbiote.enabler.resourcemanager.integration.callbacks.ListenableFutureUpdateCallback;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.junit.Test;

import org.springframework.amqp.rabbit.AsyncRabbitTemplate.RabbitConverterFuture;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;


@EnableAutoConfiguration
public class UpdateTaskConsumerTests extends AbstractTestClass {

    private static Log log = LogFactory
            .getLog(UpdateTaskConsumerTests.class);


    @Test
    public void updateTaskTest() throws Exception {
        // In this test, the value of informPlatformProxy remains the same in all the tasks

        log.info("updateTaskTest STARTED!");

        final AtomicReference<ResourceManagerUpdateResponse> resultRef = new AtomicReference<>();
        List<PlatformProxyUpdateRequest> taskUpdateRequestsReceivedByListener;

        CoreQueryRequest coreQueryRequest = new CoreQueryRequest.Builder()
                .locationName("Zurich")
                .observedProperty(Arrays.asList("temperature", "humidity"))
                .shouldRank(true)
                .build();

        List<String> resourceIds = Arrays.asList("resource1", "resource2");
        List<String> storedResourceIds = Arrays.asList("3", "4");

        Map<String, String> resourceUrls1 = new HashMap<>();
        resourceUrls1.put("resource1", symbIoTeCoreUrl + "/Sensors('resource1')");
        resourceUrls1.put("resource2", symbIoTeCoreUrl + "/Sensors('resource2')");

        ArrayList<QueryResourceResult> results = new ArrayList<>();
        QueryResourceResult result1 = new QueryResourceResult();
        QueryResourceResult result2 = new QueryResourceResult();
        result1.setId("resource1");
        result2.setId("resource2");
        results.add(result1);
        results.add(result2);

        TaskInfo task1 = new TaskInfo("1", 2, coreQueryRequest, "P0-0-0T0:0:0.06",
                true, "P0-0-0T0:0:1", true, "enablerLogic", null, resourceIds, results,
                ResourceManagerTaskInfoResponseStatus.SUCCESS, storedResourceIds, resourceUrls1, "SUCCESS");
        task1.setMaxNoResources(2);
        taskInfoRepository.save(task1);

        Map<String, String> resourceUrls2 = new HashMap<>();
        resourceUrls2.put("21", symbIoTeCoreUrl + "/Sensors('21')");
        resourceUrls2.put("22", symbIoTeCoreUrl + "/Sensors('22')");
        TaskInfo task2 = new TaskInfo(task1);
        task2.setTaskId("2");
        task2.setInformPlatformProxy(false); // Because we want to keep the same value in the updatedTask2
        task2.setResourceIds(Arrays.asList("21", "22"));
        task2.setResourceUrls(resourceUrls2);
        task2.getResourceDescriptions().get(0).setId("21");
        task2.getResourceDescriptions().get(1).setId("22");
        taskInfoRepository.save(task2);

        Map<String, String> resourceUrls3 = new HashMap<>();
        resourceUrls3.put("31", symbIoTeCoreUrl + "/Sensors('31')");
        resourceUrls3.put("32", symbIoTeCoreUrl + "/Sensors('32')");
        TaskInfo task3 = new TaskInfo(task1);
        task3.setTaskId("3");
        task3.setResourceIds(Arrays.asList("31", "32"));
        task3.setResourceUrls(resourceUrls3);
        task3.getResourceDescriptions().get(0).setId("31");
        task3.getResourceDescriptions().get(1).setId("32");
        taskInfoRepository.save(task3);

        Map<String, String> resourceUrls4 = new HashMap<>();
        resourceUrls4.put("41", symbIoTeCoreUrl + "/Sensors('41')");
        resourceUrls4.put("42", symbIoTeCoreUrl + "/Sensors('42')");
        TaskInfo task4 = new TaskInfo(task1);
        task4.setTaskId("4");
        task4.setInformPlatformProxy(false); // Because we want to keep the same value in the updatedTask4
        task4.setResourceIds(Arrays.asList("41", "42"));
        task4.setResourceUrls(resourceUrls4);
        task4.getResourceDescriptions().get(0).setId("41");
        task4.getResourceDescriptions().get(1).setId("42");
        taskInfoRepository.save(task4);

        Map<String, String> resourceUrls5 = new HashMap<>();
        resourceUrls5.put("51", symbIoTeCoreUrl + "/Sensors('51')");
        resourceUrls5.put("52", symbIoTeCoreUrl + "/Sensors('52')");
        TaskInfo task5 = new TaskInfo(task1);
        task5.setTaskId("5");
        task5.setResourceIds(Arrays.asList("51", "52"));
        task5.setResourceUrls(resourceUrls5);
        task5.getResourceDescriptions().get(0).setId("51");
        task5.getResourceDescriptions().get(1).setId("52");
        taskInfoRepository.save(task5);

        Map<String, String> resourceUrls6 = new HashMap<>();
        resourceUrls6.put("61", symbIoTeCoreUrl + "/Sensors('61')");
        resourceUrls6.put("62", symbIoTeCoreUrl + "/Sensors('62')");
        TaskInfo task6 = new TaskInfo(task1);
        task6.setTaskId("6");
        task6.setResourceIds(Arrays.asList("61", "62"));
        task6.setResourceUrls(resourceUrls6);
        task6.getResourceDescriptions().get(0).setId("61");
        task6.getResourceDescriptions().get(1).setId("62");
        taskInfoRepository.save(task6);

        // This task should reach Platform Proxy and inform it for new resources
        TaskInfo updatedTask1 = new TaskInfo(task1);
        updatedTask1.getCoreQueryRequest().setLocation_name("Paris");

        // This task should not reach Platform Proxy, because InformPlatformProxy == false
        TaskInfo updatedTask2 = new TaskInfo(task2);

        // This task should not reach Platform Proxy, because there is nothing new to report
        TaskInfo updatedTask3 = new TaskInfo(task3);

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

        ResourceManagerUpdateRequest req = new ResourceManagerUpdateRequest();
        req.setTasks(Arrays.asList(new ResourceManagerTaskInfoRequest(updatedTask1),
                new ResourceManagerTaskInfoRequest(updatedTask2),
                new ResourceManagerTaskInfoRequest(updatedTask3),
                new ResourceManagerTaskInfoRequest(updatedTask4),
                new ResourceManagerTaskInfoRequest(updatedTask5),
                new ResourceManagerTaskInfoRequest(updatedTask6)));


        log.info("Before sending the message");
        RabbitConverterFuture<ResourceManagerUpdateResponse> future = asyncRabbitTemplate
                .convertSendAndReceive(resourceManagerExchangeName, updateTaskRoutingKey, req);
        log.info("After sending the message");

        future.addCallback(new ListenableFutureUpdateCallback("updateTaskTest", resultRef));

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
        assertNotEquals(storedTaskInfo1, task1);
        assertEquals(storedTaskInfo2, task2);
        assertEquals(storedTaskInfo3, task3);
        assertNotEquals(storedTaskInfo4, task4);
        assertNotEquals(storedTaskInfo5, task5);
        assertNotEquals(storedTaskInfo6, task6);

        // Only updatedTask1.equals(storedTaskInfo1), because it has modified resources
        assertNotEquals(storedTaskInfo1, updatedTask1);
        assertEquals(storedTaskInfo2, updatedTask2);
        assertEquals(storedTaskInfo3, updatedTask3);
        assertEquals(storedTaskInfo4, updatedTask4);
        assertEquals(storedTaskInfo5, updatedTask5);
        assertEquals(storedTaskInfo6, updatedTask6);

        // Test what is stored in the database
        assertEquals(2, storedTaskInfo1.getResourceIds().size());
        assertEquals(2, storedTaskInfo1.getResourceDescriptions().size());
        assertEquals(1, storedTaskInfo1.getStoredResourceIds().size());
        assertEquals(2, storedTaskInfo1.getResourceUrls().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, storedTaskInfo1.getStatus());
        assertEquals("SUCCESS", storedTaskInfo1.getMessage());
        assertEquals("resource1", storedTaskInfo1.getResourceIds().get(0));
        assertEquals("resource2", storedTaskInfo1.getResourceIds().get(1));
        assertEquals("resource1", storedTaskInfo1.getResourceDescriptions().get(0).getId());
        assertEquals("resource2", storedTaskInfo1.getResourceDescriptions().get(1).getId());
        assertEquals("resource3", storedTaskInfo1.getStoredResourceIds().get(0));
        assertEquals(symbIoTeCoreUrl + "/Sensors('resource1')", storedTaskInfo1.getResourceUrls().get("resource1"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('resource2')", storedTaskInfo1.getResourceUrls().get("resource2"));

        assertEquals(2, storedTaskInfo2.getResourceIds().size());
        assertEquals(2, storedTaskInfo2.getResourceDescriptions().size());
        assertEquals(2, storedTaskInfo2.getStoredResourceIds().size());
        assertEquals(2, storedTaskInfo2.getResourceUrls().size());
        assertEquals("SUCCESS", storedTaskInfo2.getMessage());
        assertEquals("21", storedTaskInfo2.getResourceIds().get(0));
        assertEquals("22", storedTaskInfo2.getResourceIds().get(1));
        assertEquals("21", storedTaskInfo2.getResourceDescriptions().get(0).getId());
        assertEquals("22", storedTaskInfo2.getResourceDescriptions().get(1).getId());
        assertEquals("3", storedTaskInfo2.getStoredResourceIds().get(0));
        assertEquals("4", storedTaskInfo2.getStoredResourceIds().get(1));
        assertEquals(symbIoTeCoreUrl + "/Sensors('21')", storedTaskInfo2.getResourceUrls().get("21"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('22')", storedTaskInfo2.getResourceUrls().get("22"));

        assertEquals(2, storedTaskInfo3.getResourceIds().size());
        assertEquals(2, storedTaskInfo3.getResourceDescriptions().size());
        assertEquals(2, storedTaskInfo3.getStoredResourceIds().size());
        assertEquals(2, storedTaskInfo3.getResourceUrls().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, storedTaskInfo3.getStatus());
        assertEquals("SUCCESS", storedTaskInfo3.getMessage());
        assertEquals("31", storedTaskInfo3.getResourceIds().get(0));
        assertEquals("32", storedTaskInfo3.getResourceIds().get(1));
        assertEquals("31", storedTaskInfo3.getResourceDescriptions().get(0).getId());
        assertEquals("32", storedTaskInfo3.getResourceDescriptions().get(1).getId());
        assertEquals("3", storedTaskInfo3.getStoredResourceIds().get(0));
        assertEquals("4", storedTaskInfo3.getStoredResourceIds().get(1));
        assertEquals(symbIoTeCoreUrl + "/Sensors('31')", storedTaskInfo3.getResourceUrls().get("31"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('32')", storedTaskInfo3.getResourceUrls().get("32"));

        assertEquals(2, storedTaskInfo4.getResourceIds().size());
        assertEquals(2, storedTaskInfo4.getResourceDescriptions().size());
        assertEquals(2, storedTaskInfo4.getStoredResourceIds().size());
        assertEquals(2, storedTaskInfo4.getResourceUrls().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, storedTaskInfo4.getStatus());
        assertEquals("SUCCESS", storedTaskInfo4.getMessage());
        assertEquals("41", storedTaskInfo4.getResourceIds().get(0));
        assertEquals("42", storedTaskInfo4.getResourceIds().get(1));
        assertEquals("41", storedTaskInfo4.getResourceDescriptions().get(0).getId());
        assertEquals("42", storedTaskInfo4.getResourceDescriptions().get(1).getId());
        assertEquals("3", storedTaskInfo4.getStoredResourceIds().get(0));
        assertEquals("4", storedTaskInfo4.getStoredResourceIds().get(1));
        assertEquals(symbIoTeCoreUrl + "/Sensors('41')", storedTaskInfo4.getResourceUrls().get("41"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('42')", storedTaskInfo4.getResourceUrls().get("42"));

        assertEquals(2, storedTaskInfo5.getResourceIds().size());
        assertEquals(2, storedTaskInfo5.getResourceDescriptions().size());
        assertEquals(2, storedTaskInfo5.getStoredResourceIds().size());
        assertEquals(2, storedTaskInfo5.getResourceUrls().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, storedTaskInfo5.getStatus());
        assertEquals("SUCCESS", storedTaskInfo5.getMessage());
        assertEquals("51", storedTaskInfo5.getResourceIds().get(0));
        assertEquals("52", storedTaskInfo5.getResourceIds().get(1));
        assertEquals("51", storedTaskInfo5.getResourceDescriptions().get(0).getId());
        assertEquals("52", storedTaskInfo5.getResourceDescriptions().get(1).getId());
        assertEquals("3", storedTaskInfo5.getStoredResourceIds().get(0));
        assertEquals("4", storedTaskInfo5.getStoredResourceIds().get(1));
        assertEquals(symbIoTeCoreUrl + "/Sensors('51')", storedTaskInfo5.getResourceUrls().get("51"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('52')", storedTaskInfo5.getResourceUrls().get("52"));

        assertEquals(2, storedTaskInfo6.getResourceIds().size());
        assertEquals(2, storedTaskInfo6.getResourceDescriptions().size());
        assertEquals(2, storedTaskInfo6.getStoredResourceIds().size());
        assertEquals(2, storedTaskInfo6.getResourceUrls().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, storedTaskInfo6.getStatus());
        assertEquals("SUCCESS", storedTaskInfo6.getMessage());
        assertEquals("61", storedTaskInfo6.getResourceIds().get(0));
        assertEquals("62", storedTaskInfo6.getResourceIds().get(1));
        assertEquals("61", storedTaskInfo6.getResourceDescriptions().get(0).getId());
        assertEquals("62", storedTaskInfo6.getResourceDescriptions().get(1).getId());
        assertEquals("3", storedTaskInfo6.getStoredResourceIds().get(0));
        assertEquals("4", storedTaskInfo6.getStoredResourceIds().get(1));
        assertEquals(symbIoTeCoreUrl + "/Sensors('61')", storedTaskInfo6.getResourceUrls().get("61"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('62')", storedTaskInfo6.getResourceUrls().get("62"));

        // Test what Enabler Logic receives
        assertEquals(6, resultRef.get().getTasks().size());
        assertEquals(2, resultRef.get().getTasks().get(0).getResourceIds().size());
        assertEquals(2, resultRef.get().getTasks().get(1).getResourceIds().size());
        assertEquals(2, resultRef.get().getTasks().get(2).getResourceIds().size());
        assertEquals(2, resultRef.get().getTasks().get(3).getResourceIds().size());
        assertEquals(2, resultRef.get().getTasks().get(4).getResourceIds().size());
        assertEquals(2, resultRef.get().getTasks().get(5).getResourceIds().size());

        assertEquals(2, resultRef.get().getTasks().get(0).getResourceDescriptions().size());
        assertEquals(2, resultRef.get().getTasks().get(1).getResourceDescriptions().size());
        assertEquals(2, resultRef.get().getTasks().get(2).getResourceDescriptions().size());
        assertEquals(2, resultRef.get().getTasks().get(3).getResourceDescriptions().size());
        assertEquals(2, resultRef.get().getTasks().get(4).getResourceDescriptions().size());
        assertEquals(2, resultRef.get().getTasks().get(5).getResourceDescriptions().size());

        assertEquals(ResourceManagerTasksStatus.SUCCESS, resultRef.get().getStatus());
        assertEquals("ALL the update task requests were successful!", resultRef.get().getMessage());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, resultRef.get().getTasks().get(0).getStatus());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, resultRef.get().getTasks().get(1).getStatus());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, resultRef.get().getTasks().get(2).getStatus());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, resultRef.get().getTasks().get(3).getStatus());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, resultRef.get().getTasks().get(4).getStatus());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, resultRef.get().getTasks().get(5).getStatus());
        assertEquals("SUCCESS", resultRef.get().getTasks().get(0).getMessage());
        assertEquals("SUCCESS", resultRef.get().getTasks().get(1).getMessage());
        assertEquals("SUCCESS", resultRef.get().getTasks().get(2).getMessage());
        assertEquals("SUCCESS", resultRef.get().getTasks().get(3).getMessage());
        assertEquals("SUCCESS", resultRef.get().getTasks().get(4).getMessage());
        assertEquals("SUCCESS", resultRef.get().getTasks().get(5).getMessage());

        assertEquals("resource1", resultRef.get().getTasks().get(0).getResourceIds().get(0));
        assertEquals("resource2", resultRef.get().getTasks().get(0).getResourceIds().get(1));
        assertEquals("21", resultRef.get().getTasks().get(1).getResourceIds().get(0));
        assertEquals("22", resultRef.get().getTasks().get(1).getResourceIds().get(1));
        assertEquals("31", resultRef.get().getTasks().get(2).getResourceIds().get(0));
        assertEquals("32", resultRef.get().getTasks().get(2).getResourceIds().get(1));
        assertEquals("41", resultRef.get().getTasks().get(3).getResourceIds().get(0));
        assertEquals("42", resultRef.get().getTasks().get(3).getResourceIds().get(1));
        assertEquals("51", resultRef.get().getTasks().get(4).getResourceIds().get(0));
        assertEquals("52", resultRef.get().getTasks().get(4).getResourceIds().get(1));
        assertEquals("61", resultRef.get().getTasks().get(5).getResourceIds().get(0));
        assertEquals("62", resultRef.get().getTasks().get(5).getResourceIds().get(1));

        assertEquals("resource1", resultRef.get().getTasks().get(0).getResourceDescriptions().get(0).getId());
        assertEquals("resource2", resultRef.get().getTasks().get(0).getResourceDescriptions().get(1).getId());
        assertEquals("21", resultRef.get().getTasks().get(1).getResourceDescriptions().get(0).getId());
        assertEquals("22", resultRef.get().getTasks().get(1).getResourceDescriptions().get(1).getId());
        assertEquals("31", resultRef.get().getTasks().get(2).getResourceDescriptions().get(0).getId());
        assertEquals("32", resultRef.get().getTasks().get(2).getResourceDescriptions().get(1).getId());
        assertEquals("41", resultRef.get().getTasks().get(3).getResourceDescriptions().get(0).getId());
        assertEquals("42", resultRef.get().getTasks().get(3).getResourceDescriptions().get(1).getId());
        assertEquals("51", resultRef.get().getTasks().get(4).getResourceDescriptions().get(0).getId());
        assertEquals("52", resultRef.get().getTasks().get(4).getResourceDescriptions().get(1).getId());
        assertEquals("61", resultRef.get().getTasks().get(5).getResourceDescriptions().get(0).getId());
        assertEquals("62", resultRef.get().getTasks().get(5).getResourceDescriptions().get(1).getId());

        while(dummyPlatformProxyListener.updateAcquisitionRequestsReceived() < 3) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(300);

        // Test what Platform Proxy receives
        taskUpdateRequestsReceivedByListener = dummyPlatformProxyListener.getUpdateAcquisitionRequestsReceivedByListener();

        assertEquals(3, dummyPlatformProxyListener.updateAcquisitionRequestsReceived());
        assertEquals(0, dummyPlatformProxyListener.startAcquisitionRequestsReceived());
        assertEquals(0, dummyPlatformProxyListener.cancelTaskRequestsReceived());

        boolean foundTask1 = false;
        boolean foundTask5 = false;
        boolean foundTask6 = false;
        
        for (PlatformProxyUpdateRequest request : taskUpdateRequestsReceivedByListener) {

            log.info("Task id = " + request.getTaskId());

            if (request.getTaskId().equals("1")) {
                assertEquals("resource1", request.getResources().get(0).getResourceId());
                assertEquals("resource2", request.getResources().get(1).getResourceId());
                assertEquals("enablerLogic", request.getEnablerLogicName());
                assertEquals(60, (long) request.getQueryInterval_ms());
                foundTask1 = true;
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
                foundTask5 = true;
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
                foundTask6 = true;
                continue;
            }

            fail("The code should not reach here, because no other tasks should be received by the platform proxy");
        }

        assertTrue(foundTask1);
        assertTrue(foundTask5);
        assertTrue(foundTask6);

        log.info("updateTaskTest FINISHED!");
    }

    @Test
    public void updateSparqlQueryTest() throws Exception {
        // In this test, we update the sparqlQueryRequest

        log.info("updateTaskTest STARTED!");

        final AtomicReference<ResourceManagerUpdateResponse> resultRef = new AtomicReference<>();
        List<PlatformProxyUpdateRequest> taskUpdateRequestsReceivedByListener;

        CoreQueryRequest coreQueryRequest = new CoreQueryRequest.Builder()
                .locationName("Zurich")
                .observedProperty(Arrays.asList("temperature", "humidity"))
                .shouldRank(true)
                .build();

        SparqlQueryRequest sparqlQueryRequest = new SparqlQueryRequest("Zurich",
                SparqlQueryOutputFormat.COUNT);
                List<String> resourceIds = Arrays.asList("resource1", "resource2");
        List<String> storedResourceIds = Arrays.asList("3", "4");

        Map<String, String> resourceUrls1 = new HashMap<>();
        resourceUrls1.put("resource1", symbIoTeCoreUrl + "/Sensors('resource1')");
        resourceUrls1.put("resource2", symbIoTeCoreUrl + "/Sensors('resource2')");

        ArrayList<QueryResourceResult> results = new ArrayList<>();
        QueryResourceResult result1 = new QueryResourceResult();
        QueryResourceResult result2 = new QueryResourceResult();
        result1.setId("resource1");
        result2.setId("resource2");
        results.add(result1);
        results.add(result2);

        TaskInfo task1 = new TaskInfo("1", 2, coreQueryRequest, "P0-0-0T0:0:0.06",
                true, "P0-0-0T0:0:1", true, "enablerLogic", sparqlQueryRequest, resourceIds, results,
                ResourceManagerTaskInfoResponseStatus.SUCCESS, storedResourceIds, resourceUrls1, "SUCCESS");
        task1.setMaxNoResources(2);
        taskInfoRepository.save(task1);

        Map<String, String> resourceUrls2 = new HashMap<>();
        resourceUrls2.put("21", symbIoTeCoreUrl + "/Sensors('21')");
        resourceUrls2.put("22", symbIoTeCoreUrl + "/Sensors('22')");
        TaskInfo task2 = new TaskInfo(task1);
        task2.setTaskId("2");
        task2.setResourceIds(Arrays.asList("21", "22"));
        task2.setResourceUrls(resourceUrls2);
        task2.getResourceDescriptions().get(0).setId("21");
        task2.getResourceDescriptions().get(1).setId("22");
        taskInfoRepository.save(task2);

        Map<String, String> resourceUrls3 = new HashMap<>();
        resourceUrls3.put("31", symbIoTeCoreUrl + "/Sensors('31')");
        resourceUrls3.put("32", symbIoTeCoreUrl + "/Sensors('32')");
        TaskInfo task3 = new TaskInfo(task1);
        task3.setTaskId("3");
        task3.setResourceIds(Arrays.asList("31", "32"));
        task3.setResourceUrls(resourceUrls3);
        task3.getResourceDescriptions().get(0).setId("31");
        task3.getResourceDescriptions().get(1).setId("32");
        taskInfoRepository.save(task3);

        // This task should not reach Platform Proxy, since there are no changes
        TaskInfo updatedTask1 = new TaskInfo(task1);
        updatedTask1.getCoreQueryRequest().setLocation_name("Paris");
        updatedTask1.setSparqlQueryRequest(null);

        // This task should reach Platform Proxy and inform it for new values
        TaskInfo updatedTask2 = new TaskInfo(task2);
        updatedTask2.getCoreQueryRequest().setLocation_name("Athens");
        updatedTask2.getSparqlQueryRequest().setSparqlQuery("Paris");

        // This task should not reach Platform Proxy, since there is a sparqlQueryRequest stored
        TaskInfo updatedTask3 = new TaskInfo(task3);
        updatedTask3.getCoreQueryRequest().setLocation_name("Athens");
        updatedTask3.setSparqlQueryRequest(sparqlQueryRequest);

        ResourceManagerUpdateRequest req = new ResourceManagerUpdateRequest();
        req.setTasks(Arrays.asList(new ResourceManagerTaskInfoRequest(updatedTask1),
                new ResourceManagerTaskInfoRequest(updatedTask2),
                new ResourceManagerTaskInfoRequest(updatedTask3)));


        log.info("Before sending the message");
        RabbitConverterFuture<ResourceManagerUpdateResponse> future = asyncRabbitTemplate
                .convertSendAndReceive(resourceManagerExchangeName, updateTaskRoutingKey, req);
        log.info("After sending the message");

        future.addCallback(new ListenableFutureUpdateCallback("updateTaskTest", resultRef));

        while(!future.isDone()) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(100);

        TaskInfo storedTaskInfo1 = taskInfoRepository.findByTaskId("1");
        TaskInfo storedTaskInfo2 = taskInfoRepository.findByTaskId("2");
        TaskInfo storedTaskInfo3 = taskInfoRepository.findByTaskId("3");

        // In the 1st and 3rd tasks nothing changes, since the sparqlQuery which is set does not change so the coreQueryRequest
        // is set into that of the storedTaskInfo. In the 2nd case, the sparqlQuery changes.
        assertEquals(storedTaskInfo1, task1);
        assertNotEquals(storedTaskInfo2, task2);
        assertEquals(storedTaskInfo3, task3);

        // In the 1st and 3rd cases the coreQueryRequest is different. In the 2nd case the resources change.
        assertNotEquals(storedTaskInfo1, updatedTask1);
        assertNotEquals(storedTaskInfo2, updatedTask2);
        assertNotEquals(storedTaskInfo3, updatedTask3);


        // Test what is stored in the database
        assertEquals(2, storedTaskInfo1.getResourceIds().size());
        assertEquals(2, storedTaskInfo1.getResourceDescriptions().size());
        assertEquals(2, storedTaskInfo1.getStoredResourceIds().size());
        assertEquals(2, storedTaskInfo1.getResourceUrls().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, storedTaskInfo1.getStatus());
        assertEquals("SUCCESS", storedTaskInfo1.getMessage());
        assertEquals("resource1", storedTaskInfo1.getResourceIds().get(0));
        assertEquals("resource2", storedTaskInfo1.getResourceIds().get(1));
        assertEquals("resource1", storedTaskInfo1.getResourceDescriptions().get(0).getId());
        assertEquals("resource2", storedTaskInfo1.getResourceDescriptions().get(1).getId());
        assertEquals("3", storedTaskInfo1.getStoredResourceIds().get(0));
        assertEquals("4", storedTaskInfo1.getStoredResourceIds().get(1));
        assertEquals(symbIoTeCoreUrl + "/Sensors('resource1')", storedTaskInfo1.getResourceUrls().get("resource1"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('resource2')", storedTaskInfo1.getResourceUrls().get("resource2"));

        assertEquals(2, storedTaskInfo2.getResourceIds().size());
        assertEquals(2, storedTaskInfo2.getResourceDescriptions().size());
        assertEquals(1, storedTaskInfo2.getStoredResourceIds().size());
        assertEquals(2, storedTaskInfo2.getResourceUrls().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, storedTaskInfo2.getStatus());
        assertEquals("SUCCESS", storedTaskInfo2.getMessage());
        assertEquals("sparqlResource1", storedTaskInfo2.getResourceIds().get(0));
        assertEquals("sparqlResource2", storedTaskInfo2.getResourceIds().get(1));
        assertEquals("sparqlResource1", storedTaskInfo2.getResourceDescriptions().get(0).getId());
        assertEquals("sparqlResource2", storedTaskInfo2.getResourceDescriptions().get(1).getId());
        assertEquals("sparqlResource3", storedTaskInfo2.getStoredResourceIds().get(0));
        assertEquals(symbIoTeCoreUrl + "/Sensors('sparqlResource1')", storedTaskInfo2.getResourceUrls().get("sparqlResource1"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('sparqlResource2')", storedTaskInfo2.getResourceUrls().get("sparqlResource2"));

        assertEquals(2, storedTaskInfo3.getResourceIds().size());
        assertEquals(2, storedTaskInfo3.getResourceDescriptions().size());
        assertEquals(2, storedTaskInfo3.getStoredResourceIds().size());
        assertEquals(2, storedTaskInfo3.getResourceUrls().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, storedTaskInfo3.getStatus());
        assertEquals("SUCCESS", storedTaskInfo3.getMessage());
        assertEquals("31", storedTaskInfo3.getResourceIds().get(0));
        assertEquals("32", storedTaskInfo3.getResourceIds().get(1));
        assertEquals("31", storedTaskInfo3.getResourceDescriptions().get(0).getId());
        assertEquals("32", storedTaskInfo3.getResourceDescriptions().get(1).getId());
        assertEquals("3", storedTaskInfo3.getStoredResourceIds().get(0));
        assertEquals("4", storedTaskInfo3.getStoredResourceIds().get(1));
        assertEquals(symbIoTeCoreUrl + "/Sensors('31')", storedTaskInfo3.getResourceUrls().get("31"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('32')", storedTaskInfo3.getResourceUrls().get("32"));

        // Test what Enabler Logic receives
        assertEquals(3, resultRef.get().getTasks().size());
        assertEquals(2, resultRef.get().getTasks().get(0).getResourceIds().size());
        assertEquals(2, resultRef.get().getTasks().get(1).getResourceIds().size());
        assertEquals(2, resultRef.get().getTasks().get(2).getResourceIds().size());

        assertEquals(2, resultRef.get().getTasks().get(0).getResourceDescriptions().size());
        assertEquals(2, resultRef.get().getTasks().get(1).getResourceDescriptions().size());
        assertEquals(2, resultRef.get().getTasks().get(2).getResourceDescriptions().size());

        assertEquals(ResourceManagerTasksStatus.SUCCESS, resultRef.get().getStatus());
        assertEquals("ALL the update task requests were successful!", resultRef.get().getMessage());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, resultRef.get().getTasks().get(0).getStatus());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, resultRef.get().getTasks().get(1).getStatus());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, resultRef.get().getTasks().get(2).getStatus());
        assertEquals("SUCCESS", resultRef.get().getTasks().get(0).getMessage());
        assertEquals("SUCCESS", resultRef.get().getTasks().get(1).getMessage());
        assertEquals("SUCCESS", resultRef.get().getTasks().get(2).getMessage());

        assertEquals("resource1", resultRef.get().getTasks().get(0).getResourceIds().get(0));
        assertEquals("resource2", resultRef.get().getTasks().get(0).getResourceIds().get(1));
        assertEquals("sparqlResource1", resultRef.get().getTasks().get(1).getResourceIds().get(0));
        assertEquals("sparqlResource2", resultRef.get().getTasks().get(1).getResourceIds().get(1));
        assertEquals("31", resultRef.get().getTasks().get(2).getResourceIds().get(0));
        assertEquals("32", resultRef.get().getTasks().get(2).getResourceIds().get(1));

        assertEquals("resource1", resultRef.get().getTasks().get(0).getResourceDescriptions().get(0).getId());
        assertEquals("resource2", resultRef.get().getTasks().get(0).getResourceDescriptions().get(1).getId());
        assertEquals("sparqlResource1", resultRef.get().getTasks().get(1).getResourceDescriptions().get(0).getId());
        assertEquals("sparqlResource2", resultRef.get().getTasks().get(1).getResourceDescriptions().get(1).getId());
        assertEquals("31", resultRef.get().getTasks().get(2).getResourceDescriptions().get(0).getId());
        assertEquals("32", resultRef.get().getTasks().get(2).getResourceDescriptions().get(1).getId());

        while(dummyPlatformProxyListener.updateAcquisitionRequestsReceived() < 1) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(300);

        // Test what Platform Proxy receives
        taskUpdateRequestsReceivedByListener = dummyPlatformProxyListener.getUpdateAcquisitionRequestsReceivedByListener();

        assertEquals(1, dummyPlatformProxyListener.updateAcquisitionRequestsReceived());
        assertEquals(0, dummyPlatformProxyListener.startAcquisitionRequestsReceived());
        assertEquals(0, dummyPlatformProxyListener.cancelTaskRequestsReceived());

        boolean foundTask2 = false;

        for (PlatformProxyUpdateRequest request : taskUpdateRequestsReceivedByListener) {

            log.info("Task id = " + request.getTaskId());

            if (request.getTaskId().equals("2")) {
                assertEquals("sparqlResource1", request.getResources().get(0).getResourceId());
                assertEquals("sparqlResource2", request.getResources().get(1).getResourceId());
                foundTask2 = true;
                continue;
            }

            fail("The code should not reach here, because no other tasks should be received by the platform proxy");
        }

        assertTrue(foundTask2);

        log.info("updateTaskTest FINISHED!");
    }

    @Test
    public void wrongQueryIntervalFormatUpdateTest() throws Exception {
        log.info("wrongQueryIntervalFormatUpdateTest STARTED!");

        final AtomicReference<ResourceManagerUpdateResponse> resultRef = new AtomicReference<>();

        ResourceManagerUpdateRequest query = createValidUpdateQueryToResourceManager(2);
        Field queryIntervalField = query.getTasks().get(1).getClass().getDeclaredField("queryInterval");
        queryIntervalField.setAccessible(true);
        queryIntervalField.set(query.getTasks().get(1), "10s");

        log.info("Before sending the message");
        RabbitConverterFuture<ResourceManagerUpdateResponse> future = asyncRabbitTemplate
                .convertSendAndReceive(resourceManagerExchangeName, updateTaskRoutingKey, query);
        log.info("After sending the message");

        future.addCallback(new ListenableFutureUpdateCallback("wrongQueryIntervalFormatUpdateTest", resultRef));

        while(!future.isDone()) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(100);

        // Test what Enabler Logic receives
        assertEquals(ResourceManagerTasksStatus.FAILED_WRONG_FORMAT_INTERVAL, resultRef.get().getStatus());
        assertTrue(resultRef.get().getMessage().contains("Invalid format"));

        TimeUnit.MILLISECONDS.sleep(500);

        // Test what Platform Proxy receives
        assertEquals(0, dummyPlatformProxyListener.startAcquisitionRequestsReceived());
        assertEquals(0, dummyPlatformProxyListener.updateAcquisitionRequestsReceived());

        // Test what is stored in the database
        TaskInfo taskInfo = taskInfoRepository.findByTaskId("1");
        assertNull(taskInfo);

        taskInfo = taskInfoRepository.findByTaskId("2");
        assertNull(taskInfo);

        log.info("wrongQueryIntervalFormatUpdateTest FINISHED!");

    }

    @Test
    public void wrongCacheIntervalFormatUpdateTest() throws Exception {
        log.info("wrongCacheIntervalFormatUpdateTest STARTED!");

        final AtomicReference<ResourceManagerUpdateResponse> resultRef = new AtomicReference<>();

        ResourceManagerUpdateRequest query = createValidUpdateQueryToResourceManager(2);
        Field cachingIntervalField = query.getTasks().get(1).getClass().getDeclaredField("cachingInterval");
        cachingIntervalField.setAccessible(true);
        cachingIntervalField.set(query.getTasks().get(1), "10s");

        log.info("Before sending the message");
        RabbitConverterFuture<ResourceManagerUpdateResponse> future = asyncRabbitTemplate
                .convertSendAndReceive(resourceManagerExchangeName, updateTaskRoutingKey, query);
        log.info("After sending the message");

        future.addCallback(new ListenableFutureUpdateCallback("wrongCacheIntervalFormatUpdateTest", resultRef));

        while(!future.isDone()) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(100);

        // Test what Enabler Logic receives
        assertEquals(ResourceManagerTasksStatus.FAILED_WRONG_FORMAT_INTERVAL, resultRef.get().getStatus());
        assertTrue(resultRef.get().getMessage().contains("Invalid format"));

        TimeUnit.MILLISECONDS.sleep(500);

        // Test what Platform Proxy receives
        assertEquals(0, dummyPlatformProxyListener.startAcquisitionRequestsReceived());
        assertEquals(0, dummyPlatformProxyListener.updateAcquisitionRequestsReceived());

        // Test what is stored in the database
        TaskInfo taskInfo = taskInfoRepository.findByTaskId("1");
        assertNull(taskInfo);

        taskInfo = taskInfoRepository.findByTaskId("2");
        assertNull(taskInfo);

        log.info("wrongCacheIntervalFormatUpdateTest FINISHED!");
    }

    @Test
    public void updateTaskWithInformPlatformProxyBecomingFalseTest() throws Exception {
        // In this test, the value of informPlatformProxy changes from true to false

        log.info("updateTaskWithInformPlatformProxyBecomingFalseTest STARTED!");

        final AtomicReference<ResourceManagerUpdateResponse> resultRef = new AtomicReference<>();
        List<CancelTaskRequest> cancelTaskRequestsReceivedByListener;

        CoreQueryRequest coreQueryRequest = new CoreQueryRequest.Builder()
                .locationName("Zurich")
                .observedProperty(Arrays.asList("temperature", "humidity"))
                .shouldRank(true)
                .build();

        List<String> resourceIds = Arrays.asList("resource1", "resource2");
        List<String> storedResourceIds = Arrays.asList("3", "4");

        Map<String, String> resourceUrls1 = new HashMap<>();
        resourceUrls1.put("resource1", symbIoTeCoreUrl + "/Sensors('resource1')");
        resourceUrls1.put("resource2", symbIoTeCoreUrl + "/Sensors('resource2')");

        ArrayList<QueryResourceResult> results = new ArrayList<>();
        QueryResourceResult result1 = new QueryResourceResult();
        QueryResourceResult result2 = new QueryResourceResult();
        result1.setId("resource1");
        result2.setId("resource2");
        results.add(result1);
        results.add(result2);

        TaskInfo task1 = new TaskInfo("1", 2, coreQueryRequest, "P0-0-0T0:0:0.06",
                true, "P0-0-0T0:0:1", true, "enablerLogic", null, resourceIds, results,
                ResourceManagerTaskInfoResponseStatus.SUCCESS, storedResourceIds, resourceUrls1, "SUCCESS");
        task1.setMaxNoResources(2);
        taskInfoRepository.save(task1);

        Map<String, String> resourceUrls2 = new HashMap<>();
        resourceUrls2.put("21", symbIoTeCoreUrl + "/Sensors('21')");
        resourceUrls2.put("22", symbIoTeCoreUrl + "/Sensors('22')");
        TaskInfo task2 = new TaskInfo(task1);
        task2.setTaskId("2");
        task2.setResourceIds(Arrays.asList("21", "22"));
        task2.setResourceUrls(resourceUrls2);
        task2.getResourceDescriptions().get(0).setId("21");
        task2.getResourceDescriptions().get(1).setId("22");
        taskInfoRepository.save(task2);

         // This task should reach Platform Proxy and inform it for new resources
        TaskInfo updatedTask1 = new TaskInfo(task1);
        updatedTask1.getCoreQueryRequest().setLocation_name("Paris");
        updatedTask1.setInformPlatformProxy(false);

        // This task should not reach Platform Proxy, because InformPlatformProxy == false
        TaskInfo updatedTask2 = new TaskInfo(task2);
        updatedTask2.setInformPlatformProxy(false);

        ResourceManagerUpdateRequest req = new ResourceManagerUpdateRequest();
        req.setTasks(Arrays.asList(new ResourceManagerTaskInfoRequest(updatedTask1),
                new ResourceManagerTaskInfoRequest(updatedTask2)));


        log.info("Before sending the message");
        RabbitConverterFuture<ResourceManagerUpdateResponse> future = asyncRabbitTemplate
                .convertSendAndReceive(resourceManagerExchangeName, updateTaskRoutingKey, req);
        log.info("After sending the message");

        future.addCallback(new ListenableFutureUpdateCallback("updateTaskWithInformPlatformProxyBecomingFalseTest", resultRef));

        while(!future.isDone()) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(100);

        TaskInfo storedTaskInfo1 = taskInfoRepository.findByTaskId("1");
        TaskInfo storedTaskInfo2 = taskInfoRepository.findByTaskId("2");

        // Both tasks change their informPlatformProxy field
        assertNotEquals(storedTaskInfo1, task1);
        assertNotEquals(storedTaskInfo2, task2);

        // Only updatedTask1.equals(storedTaskInfo1) should be false, because it has modified resources
        assertNotEquals(storedTaskInfo1, updatedTask1);
        assertEquals(storedTaskInfo2, updatedTask2);

        // Test what is stored in the database
        assertEquals(2, storedTaskInfo1.getResourceIds().size());
        assertEquals(2, storedTaskInfo1.getResourceDescriptions().size());
        assertEquals(1, storedTaskInfo1.getStoredResourceIds().size());
        assertEquals(2, storedTaskInfo1.getResourceUrls().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, storedTaskInfo1.getStatus());
        assertEquals("SUCCESS", storedTaskInfo1.getMessage());
        assertEquals("resource1", storedTaskInfo1.getResourceIds().get(0));
        assertEquals("resource2", storedTaskInfo1.getResourceIds().get(1));
        assertEquals("resource1", storedTaskInfo1.getResourceDescriptions().get(0).getId());
        assertEquals("resource2", storedTaskInfo1.getResourceDescriptions().get(1).getId());
        assertEquals("resource3", storedTaskInfo1.getStoredResourceIds().get(0));
        assertEquals(symbIoTeCoreUrl + "/Sensors('resource1')", storedTaskInfo1.getResourceUrls().get("resource1"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('resource2')", storedTaskInfo1.getResourceUrls().get("resource2"));

        assertEquals(2, storedTaskInfo2.getResourceIds().size());
        assertEquals(2, storedTaskInfo2.getResourceDescriptions().size());
        assertEquals(2, storedTaskInfo2.getStoredResourceIds().size());
        assertEquals(2, storedTaskInfo2.getResourceUrls().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, storedTaskInfo2.getStatus());
        assertEquals("SUCCESS", storedTaskInfo2.getMessage());
        assertEquals("21", storedTaskInfo2.getResourceIds().get(0));
        assertEquals("22", storedTaskInfo2.getResourceIds().get(1));
        assertEquals("21", storedTaskInfo2.getResourceDescriptions().get(0).getId());
        assertEquals("22", storedTaskInfo2.getResourceDescriptions().get(1).getId());
        assertEquals("3", storedTaskInfo2.getStoredResourceIds().get(0));
        assertEquals("4", storedTaskInfo2.getStoredResourceIds().get(1));
        assertEquals(symbIoTeCoreUrl + "/Sensors('21')", storedTaskInfo2.getResourceUrls().get("21"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('22')", storedTaskInfo2.getResourceUrls().get("22"));

        // Test what Enabler Logic receives
        assertEquals(2, resultRef.get().getTasks().size());
        assertEquals(2, resultRef.get().getTasks().get(0).getResourceIds().size());
        assertEquals(2, resultRef.get().getTasks().get(1).getResourceIds().size());
        assertEquals(2, resultRef.get().getTasks().get(0).getResourceDescriptions().size());
        assertEquals(2, resultRef.get().getTasks().get(1).getResourceDescriptions().size());

        assertEquals(ResourceManagerTasksStatus.SUCCESS, resultRef.get().getStatus());
        assertEquals("ALL the update task requests were successful!", resultRef.get().getMessage());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, resultRef.get().getTasks().get(0).getStatus());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, resultRef.get().getTasks().get(1).getStatus());
        assertEquals("SUCCESS", resultRef.get().getTasks().get(0).getMessage());
        assertEquals("SUCCESS", resultRef.get().getTasks().get(1).getMessage());

        assertEquals("resource1", resultRef.get().getTasks().get(0).getResourceIds().get(0));
        assertEquals("resource2", resultRef.get().getTasks().get(0).getResourceIds().get(1));
        assertEquals("21", resultRef.get().getTasks().get(1).getResourceIds().get(0));
        assertEquals("22", resultRef.get().getTasks().get(1).getResourceIds().get(1));

        assertEquals("resource1", resultRef.get().getTasks().get(0).getResourceDescriptions().get(0).getId());
        assertEquals("resource2", resultRef.get().getTasks().get(0).getResourceDescriptions().get(1).getId());
        assertEquals("21", resultRef.get().getTasks().get(1).getResourceDescriptions().get(0).getId());
        assertEquals("22", resultRef.get().getTasks().get(1).getResourceDescriptions().get(1).getId());

        while(dummyPlatformProxyListener.cancelTaskRequestsReceived() < 1) {
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
    public void updateTaskWithInformPlatformProxyBecomingTrueTest() throws Exception {
        // In this test, the value of informPlatformProxy changes from false to true

        log.info("updateTaskWithInformPlatformProxyBecomingTrueTest STARTED!");

        final AtomicReference<ResourceManagerUpdateResponse> resultRef = new AtomicReference<>();
        List<PlatformProxyAcquisitionStartRequest> startAcquisitionRequestsReceivedByListener;

        CoreQueryRequest coreQueryRequest = new CoreQueryRequest.Builder()
                .locationName("Zurich")
                .observedProperty(Arrays.asList("temperature", "humidity"))
                .shouldRank(true)
                .build();

        List<String> resourceIds = Arrays.asList("resource1", "resource2");
        List<String> storedResourceIds = Arrays.asList("3", "4");

        Map<String, String> resourceUrls1 = new HashMap<>();
        resourceUrls1.put("resource1", symbIoTeCoreUrl + "/Sensors('resource1')");
        resourceUrls1.put("resource2", symbIoTeCoreUrl + "/Sensors('resource2')");

        ArrayList<QueryResourceResult> results = new ArrayList<>();
        QueryResourceResult result1 = new QueryResourceResult();
        QueryResourceResult result2 = new QueryResourceResult();
        result1.setId("resource1");
        result2.setId("resource2");
        results.add(result1);
        results.add(result2);

        TaskInfo task1 = new TaskInfo("1", 2, coreQueryRequest, "P0-0-0T0:0:0.06",
                true, "P0-0-0T0:0:1", false, "enablerLogic", null, resourceIds, results,
                ResourceManagerTaskInfoResponseStatus.SUCCESS, storedResourceIds, resourceUrls1, "SUCCESS");
        task1.setMaxNoResources(2);
        taskInfoRepository.save(task1);

        Map<String, String> resourceUrls2 = new HashMap<>();
        resourceUrls2.put("21", symbIoTeCoreUrl + "/Sensors('21')");
        resourceUrls2.put("22", symbIoTeCoreUrl + "/Sensors('22')");
        TaskInfo task2 = new TaskInfo(task1);
        task2.setTaskId("2");
        task2.setResourceIds(Arrays.asList("21", "22"));
        task2.setResourceUrls(resourceUrls2);
        task2.getResourceDescriptions().get(0).setId("21");
        task2.getResourceDescriptions().get(1).setId("22");
        taskInfoRepository.save(task2);

        // This task should reach Platform Proxy and inform it for new resources
        TaskInfo updatedTask1 = new TaskInfo(task1);
        updatedTask1.getCoreQueryRequest().setLocation_name("Paris");
        updatedTask1.setInformPlatformProxy(true);

        // This task should not reach Platform Proxy, because InformPlatformProxy == false
        TaskInfo updatedTask2 = new TaskInfo(task2);
        updatedTask2.setInformPlatformProxy(true);

        ResourceManagerUpdateRequest req = new ResourceManagerUpdateRequest();
        req.setTasks(Arrays.asList(new ResourceManagerTaskInfoRequest(updatedTask1),
                new ResourceManagerTaskInfoRequest(updatedTask2)));


        log.info("Before sending the message");
        RabbitConverterFuture<ResourceManagerUpdateResponse> future = asyncRabbitTemplate
                .convertSendAndReceive(resourceManagerExchangeName, updateTaskRoutingKey, req);
        log.info("After sending the message");

        future.addCallback(new ListenableFutureUpdateCallback("updateTaskWithInformPlatformProxyBecomingFalseTest",
                resultRef));

        while(!future.isDone()) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(100);

        TaskInfo storedTaskInfo1 = taskInfoRepository.findByTaskId("1");
        TaskInfo storedTaskInfo2 = taskInfoRepository.findByTaskId("2");

        // Both tasks change their informPlatformProxy field
        assertNotEquals(storedTaskInfo1, task1);
        assertNotEquals(storedTaskInfo2, task2);

        // Only updatedTask1.equals(storedTaskInfo1) should be false, because it has modified resources
        assertNotEquals(storedTaskInfo1, updatedTask1);
        assertEquals(storedTaskInfo2, updatedTask2);

        // Test what is stored in the database
        assertEquals(2, storedTaskInfo1.getResourceIds().size());
        assertEquals(2, storedTaskInfo1.getResourceDescriptions().size());
        assertEquals(1, storedTaskInfo1.getStoredResourceIds().size());
        assertEquals(2, storedTaskInfo1.getResourceUrls().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, storedTaskInfo1.getStatus());
        assertEquals("SUCCESS", storedTaskInfo1.getMessage());
        assertEquals("resource1", storedTaskInfo1.getResourceIds().get(0));
        assertEquals("resource2", storedTaskInfo1.getResourceIds().get(1));
        assertEquals("resource1", storedTaskInfo1.getResourceDescriptions().get(0).getId());
        assertEquals("resource2", storedTaskInfo1.getResourceDescriptions().get(1).getId());
        assertEquals("resource3", storedTaskInfo1.getStoredResourceIds().get(0));
        assertEquals(symbIoTeCoreUrl + "/Sensors('resource1')", storedTaskInfo1.getResourceUrls().get("resource1"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('resource2')", storedTaskInfo1.getResourceUrls().get("resource2"));

        assertEquals(2, storedTaskInfo2.getResourceIds().size());
        assertEquals(2, storedTaskInfo2.getResourceDescriptions().size());
        assertEquals(2, storedTaskInfo2.getStoredResourceIds().size());
        assertEquals(2, storedTaskInfo2.getResourceUrls().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, storedTaskInfo2.getStatus());
        assertEquals("SUCCESS", storedTaskInfo2.getMessage());
        assertEquals("21", storedTaskInfo2.getResourceIds().get(0));
        assertEquals("22", storedTaskInfo2.getResourceIds().get(1));
        assertEquals("21", storedTaskInfo2.getResourceDescriptions().get(0).getId());
        assertEquals("22", storedTaskInfo2.getResourceDescriptions().get(1).getId());
        assertEquals("3", storedTaskInfo2.getStoredResourceIds().get(0));
        assertEquals("4", storedTaskInfo2.getStoredResourceIds().get(1));
        assertEquals(symbIoTeCoreUrl + "/Sensors('21')", storedTaskInfo2.getResourceUrls().get("21"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('22')", storedTaskInfo2.getResourceUrls().get("22"));

        // Test what Enabler Logic receives
        assertEquals(2, resultRef.get().getTasks().size());
        assertEquals(2, resultRef.get().getTasks().get(0).getResourceIds().size());
        assertEquals(2, resultRef.get().getTasks().get(1).getResourceIds().size());
        assertEquals(2, resultRef.get().getTasks().get(0).getResourceDescriptions().size());
        assertEquals(2, resultRef.get().getTasks().get(1).getResourceDescriptions().size());

        assertEquals(ResourceManagerTasksStatus.SUCCESS, resultRef.get().getStatus());
        assertEquals("ALL the update task requests were successful!", resultRef.get().getMessage());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, resultRef.get().getTasks().get(0).getStatus());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, resultRef.get().getTasks().get(1).getStatus());
        assertEquals("SUCCESS", resultRef.get().getTasks().get(0).getMessage());
        assertEquals("SUCCESS", resultRef.get().getTasks().get(1).getMessage());

        assertEquals("resource1", resultRef.get().getTasks().get(0).getResourceIds().get(0));
        assertEquals("resource2", resultRef.get().getTasks().get(0).getResourceIds().get(1));
        assertEquals("21", resultRef.get().getTasks().get(1).getResourceIds().get(0));
        assertEquals("22", resultRef.get().getTasks().get(1).getResourceIds().get(1));

        assertEquals("resource1", resultRef.get().getTasks().get(0).getResourceDescriptions().get(0).getId());
        assertEquals("resource2", resultRef.get().getTasks().get(0).getResourceDescriptions().get(1).getId());
        assertEquals("21", resultRef.get().getTasks().get(1).getResourceDescriptions().get(0).getId());
        assertEquals("22", resultRef.get().getTasks().get(1).getResourceDescriptions().get(1).getId());

        while(dummyPlatformProxyListener.startAcquisitionRequestsReceived() < 2) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(300);

        // Test what Platform Proxy receives
        startAcquisitionRequestsReceivedByListener = dummyPlatformProxyListener.getStartAcquisitionRequestsReceivedByListener();

        assertEquals(0, dummyPlatformProxyListener.updateAcquisitionRequestsReceived());
        assertEquals(2, dummyPlatformProxyListener.startAcquisitionRequestsReceived());
        assertEquals(0, dummyPlatformProxyListener.cancelTaskRequestsReceived());

        boolean foundTask1 = false;
        boolean foundTask2 = false;
        
        for (PlatformProxyAcquisitionStartRequest request : startAcquisitionRequestsReceivedByListener) {

            log.info("Task id = " + request.getTaskId());

            if (request.getTaskId().equals("1")) {
                Map<String, String> resourcesMap = new HashMap<>();
                resourcesMap.put("resource1", symbIoTeCoreUrl + "/Sensors('resource1')");
                resourcesMap.put("resource2", symbIoTeCoreUrl + "/Sensors('resource2')");

                assertEquals(2, request.getResources().size());
                assertEquals(resourcesMap.get(request.getResources().get(0).getResourceId()),
                        request.getResources().get(0).getAccessURL());
                assertEquals(resourcesMap.get(request.getResources().get(1).getResourceId()),
                        request.getResources().get(1).getAccessURL());
                foundTask1 = true;
                continue;
            }

            if (request.getTaskId().equals("2")) {
                Map<String, String> resourcesMap = new HashMap<>();
                resourcesMap.put("21", symbIoTeCoreUrl + "/Sensors('21')");
                resourcesMap.put("22", symbIoTeCoreUrl + "/Sensors('22')");

                assertEquals(2, request.getResources().size());
                assertEquals(resourcesMap.get(request.getResources().get(0).getResourceId()),
                        request.getResources().get(0).getAccessURL());
                assertEquals(resourcesMap.get(request.getResources().get(1).getResourceId()),
                        request.getResources().get(1).getAccessURL());
                foundTask2 = true;
                continue;
            }

            fail("The code should not reach here, because no other tasks should be received by the platform proxy");
        }

        assertTrue(foundTask1);
        assertTrue(foundTask2);
        
        log.info("updateTaskWithInformPlatformProxyBecomingTrueTest FINISHED!");
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

        ArrayList<QueryResourceResult> results = new ArrayList<>();
        QueryResourceResult result1 = new QueryResourceResult();
        QueryResourceResult result2 = new QueryResourceResult();
        result1.setId("resource1");
        result2.setId("resource2");
        results.add(result1);
        results.add(result2);

        TaskInfo task1 = new TaskInfo("1", 2, coreQueryRequest, "P0-0-0T0:0:0.06",
                true, "P0-0-0T1:0:1", true, "enablerLogic", null, resourceIds, results,
                ResourceManagerTaskInfoResponseStatus.SUCCESS, storedResourceIds, resourceUrls1, "SUCCESS");
        task1.setMaxNoResources(2);
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
        task2.getResourceDescriptions().get(0).setId("21");
        task2.getResourceDescriptions().get(1).setId("22");
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
        task3.getResourceDescriptions().get(0).setId("31");
        task3.getResourceDescriptions().get(1).setId("32");
        QueryResourceResult result3 = new QueryResourceResult();
        result3.setId("resource1");
        task3.getResourceDescriptions().add(result3);
        taskInfoRepository.save(task3);

        Map<String, String> resourceUrls4 = new HashMap<>();
        resourceUrls4.put("41", symbIoTeCoreUrl + "/Sensors('41')");
        resourceUrls4.put("42", symbIoTeCoreUrl + "/Sensors('42')");
        TaskInfo task4 = new TaskInfo(task1);
        task4.setTaskId("4");
        task4.setResourceIds(Arrays.asList("41", "42"));
        task4.setAllowCaching(false);
        task4.setResourceUrls(resourceUrls4);
        task4.getResourceDescriptions().get(0).setId("41");
        task4.getResourceDescriptions().get(1).setId("42");
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
        assertNotEquals(storedTaskInfo1, task1);
        assertNotEquals(storedTaskInfo2, task2);
        assertNotEquals(storedTaskInfo3, task3);
        assertNotEquals(storedTaskInfo4, task4);

        // Both should be false, because the stored resources are cleared
        assertNotEquals(storedTaskInfo1, updatedTask1);
        assertNotEquals(storedTaskInfo2, updatedTask2);

        // Because the storedResources are updated
        assertNotEquals(storedTaskInfo3, updatedTask3);
        assertNotEquals(storedTaskInfo4, updatedTask4);

        // Test if the stored resources were cleared in the first 2 tasks
        assertEquals(2, storedTaskInfo1.getResourceIds().size());
        assertEquals(2, storedTaskInfo1.getResourceDescriptions().size());
        assertEquals(2, storedTaskInfo1.getResourceDescriptions().size());
        assertEquals(0, storedTaskInfo1.getStoredResourceIds().size());
        assertEquals(2, storedTaskInfo1.getResourceUrls().size());
        assertEquals(2, storedTaskInfo2.getResourceIds().size());
        assertEquals(2, storedTaskInfo2.getResourceDescriptions().size());
        assertEquals(2, storedTaskInfo2.getResourceDescriptions().size());
        assertEquals(0, storedTaskInfo2.getStoredResourceIds().size());
        assertEquals(2, storedTaskInfo2.getResourceUrls().size());

        // Test the statuses
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, storedTaskInfo1.getStatus());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, storedTaskInfo2.getStatus());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, storedTaskInfo3.getStatus());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, storedTaskInfo4.getStatus());

        // Test the messages
        assertEquals("SUCCESS", storedTaskInfo1.getMessage());
        assertEquals("SUCCESS", storedTaskInfo2.getMessage());
        assertEquals("SUCCESS", storedTaskInfo3.getMessage());
        assertEquals("SUCCESS", storedTaskInfo4.getMessage());

        // Test if stored resources were added in the last 2 tasks
        assertEquals(3, storedTaskInfo3.getResourceIds().size());
        assertEquals(3, storedTaskInfo3.getResourceDescriptions().size());
        assertEquals(2, storedTaskInfo3.getStoredResourceIds().size());
        assertEquals(3, storedTaskInfo3.getResourceUrls().size());
        assertEquals(2, storedTaskInfo4.getResourceIds().size());
        assertEquals(2, storedTaskInfo4.getResourceDescriptions().size());
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

        // Check the resourceDescription
        assertEquals("resource1", storedTaskInfo1.getResourceDescriptions().get(0).getId());
        assertEquals("resource2", storedTaskInfo1.getResourceDescriptions().get(1).getId());
        assertEquals("21", storedTaskInfo2.getResourceDescriptions().get(0).getId());
        assertEquals("22", storedTaskInfo2.getResourceDescriptions().get(1).getId());
        assertEquals("31", storedTaskInfo3.getResourceDescriptions().get(0).getId());
        assertEquals("32", storedTaskInfo3.getResourceDescriptions().get(1).getId());
        assertEquals("resource1", storedTaskInfo3.getResourceDescriptions().get(2).getId());
        assertEquals("resource1", storedTaskInfo4.getResourceDescriptions().get(0).getId());
        assertEquals("resource2", storedTaskInfo4.getResourceDescriptions().get(1).getId());

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
        assertEquals(2, resultRef.get().getTasks().get(0).getResourceDescriptions().size());
        assertEquals(2, resultRef.get().getTasks().get(1).getResourceDescriptions().size());
        assertEquals(3, resultRef.get().getTasks().get(2).getResourceDescriptions().size());
        assertEquals(2, resultRef.get().getTasks().get(3).getResourceDescriptions().size());

        assertEquals(ResourceManagerTasksStatus.SUCCESS, resultRef.get().getStatus());
        assertEquals("ALL the update task requests were successful!", resultRef.get().getMessage());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, resultRef.get().getTasks().get(0).getStatus());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, resultRef.get().getTasks().get(1).getStatus());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, resultRef.get().getTasks().get(2).getStatus());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, resultRef.get().getTasks().get(3).getStatus());
        assertEquals("SUCCESS", resultRef.get().getTasks().get(0).getMessage());
        assertEquals("SUCCESS", resultRef.get().getTasks().get(1).getMessage());
        assertEquals("SUCCESS", resultRef.get().getTasks().get(2).getMessage());
        assertEquals("SUCCESS", resultRef.get().getTasks().get(3).getMessage());

        assertEquals("resource1", resultRef.get().getTasks().get(0).getResourceIds().get(0));
        assertEquals("resource2", resultRef.get().getTasks().get(0).getResourceIds().get(1));
        assertEquals("21", resultRef.get().getTasks().get(1).getResourceIds().get(0));
        assertEquals("22", resultRef.get().getTasks().get(1).getResourceIds().get(1));
        assertEquals("31", resultRef.get().getTasks().get(2).getResourceIds().get(0));
        assertEquals("32", resultRef.get().getTasks().get(2).getResourceIds().get(1));
        assertEquals("resource1", resultRef.get().getTasks().get(2).getResourceIds().get(2));
        assertEquals("resource1", resultRef.get().getTasks().get(3).getResourceIds().get(0));
        assertEquals("resource2", resultRef.get().getTasks().get(3).getResourceIds().get(1));

        assertEquals("resource1", resultRef.get().getTasks().get(0).getResourceDescriptions().get(0).getId());
        assertEquals("resource2", resultRef.get().getTasks().get(0).getResourceDescriptions().get(1).getId());
        assertEquals("21", resultRef.get().getTasks().get(1).getResourceDescriptions().get(0).getId());
        assertEquals("22", resultRef.get().getTasks().get(1).getResourceDescriptions().get(1).getId());
        assertEquals("31", resultRef.get().getTasks().get(2).getResourceDescriptions().get(0).getId());
        assertEquals("32", resultRef.get().getTasks().get(2).getResourceDescriptions().get(1).getId());
        assertEquals("resource1", resultRef.get().getTasks().get(2).getResourceDescriptions().get(2).getId());
        assertEquals("resource1", resultRef.get().getTasks().get(3).getResourceDescriptions().get(0).getId());
        assertEquals("resource2", resultRef.get().getTasks().get(3).getResourceDescriptions().get(1).getId());

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

        assertTrue(foundTask1);
        assertTrue(foundTask4);
        
        log.info("updateTaskWithAllowCachingTest FINISHED!");
    }


    @Test
    public void updateTaskWithCachingIntervalTest() throws Exception {

        log.info("updateTaskWithCachingIntervalTest STARTED!");

        final AtomicReference<ResourceManagerUpdateResponse> resultRef = new AtomicReference<>();

        CoreQueryRequest coreQueryRequest = new CoreQueryRequest.Builder()
                .locationName("Paris")
                .observedProperty(Arrays.asList("temperature", "humidity"))
                .shouldRank(true)
                .build();

        List<String> resourceIds = Arrays.asList("11", "12");
        List<String> storedResourceIds = Arrays.asList("3", "4");

        Map<String, String> resourceUrls1 = new HashMap<>();
        resourceUrls1.put("11", symbIoTeCoreUrl + "/Sensors('11')");
        resourceUrls1.put("12", symbIoTeCoreUrl + "/Sensors('12')");

        ArrayList<QueryResourceResult> results = new ArrayList<>();
        QueryResourceResult result1 = new QueryResourceResult();
        QueryResourceResult result2 = new QueryResourceResult();
        result1.setId("11");
        result2.setId("12");
        results.add(result1);
        results.add(result2);

        TaskInfo task1 = new TaskInfo("1", 2, coreQueryRequest, "P0-0-0T0:0:0.06",
                true, "P0-0-0T1:0:0", true, "enablerLogic", null, resourceIds, results,
                ResourceManagerTaskInfoResponseStatus.SUCCESS, storedResourceIds, resourceUrls1, "SUCCESS");
        task1.setMaxNoResources(2);
        taskInfoRepository.save(task1);
        searchHelper.getScheduledTaskInfoUpdateMap().put(task1.getTaskId(),
                new ScheduledTaskInfoUpdate(taskInfoRepository, searchHelper, task1));

        Map<String, String> resourceUrls2 = new HashMap<>();
        resourceUrls2.put("21", symbIoTeCoreUrl + "/Sensors('21')");
        resourceUrls2.put("22", symbIoTeCoreUrl + "/Sensors('22')");
        TaskInfo task2 = new TaskInfo(task1);
        task2.setTaskId("2");
        task2.setResourceIds(Arrays.asList("21", "22"));
        task2.setResourceUrls(resourceUrls2);
        task2.getResourceDescriptions().get(0).setId("21");
        task2.getResourceDescriptions().get(1).setId("22");
        taskInfoRepository.save(task2);
        searchHelper.getScheduledTaskInfoUpdateMap().put(task2.getTaskId(),
                new ScheduledTaskInfoUpdate(taskInfoRepository, searchHelper, task2));

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
        task3.getResourceDescriptions().get(0).setId("31");
        task3.getResourceDescriptions().get(1).setId("32");
        QueryResourceResult result3 = new QueryResourceResult();
        result3.setId("resource1");
        task3.getResourceDescriptions().add(result3);
        taskInfoRepository.save(task3);

        // Check the stored ScheduledTaskInfoUpdates
        assertNotNull(searchHelper.getScheduledTaskInfoUpdateMap().get(task1.getTaskId()));
        assertNotNull(searchHelper.getScheduledTaskInfoUpdateMap().get(task2.getTaskId()));
        assertNull(searchHelper.getScheduledTaskInfoUpdateMap().get(task3.getTaskId()));

        // This task should not reach Platform Proxy, because nothing that concerns the Platform Proxy changes
        // true -> false transition
        // cachingInterval changed
        // there should not be any ScheduledTaskInfoUpdate after the update
        TaskInfo updatedTask1 = new TaskInfo(task1);
        updatedTask1.setAllowCaching(false);
        updatedTask1.setCachingInterval("P0-0-0T0:0:1");


        // This task should not reach Platform Proxy, because nothing that concerns the Platform Proxy changes
        // true -> false transition
        // cachingInterval changed
        // there should an updated ScheduledTaskInfoUpdate after the update
        TaskInfo updatedTask2 = new TaskInfo(task2);
        updatedTask2.setCachingInterval("P0-0-0T0:0:1");

        // This task should not reach Platform Proxy, because nothing that concerns the Platform Proxy changes
        // false -> true transition
        // cachingInterval changed
        // there should an updated ScheduledTaskInfoUpdate after the update
        TaskInfo updatedTask3 = new TaskInfo(task3);
        updatedTask3.setAllowCaching(true);
        updatedTask3.setCachingInterval("P0-0-0T0:0:1");

        ResourceManagerUpdateRequest req = new ResourceManagerUpdateRequest();
        req.setTasks(Arrays.asList(new ResourceManagerTaskInfoRequest(updatedTask1),
                new ResourceManagerTaskInfoRequest(updatedTask2),
                new ResourceManagerTaskInfoRequest(updatedTask3)));


        log.info("Before sending the message");
        RabbitConverterFuture<ResourceManagerUpdateResponse> future = asyncRabbitTemplate
                .convertSendAndReceive(resourceManagerExchangeName, updateTaskRoutingKey, req);
        log.info("After sending the message");

        future.addCallback(new ListenableFutureUpdateCallback("updateTaskWithCachingIntervalTest", resultRef));

        while(!future.isDone()) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(100);

        TaskInfo storedTaskInfo1 = taskInfoRepository.findByTaskId("1");
        TaskInfo storedTaskInfo2 = taskInfoRepository.findByTaskId("2");
        TaskInfo storedTaskInfo3 = taskInfoRepository.findByTaskId("3");

        // All tasks change their allowCaching field
        assertNotEquals(storedTaskInfo1, task1);
        assertNotEquals(storedTaskInfo2, task2);
        assertNotEquals(storedTaskInfo3, task3);

        // Both should be false, because the stored resources are changed in every case
        assertNotEquals(storedTaskInfo1, updatedTask1);
        assertNotEquals(storedTaskInfo2, updatedTask2);
        assertNotEquals(storedTaskInfo3, updatedTask3);

        // Test if the stored resources were cleared in the first 1 task
        assertEquals(2, storedTaskInfo1.getResourceIds().size());
        assertEquals(2, storedTaskInfo1.getResourceDescriptions().size());
        assertEquals(0, storedTaskInfo1.getStoredResourceIds().size());
        assertEquals(2, storedTaskInfo1.getResourceUrls().size());
        assertEquals(2, storedTaskInfo2.getResourceIds().size());
        assertEquals(2, storedTaskInfo2.getResourceDescriptions().size());
        assertEquals(3, storedTaskInfo2.getStoredResourceIds().size());
        assertEquals(2, storedTaskInfo2.getResourceUrls().size());
        assertEquals(3, storedTaskInfo3.getResourceIds().size());
        assertEquals(3, storedTaskInfo3.getResourceDescriptions().size());
        assertEquals(2, storedTaskInfo3.getStoredResourceIds().size());
        assertEquals(3, storedTaskInfo3.getResourceUrls().size());

        // Check the statuses
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, storedTaskInfo1.getStatus());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, storedTaskInfo2.getStatus());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, storedTaskInfo3.getStatus());

        // Check the message
        assertEquals("SUCCESS", storedTaskInfo1.getMessage());
        assertEquals("SUCCESS", storedTaskInfo2.getMessage());
        assertEquals("SUCCESS", storedTaskInfo3.getMessage());

        // Check the resourceIds
        assertEquals("11", storedTaskInfo1.getResourceIds().get(0));
        assertEquals("12", storedTaskInfo1.getResourceIds().get(1));
        assertEquals("21", storedTaskInfo2.getResourceIds().get(0));
        assertEquals("22", storedTaskInfo2.getResourceIds().get(1));
        assertEquals("31", storedTaskInfo3.getResourceIds().get(0));
        assertEquals("32", storedTaskInfo3.getResourceIds().get(1));
        assertEquals("resource1", storedTaskInfo3.getResourceIds().get(2));

        // Check the resourceDescriptions
        assertEquals("11", storedTaskInfo1.getResourceDescriptions().get(0).getId());
        assertEquals("12", storedTaskInfo1.getResourceDescriptions().get(1).getId());
        assertEquals("21", storedTaskInfo2.getResourceDescriptions().get(0).getId());
        assertEquals("22", storedTaskInfo2.getResourceDescriptions().get(1).getId());
        assertEquals("31", storedTaskInfo3.getResourceDescriptions().get(0).getId());
        assertEquals("32", storedTaskInfo3.getResourceDescriptions().get(1).getId());
        assertEquals("resource1", storedTaskInfo3.getResourceDescriptions().get(2).getId());

        // Check the storedResourceIds
        assertEquals("resource1", storedTaskInfo2.getStoredResourceIds().get(0));
        assertEquals("resource2", storedTaskInfo2.getStoredResourceIds().get(1));
        assertEquals("resource3", storedTaskInfo2.getStoredResourceIds().get(2));
        assertEquals("resource2", storedTaskInfo3.getStoredResourceIds().get(0));
        assertEquals("resource3", storedTaskInfo3.getStoredResourceIds().get(1));

        // Check the resourceUrls
        assertEquals(symbIoTeCoreUrl + "/Sensors('11')", storedTaskInfo1.getResourceUrls().get("11"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('12')", storedTaskInfo1.getResourceUrls().get("12"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('21')", storedTaskInfo2.getResourceUrls().get("21"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('22')", storedTaskInfo2.getResourceUrls().get("22"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('31')", storedTaskInfo3.getResourceUrls().get("31"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('32')", storedTaskInfo3.getResourceUrls().get("32"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('resource1')", storedTaskInfo3.getResourceUrls().get("resource1"));


        // Check the stored ScheduledTaskInfoUpdates
        assertNull(searchHelper.getScheduledTaskInfoUpdateMap().get(task1.getTaskId()));
        assertNotNull(searchHelper.getScheduledTaskInfoUpdateMap().get(task2.getTaskId()));
        assertNotNull(searchHelper.getScheduledTaskInfoUpdateMap().get(task3.getTaskId()));

        // Test what Enabler Logic receives
        assertEquals(3, resultRef.get().getTasks().size());
        assertEquals(2, resultRef.get().getTasks().get(0).getResourceIds().size());
        assertEquals(2, resultRef.get().getTasks().get(1).getResourceIds().size());
        assertEquals(3, resultRef.get().getTasks().get(2).getResourceIds().size());
        assertEquals(2, resultRef.get().getTasks().get(0).getResourceDescriptions().size());
        assertEquals(2, resultRef.get().getTasks().get(1).getResourceDescriptions().size());
        assertEquals(3, resultRef.get().getTasks().get(2).getResourceDescriptions().size());

        assertEquals(ResourceManagerTasksStatus.SUCCESS, resultRef.get().getStatus());
        assertEquals("ALL the update task requests were successful!", resultRef.get().getMessage());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, resultRef.get().getTasks().get(0).getStatus());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, resultRef.get().getTasks().get(1).getStatus());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, resultRef.get().getTasks().get(2).getStatus());
        assertEquals("SUCCESS", resultRef.get().getTasks().get(0).getMessage());
        assertEquals("SUCCESS", resultRef.get().getTasks().get(1).getMessage());
        assertEquals("SUCCESS", resultRef.get().getTasks().get(2).getMessage());

        assertEquals("11", resultRef.get().getTasks().get(0).getResourceIds().get(0));
        assertEquals("12", resultRef.get().getTasks().get(0).getResourceIds().get(1));
        assertEquals("21", resultRef.get().getTasks().get(1).getResourceIds().get(0));
        assertEquals("22", resultRef.get().getTasks().get(1).getResourceIds().get(1));
        assertEquals("31", resultRef.get().getTasks().get(2).getResourceIds().get(0));
        assertEquals("32", resultRef.get().getTasks().get(2).getResourceIds().get(1));
        assertEquals("resource1", resultRef.get().getTasks().get(2).getResourceIds().get(2));

        assertEquals("11", resultRef.get().getTasks().get(0).getResourceDescriptions().get(0).getId());
        assertEquals("12", resultRef.get().getTasks().get(0).getResourceDescriptions().get(1).getId());
        assertEquals("21", resultRef.get().getTasks().get(1).getResourceDescriptions().get(0).getId());
        assertEquals("22", resultRef.get().getTasks().get(1).getResourceDescriptions().get(1).getId());
        assertEquals("31", resultRef.get().getTasks().get(2).getResourceDescriptions().get(0).getId());
        assertEquals("32", resultRef.get().getTasks().get(2).getResourceDescriptions().get(1).getId());
        assertEquals("resource1", resultRef.get().getTasks().get(2).getResourceDescriptions().get(2).getId());

        TimeUnit.MILLISECONDS.sleep(500);

        // Test what Platform Proxy receives
        assertEquals(0, dummyPlatformProxyListener.updateAcquisitionRequestsReceived());
        assertEquals(0, dummyPlatformProxyListener.startAcquisitionRequestsReceived());
        assertEquals(0, dummyPlatformProxyListener.cancelTaskRequestsReceived());

        // Check if timer is activated

        // Clear the storedResourceIds from task3 and task4
        storedTaskInfo2.getStoredResourceIds().clear();
        taskInfoRepository.save(storedTaskInfo2);
        storedTaskInfo3.getStoredResourceIds().clear();
        taskInfoRepository.save(storedTaskInfo3);

        // Wait a little more than 1 sec in order to check if stored resources are updated
        TimeUnit.MILLISECONDS.sleep(1200);

        storedTaskInfo1 = taskInfoRepository.findByTaskId("1");
        storedTaskInfo2 = taskInfoRepository.findByTaskId("2");
        storedTaskInfo3 = taskInfoRepository.findByTaskId("3");

        // Test if the stored resources
        assertEquals(2, storedTaskInfo1.getResourceIds().size());
        assertEquals(2, storedTaskInfo1.getResourceDescriptions().size());
        assertEquals(0, storedTaskInfo1.getStoredResourceIds().size());
        assertEquals(2, storedTaskInfo1.getResourceUrls().size());
        assertEquals(2, storedTaskInfo2.getResourceIds().size());
        assertEquals(2, storedTaskInfo2.getResourceDescriptions().size());
        assertEquals(3, storedTaskInfo2.getStoredResourceIds().size());
        assertEquals(2, storedTaskInfo2.getResourceUrls().size());
        assertEquals(3, storedTaskInfo3.getResourceIds().size());
        assertEquals(3, storedTaskInfo3.getResourceDescriptions().size());
        assertEquals(2, storedTaskInfo3.getStoredResourceIds().size());
        assertEquals(3, storedTaskInfo3.getResourceUrls().size());

        // Test the statuses
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, storedTaskInfo1.getStatus());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, storedTaskInfo2.getStatus());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, storedTaskInfo3.getStatus());

        // Test the message
        assertEquals("SUCCESS", storedTaskInfo1.getMessage());
        assertEquals("SUCCESS", storedTaskInfo2.getMessage());
        assertEquals("SUCCESS", storedTaskInfo3.getMessage());

        // Check the resourceIds
        assertEquals("11", storedTaskInfo1.getResourceIds().get(0));
        assertEquals("12", storedTaskInfo1.getResourceIds().get(1));
        assertEquals("21", storedTaskInfo2.getResourceIds().get(0));
        assertEquals("22", storedTaskInfo2.getResourceIds().get(1));
        assertEquals("31", storedTaskInfo3.getResourceIds().get(0));
        assertEquals("32", storedTaskInfo3.getResourceIds().get(1));
        assertEquals("resource1", storedTaskInfo3.getResourceIds().get(2));

        // Check the resourceDescriptions
        assertEquals("11", storedTaskInfo1.getResourceDescriptions().get(0).getId());
        assertEquals("12", storedTaskInfo1.getResourceDescriptions().get(1).getId());
        assertEquals("21", storedTaskInfo2.getResourceDescriptions().get(0).getId());
        assertEquals("22", storedTaskInfo2.getResourceDescriptions().get(1).getId());
        assertEquals("31", storedTaskInfo3.getResourceDescriptions().get(0).getId());
        assertEquals("32", storedTaskInfo3.getResourceDescriptions().get(1).getId());
        assertEquals("resource1", storedTaskInfo3.getResourceDescriptions().get(2).getId());

        // Check the storedResourceIds
        assertEquals("resource1", storedTaskInfo2.getStoredResourceIds().get(0));
        assertEquals("resource2", storedTaskInfo2.getStoredResourceIds().get(1));
        assertEquals("resource3", storedTaskInfo2.getStoredResourceIds().get(2));
        assertEquals("resource2", storedTaskInfo3.getStoredResourceIds().get(0));
        assertEquals("resource3", storedTaskInfo3.getStoredResourceIds().get(1));

        // Check the resourceUrls
        assertEquals(symbIoTeCoreUrl + "/Sensors('11')", storedTaskInfo1.getResourceUrls().get("11"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('12')", storedTaskInfo1.getResourceUrls().get("12"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('21')", storedTaskInfo2.getResourceUrls().get("21"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('22')", storedTaskInfo2.getResourceUrls().get("22"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('31')", storedTaskInfo3.getResourceUrls().get("31"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('32')", storedTaskInfo3.getResourceUrls().get("32"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('resource1')", storedTaskInfo3.getResourceUrls().get("resource1"));

        // Check the stored ScheduledTaskInfoUpdates
        assertNull(searchHelper.getScheduledTaskInfoUpdateMap().get(task1.getTaskId()));
        assertNotNull(searchHelper.getScheduledTaskInfoUpdateMap().get(task2.getTaskId()));
        assertNotNull(searchHelper.getScheduledTaskInfoUpdateMap().get(task3.getTaskId()));

        log.info("updateTaskWithCachingIntervalTest FINISHED!");
    }


    @Test
    public void changeInMinNoResourcesTest() throws Exception {

        log.info("changeInMinNoResourcesTest STARTED!");

        final AtomicReference<ResourceManagerUpdateResponse> resultRef = new AtomicReference<>();
        List<PlatformProxyUpdateRequest> taskUpdateRequestsReceivedByListener;

        CoreQueryRequest coreQueryRequest = new CoreQueryRequest.Builder()
                .locationName("Paris")
                .observedProperty(Arrays.asList("temperature", "humidity"))
                .shouldRank(true)
                .build();

        List<String> resourceIds = Arrays.asList("resource1", "resource2");
        List<String> storedResourceIds = Arrays.asList("3", "4");

        Map<String, String> resourceUrls1 = new HashMap<>();
        resourceUrls1.put("resource1", symbIoTeCoreUrl + "/Sensors('resource1')");
        resourceUrls1.put("resource2", symbIoTeCoreUrl + "/Sensors('resource2')");

        ArrayList<QueryResourceResult> results = new ArrayList<>();
        QueryResourceResult result1 = new QueryResourceResult();
        QueryResourceResult result2 = new QueryResourceResult();
        result1.setId("resource1");
        result2.setId("resource2");
        results.add(result1);
        results.add(result2);

        TaskInfo task1 = new TaskInfo("1", 2, coreQueryRequest, "P0-0-0T0:0:0.06",
                true, "P0-0-0T0:0:1", true, "enablerLogic", null, resourceIds, results,
                ResourceManagerTaskInfoResponseStatus.SUCCESS, storedResourceIds, resourceUrls1, "SUCCESS");
        task1.setMaxNoResources(2);
        taskInfoRepository.save(task1);

        Map<String, String> resourceUrls2 = new HashMap<>();
        resourceUrls2.put("21", symbIoTeCoreUrl + "/Sensors('21')");
        resourceUrls2.put("22", symbIoTeCoreUrl + "/Sensors('22')");
        TaskInfo task2 = new TaskInfo(task1);
        task2.setTaskId("2");
        task2.setResourceIds(Arrays.asList("21", "22"));
        task2.setResourceUrls(resourceUrls2);
        task2.getResourceDescriptions().get(0).setId("21");
        task2.getResourceDescriptions().get(1).setId("22");
        taskInfoRepository.save(task2);

        Map<String, String> resourceUrls3 = new HashMap<>();
        resourceUrls3.put("31", symbIoTeCoreUrl + "/Sensors('31')");
        resourceUrls3.put("32", symbIoTeCoreUrl + "/Sensors('32')");
        TaskInfo task3 = new TaskInfo(task1);
        task3.setTaskId("3");
        task3.setResourceIds(Arrays.asList("31", "32"));
        task3.setResourceUrls(resourceUrls3);
        task3.getResourceDescriptions().get(0).setId("31");
        task3.getResourceDescriptions().get(1).setId("32");
        taskInfoRepository.save(task3);

        Map<String, String> resourceUrls4 = new HashMap<>();
        resourceUrls4.put("41", symbIoTeCoreUrl + "/Sensors('41')");
        resourceUrls4.put("42", symbIoTeCoreUrl + "/Sensors('42')");
        TaskInfo task4 = new TaskInfo(task1);
        task4.setTaskId("4");
        task4.setResourceIds(Arrays.asList("41", "42"));
        task4.setStoredResourceIds(Arrays.asList("3", "4", "badCRAMrespose"));
        task4.setResourceUrls(resourceUrls4);
        task4.getResourceDescriptions().get(0).setId("41");
        task4.getResourceDescriptions().get(1).setId("42");
        taskInfoRepository.save(task4);

        Map<String, String> resourceUrls5 = new HashMap<>();
        resourceUrls5.put("51", symbIoTeCoreUrl + "/Sensors('51')");
        resourceUrls5.put("52", symbIoTeCoreUrl + "/Sensors('52')");
        TaskInfo task5 = new TaskInfo(task1);
        task5.setTaskId("5");
        task5.setMaxNoResources(3);
        task5.setMinNoResources(3);
        task5.setResourceIds(Arrays.asList("51", "52"));
        task5.setStoredResourceIds(new ArrayList<>());
        task5.setResourceUrls(resourceUrls5);
        task5.setStatus(ResourceManagerTaskInfoResponseStatus.NOT_ENOUGH_RESOURCES);
        task5.getResourceDescriptions().get(0).setId("51");
        task5.getResourceDescriptions().get(1).setId("52");
        taskInfoRepository.save(task5);

        // This task should not reach Platform Proxy, since minNoResources decreased
        TaskInfo updatedTask1 = new TaskInfo(task1);
        updatedTask1.setMinNoResources(1);

        // This task should reach Platform Proxy, since minNoResources increased and there were enough resources
        TaskInfo updatedTask2 = new TaskInfo(task2);
        updatedTask2.setMaxNoResources(3);
        updatedTask2.setMinNoResources(3);

        // This task should not reach Platform Proxy, since minNoResources increased and there were not enough storedResources
        TaskInfo updatedTask3 = new TaskInfo(task3);
        updatedTask3.setMaxNoResources(5);
        updatedTask3.setMinNoResources(5);

        // This task should not reach Platform Proxy, because there are not enough available resources
        TaskInfo updatedTask4 = new TaskInfo(task4);
        updatedTask4.setMaxNoResources(5);
        updatedTask4.setMinNoResources(5);

        // This task should reach Platform Proxy, because a task with status NOT_ENOUGH_RESOURCES was updated
        TaskInfo updatedTask5 = new TaskInfo(task5);
        updatedTask5.setMinNoResources(2);

        ResourceManagerUpdateRequest req = new ResourceManagerUpdateRequest();
        req.setTasks(Arrays.asList(new ResourceManagerTaskInfoRequest(updatedTask1),
                new ResourceManagerTaskInfoRequest(updatedTask2),
                new ResourceManagerTaskInfoRequest(updatedTask3),
                new ResourceManagerTaskInfoRequest(updatedTask4),
                new ResourceManagerTaskInfoRequest(updatedTask5)));


        log.info("Before sending the message");
        RabbitConverterFuture<ResourceManagerUpdateResponse> future = asyncRabbitTemplate
                .convertSendAndReceive(resourceManagerExchangeName, updateTaskRoutingKey, req);
        log.info("After sending the message");

        future.addCallback(new ListenableFutureUpdateCallback("changeInMinNoResourcesTest", resultRef));

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

        // All tasks minNoResources field has been changed
        assertNotEquals(storedTaskInfo1, task1);
        assertNotEquals(storedTaskInfo2, task2);
        assertNotEquals(storedTaskInfo3, task3);
        assertNotEquals(storedTaskInfo4, task4);
        assertNotEquals(storedTaskInfo5, task5);

        assertEquals(storedTaskInfo1, updatedTask1); // Nothing changes
        assertNotEquals(storedTaskInfo2, updatedTask2); // The storedResourcesIds are updated
        assertNotEquals(storedTaskInfo3, updatedTask3); // The status changed to NOT_ENOUGH_RESOURCES
        assertNotEquals(storedTaskInfo4, updatedTask4); // The status changed to NOT_ENOUGH_RESOURCES
        assertNotEquals(storedTaskInfo5, updatedTask5); // The status changed to SUCCESS

        // Check the statuses
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, storedTaskInfo1.getStatus());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, storedTaskInfo2.getStatus());
        assertEquals(ResourceManagerTaskInfoResponseStatus.NOT_ENOUGH_RESOURCES, storedTaskInfo3.getStatus());
        assertEquals(ResourceManagerTaskInfoResponseStatus.NOT_ENOUGH_RESOURCES, storedTaskInfo4.getStatus());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, storedTaskInfo5.getStatus());
        assertEquals("SUCCESS", storedTaskInfo1.getMessage());
        assertEquals("There were enough resources to satisfy the minNoResources. " +
                "The maximum number of resources is satisfied. ", storedTaskInfo2.getMessage());
        assertEquals("Not enough resources. Only 4 were found. ", storedTaskInfo3.getMessage());
        assertEquals("Not enough resources. Only 4 were found. ", storedTaskInfo4.getMessage());
        assertEquals("The minNoResources was decreased to 2. ", storedTaskInfo5.getMessage());

        // Check the sizes of resourceIds, storedResourceIds and resourceUrls
        assertEquals(2, storedTaskInfo1.getResourceIds().size());
        assertEquals(2, storedTaskInfo1.getResourceDescriptions().size());
        assertEquals(2, storedTaskInfo1.getStoredResourceIds().size());
        assertEquals(2, storedTaskInfo1.getResourceUrls().size());
        assertEquals(3, storedTaskInfo2.getResourceIds().size());
        assertEquals(3, storedTaskInfo2.getResourceDescriptions().size());
        assertEquals(1, storedTaskInfo2.getStoredResourceIds().size());
        assertEquals(3, storedTaskInfo2.getResourceUrls().size());
        assertEquals(4, storedTaskInfo3.getResourceIds().size());
        assertEquals(4, storedTaskInfo3.getResourceDescriptions().size());
        assertEquals(0, storedTaskInfo3.getStoredResourceIds().size());
        assertEquals(4, storedTaskInfo3.getResourceUrls().size());
        assertEquals(4, storedTaskInfo4.getResourceIds().size());
        assertEquals(4, storedTaskInfo4.getResourceDescriptions().size());
        assertEquals(0, storedTaskInfo4.getStoredResourceIds().size());
        assertEquals(4, storedTaskInfo4.getResourceUrls().size());
        assertEquals(2, storedTaskInfo5.getResourceIds().size());
        assertEquals(2, storedTaskInfo5.getResourceDescriptions().size());
        assertEquals(0, storedTaskInfo5.getStoredResourceIds().size());
        assertEquals(2, storedTaskInfo5.getResourceUrls().size());

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
        assertEquals("51", storedTaskInfo5.getResourceIds().get(0));
        assertEquals("52", storedTaskInfo5.getResourceIds().get(1));

        // Check the resourceDescriptions
        assertEquals("resource1", storedTaskInfo1.getResourceDescriptions().get(0).getId());
        assertEquals("resource2", storedTaskInfo1.getResourceDescriptions().get(1).getId());
        assertEquals("21", storedTaskInfo2.getResourceDescriptions().get(0).getId());
        assertEquals("22", storedTaskInfo2.getResourceDescriptions().get(1).getId());
        assertEquals("3", storedTaskInfo2.getResourceDescriptions().get(2).getId());
        assertEquals("31", storedTaskInfo3.getResourceDescriptions().get(0).getId());
        assertEquals("32", storedTaskInfo3.getResourceDescriptions().get(1).getId());
        assertEquals("3", storedTaskInfo3.getResourceDescriptions().get(2).getId());
        assertEquals("4", storedTaskInfo3.getResourceDescriptions().get(3).getId());
        assertEquals("41", storedTaskInfo4.getResourceDescriptions().get(0).getId());
        assertEquals("42", storedTaskInfo4.getResourceDescriptions().get(1).getId());
        assertEquals("3", storedTaskInfo4.getResourceDescriptions().get(2).getId());
        assertEquals("4", storedTaskInfo4.getResourceDescriptions().get(3).getId());
        assertEquals("51", storedTaskInfo5.getResourceDescriptions().get(0).getId());
        assertEquals("52", storedTaskInfo5.getResourceDescriptions().get(1).getId());

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
        assertEquals(symbIoTeCoreUrl + "/Sensors('51')", storedTaskInfo5.getResourceUrls().get("51"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('52')", storedTaskInfo5.getResourceUrls().get("52"));

        // Test what Enabler Logic receives
        assertEquals(5, resultRef.get().getTasks().size());
        assertEquals(ResourceManagerTasksStatus.PARTIAL_SUCCESS, resultRef.get().getStatus());
        assertEquals("Failed update tasks id : [3, 4]", resultRef.get().getMessage());
        assertEquals(2, resultRef.get().getTasks().get(0).getResourceIds().size());
        assertEquals(3, resultRef.get().getTasks().get(1).getResourceIds().size());
        assertEquals(4, resultRef.get().getTasks().get(2).getResourceIds().size());
        assertEquals(4, resultRef.get().getTasks().get(3).getResourceIds().size());
        assertEquals(2, resultRef.get().getTasks().get(4).getResourceIds().size());
        assertEquals(2, resultRef.get().getTasks().get(0).getResourceDescriptions().size());
        assertEquals(3, resultRef.get().getTasks().get(1).getResourceDescriptions().size());
        assertEquals(4, resultRef.get().getTasks().get(2).getResourceDescriptions().size());
        assertEquals(4, resultRef.get().getTasks().get(3).getResourceDescriptions().size());
        assertEquals(2, resultRef.get().getTasks().get(4).getResourceDescriptions().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, resultRef.get().getTasks().get(0).getStatus());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, resultRef.get().getTasks().get(1).getStatus());
        assertEquals(ResourceManagerTaskInfoResponseStatus.NOT_ENOUGH_RESOURCES, resultRef.get().getTasks().get(2).getStatus());
        assertEquals(ResourceManagerTaskInfoResponseStatus.NOT_ENOUGH_RESOURCES, resultRef.get().getTasks().get(3).getStatus());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, resultRef.get().getTasks().get(4).getStatus());
        assertEquals("SUCCESS", resultRef.get().getTasks().get(0).getMessage());
        assertEquals("There were enough resources to satisfy the minNoResources. " +
                "The maximum number of resources is satisfied. ", resultRef.get().getTasks().get(1).getMessage());
        assertEquals("Not enough resources. Only 4 were found. ", resultRef.get().getTasks().get(2).getMessage());
        assertEquals("Not enough resources. Only 4 were found. ", resultRef.get().getTasks().get(3).getMessage());
        assertEquals("The minNoResources was decreased to 2. ", resultRef.get().getTasks().get(4).getMessage());

        assertEquals("resource1", resultRef.get().getTasks().get(0).getResourceIds().get(0));
        assertEquals("resource2", resultRef.get().getTasks().get(0).getResourceIds().get(1));
        assertEquals("21", resultRef.get().getTasks().get(1).getResourceIds().get(0));
        assertEquals("22", resultRef.get().getTasks().get(1).getResourceIds().get(1));
        assertEquals("3", resultRef.get().getTasks().get(1).getResourceIds().get(2));
        assertEquals("31", resultRef.get().getTasks().get(2).getResourceIds().get(0));
        assertEquals("32", resultRef.get().getTasks().get(2).getResourceIds().get(1));
        assertEquals("3", resultRef.get().getTasks().get(2).getResourceIds().get(2));
        assertEquals("4", resultRef.get().getTasks().get(2).getResourceIds().get(3));
        assertEquals("41", resultRef.get().getTasks().get(3).getResourceIds().get(0));
        assertEquals("42", resultRef.get().getTasks().get(3).getResourceIds().get(1));
        assertEquals("3", resultRef.get().getTasks().get(3).getResourceIds().get(2));
        assertEquals("4", resultRef.get().getTasks().get(3).getResourceIds().get(3));
        assertEquals("51", resultRef.get().getTasks().get(4).getResourceIds().get(0));
        assertEquals("52", resultRef.get().getTasks().get(4).getResourceIds().get(1));

        assertEquals("resource1", resultRef.get().getTasks().get(0).getResourceDescriptions().get(0).getId());
        assertEquals("resource2", resultRef.get().getTasks().get(0).getResourceDescriptions().get(1).getId());
        assertEquals("21", resultRef.get().getTasks().get(1).getResourceDescriptions().get(0).getId());
        assertEquals("22", resultRef.get().getTasks().get(1).getResourceDescriptions().get(1).getId());
        assertEquals("3", resultRef.get().getTasks().get(1).getResourceDescriptions().get(2).getId());
        assertEquals("31", resultRef.get().getTasks().get(2).getResourceDescriptions().get(0).getId());
        assertEquals("32", resultRef.get().getTasks().get(2).getResourceDescriptions().get(1).getId());
        assertEquals("3", resultRef.get().getTasks().get(2).getResourceDescriptions().get(2).getId());
        assertEquals("4", resultRef.get().getTasks().get(2).getResourceDescriptions().get(3).getId());
        assertEquals("41", resultRef.get().getTasks().get(3).getResourceDescriptions().get(0).getId());
        assertEquals("42", resultRef.get().getTasks().get(3).getResourceDescriptions().get(1).getId());
        assertEquals("3", resultRef.get().getTasks().get(3).getResourceDescriptions().get(2).getId());
        assertEquals("4", resultRef.get().getTasks().get(3).getResourceDescriptions().get(3).getId());
        assertEquals("51", resultRef.get().getTasks().get(4).getResourceDescriptions().get(0).getId());
        assertEquals("52", resultRef.get().getTasks().get(4).getResourceDescriptions().get(1).getId());

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

        boolean foundTask2 = false;
        boolean foundTask5 = false;

        for (PlatformProxyUpdateRequest request : taskUpdateRequestsReceivedByListener) {

            log.info("Task id = " + request.getTaskId());

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
                foundTask2 = true;
                continue;
            }

            if (request.getTaskId().equals("5")) {
                Map<String, String> resourcesMap = new HashMap<>();
                resourcesMap.put("51", symbIoTeCoreUrl + "/Sensors('51')");
                resourcesMap.put("52", symbIoTeCoreUrl + "/Sensors('52')");
                resourcesMap.put("3", symbIoTeCoreUrl + "/Sensors('3')");

                assertEquals(2, request.getResources().size());
                assertEquals(resourcesMap.get(request.getResources().get(0).getResourceId()),
                        request.getResources().get(0).getAccessURL());
                assertEquals(resourcesMap.get(request.getResources().get(1).getResourceId()),
                        request.getResources().get(1).getAccessURL());
                foundTask5 = true;
                continue;
            }

            fail("The code should not reach here, because no other tasks should be received by the platform proxy");
        }

        assertTrue(foundTask2);
        assertTrue(foundTask5);

        log.info("changeInMinNoResourcesTest FINISHED!");
    }


    @Test
    public void changeInMaxNoResourcesTest() throws Exception {

        log.info("changeInMaxNoResourcesTest STARTED!");

        final AtomicReference<ResourceManagerUpdateResponse> resultRef = new AtomicReference<>();
        List<PlatformProxyUpdateRequest> taskUpdateRequestsReceivedByListener;

        CoreQueryRequest coreQueryRequest = new CoreQueryRequest.Builder()
                .locationName("Paris")
                .observedProperty(Arrays.asList("temperature", "humidity"))
                .shouldRank(true)
                .build();

        List<String> resourceIds = Arrays.asList("resource1", "resource2");
        List<String> storedResourceIds = Arrays.asList("3", "4");

        Map<String, String> resourceUrls1 = new HashMap<>();
        resourceUrls1.put("resource1", symbIoTeCoreUrl + "/Sensors('resource1')");
        resourceUrls1.put("resource2", symbIoTeCoreUrl + "/Sensors('resource2')");

        ArrayList<QueryResourceResult> results = new ArrayList<>();
        QueryResourceResult result1 = new QueryResourceResult();
        QueryResourceResult result2 = new QueryResourceResult();
        result1.setId("resource1");
        result2.setId("resource2");
        results.add(result1);
        results.add(result2);

        TaskInfo task1 = new TaskInfo("1", 2, coreQueryRequest, "P0-0-0T0:0:0.06",
                true, "P0-0-0T0:0:1", true, "enablerLogic", null, resourceIds, results,
                ResourceManagerTaskInfoResponseStatus.SUCCESS, storedResourceIds, resourceUrls1, "SUCCESS");
        task1.setMaxNoResources(2);
        taskInfoRepository.save(task1);

        Map<String, String> resourceUrls2 = new HashMap<>();
        resourceUrls2.put("21", symbIoTeCoreUrl + "/Sensors('21')");
        resourceUrls2.put("22", symbIoTeCoreUrl + "/Sensors('22')");
        TaskInfo task2 = new TaskInfo(task1);
        task2.setTaskId("2");
        task2.setResourceIds(Arrays.asList("21", "22"));
        task2.setResourceUrls(resourceUrls2);
        task2.getResourceDescriptions().get(0).setId("21");
        task2.getResourceDescriptions().get(1).setId("22");
        taskInfoRepository.save(task2);

        Map<String, String> resourceUrls3 = new HashMap<>();
        resourceUrls3.put("31", symbIoTeCoreUrl + "/Sensors('31')");
        resourceUrls3.put("32", symbIoTeCoreUrl + "/Sensors('32')");
        TaskInfo task3 = new TaskInfo(task1);
        task3.setTaskId("3");
        task3.setResourceIds(Arrays.asList("31", "32"));
        task3.setResourceUrls(resourceUrls3);
        task3.getResourceDescriptions().get(0).setId("31");
        task3.getResourceDescriptions().get(1).setId("32");
        taskInfoRepository.save(task3);

        // This task should not reach Platform Proxy, since maxNoResources decreased
        TaskInfo updatedTask1 = new TaskInfo(task1);
        updatedTask1.setMinNoResources(1);
        updatedTask1.setMaxNoResources(1);

        // This task should reach Platform Proxy, since maxNoResources increased and there were enough resources
        TaskInfo updatedTask2 = new TaskInfo(task2);
        updatedTask2.setMaxNoResources(3);

        // This task should not reach Platform Proxy, since maxNoResources increased and there were not enough storedResources
        TaskInfo updatedTask3 = new TaskInfo(task3);
        updatedTask3.setMaxNoResources(5);

        ResourceManagerUpdateRequest req = new ResourceManagerUpdateRequest();
        req.setTasks(Arrays.asList(new ResourceManagerTaskInfoRequest(updatedTask1),
                new ResourceManagerTaskInfoRequest(updatedTask2),
                new ResourceManagerTaskInfoRequest(updatedTask3)));


        log.info("Before sending the message");
        RabbitConverterFuture<ResourceManagerUpdateResponse> future = asyncRabbitTemplate
                .convertSendAndReceive(resourceManagerExchangeName, updateTaskRoutingKey, req);
        log.info("After sending the message");

        future.addCallback(new ListenableFutureUpdateCallback("changeInMaxNoResourcesTest", resultRef));

        while(!future.isDone()) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(100);

        TaskInfo storedTaskInfo1 = taskInfoRepository.findByTaskId("1");
        TaskInfo storedTaskInfo2 = taskInfoRepository.findByTaskId("2");
        TaskInfo storedTaskInfo3 = taskInfoRepository.findByTaskId("3");

        // All tasks minNoResources field has been changed
        assertNotEquals(storedTaskInfo1, task1);
        assertNotEquals(storedTaskInfo2, task2);
        assertNotEquals(storedTaskInfo3, task3);

        assertNotEquals(storedTaskInfo1, updatedTask1); // The resourceIds are updated
        assertNotEquals(storedTaskInfo2, updatedTask2); // The resourceIds are updated
        assertNotEquals(storedTaskInfo3, updatedTask3); // The resourceIds changed

        // Check the statuses
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, storedTaskInfo1.getStatus());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, storedTaskInfo2.getStatus());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, storedTaskInfo3.getStatus());
        assertEquals("The maxNoResources was decreased to 1, so the following resources were removed: [resource2]. ",
                storedTaskInfo1.getMessage());
        assertEquals("The maximum number of resources is satisfied. ", storedTaskInfo2.getMessage());
        assertEquals("Not enough resources are available for satisfying the maxNoResources. ", storedTaskInfo3.getMessage());

        // Check the sizes of resourceIds, storedResourceIds and resourceUrls
        assertEquals(1, storedTaskInfo1.getResourceIds().size());
        assertEquals(1, storedTaskInfo1.getResourceDescriptions().size());
        assertEquals(3, storedTaskInfo1.getStoredResourceIds().size());
        assertEquals(1, storedTaskInfo1.getResourceUrls().size());
        assertEquals(3, storedTaskInfo2.getResourceIds().size());
        assertEquals(3, storedTaskInfo2.getResourceDescriptions().size());
        assertEquals(1, storedTaskInfo2.getStoredResourceIds().size());
        assertEquals(3, storedTaskInfo2.getResourceUrls().size());
        assertEquals(4, storedTaskInfo3.getResourceIds().size());
        assertEquals(4, storedTaskInfo3.getResourceDescriptions().size());
        assertEquals(0, storedTaskInfo3.getStoredResourceIds().size());
        assertEquals(4, storedTaskInfo3.getResourceUrls().size());

        // Check the resourceIds
        assertEquals("resource1", storedTaskInfo1.getResourceIds().get(0));
        assertEquals("21", storedTaskInfo2.getResourceIds().get(0));
        assertEquals("22", storedTaskInfo2.getResourceIds().get(1));
        assertEquals("3", storedTaskInfo2.getResourceIds().get(2));
        assertEquals("31", storedTaskInfo3.getResourceIds().get(0));
        assertEquals("32", storedTaskInfo3.getResourceIds().get(1));
        assertEquals("3", storedTaskInfo3.getResourceIds().get(2));
        assertEquals("4", storedTaskInfo3.getResourceIds().get(3));

        // Check the resourceDescriptions
        assertEquals("resource1", storedTaskInfo1.getResourceDescriptions().get(0).getId());
        assertEquals("21", storedTaskInfo2.getResourceDescriptions().get(0).getId());
        assertEquals("22", storedTaskInfo2.getResourceDescriptions().get(1).getId());
        assertEquals("3", storedTaskInfo2.getResourceDescriptions().get(2).getId());
        assertEquals("31", storedTaskInfo3.getResourceDescriptions().get(0).getId());
        assertEquals("32", storedTaskInfo3.getResourceDescriptions().get(1).getId());
        assertEquals("3", storedTaskInfo3.getResourceDescriptions().get(2).getId());
        assertEquals("4", storedTaskInfo3.getResourceDescriptions().get(3).getId());

        // Check the storedResourceIds
        assertEquals("resource2", storedTaskInfo1.getStoredResourceIds().get(0));
        assertEquals("3", storedTaskInfo1.getStoredResourceIds().get(1));
        assertEquals("4", storedTaskInfo1.getStoredResourceIds().get(2));
        assertEquals("4", storedTaskInfo2.getStoredResourceIds().get(0));

        // Check the resourceUrls
        assertEquals(symbIoTeCoreUrl + "/Sensors('resource1')", storedTaskInfo1.getResourceUrls().get("resource1"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('21')", storedTaskInfo2.getResourceUrls().get("21"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('22')", storedTaskInfo2.getResourceUrls().get("22"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('3')", storedTaskInfo2.getResourceUrls().get("3"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('31')", storedTaskInfo3.getResourceUrls().get("31"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('32')", storedTaskInfo3.getResourceUrls().get("32"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('3')", storedTaskInfo3.getResourceUrls().get("3"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('4')", storedTaskInfo3.getResourceUrls().get("4"));

        // Test what Enabler Logic receives
        assertEquals(3, resultRef.get().getTasks().size());
        assertEquals(ResourceManagerTasksStatus.SUCCESS, resultRef.get().getStatus());
        assertEquals("ALL the update task requests were successful!", resultRef.get().getMessage());
        assertEquals(1, resultRef.get().getTasks().get(0).getResourceIds().size());
        assertEquals(3, resultRef.get().getTasks().get(1).getResourceIds().size());
        assertEquals(4, resultRef.get().getTasks().get(2).getResourceIds().size());
        assertEquals(1, resultRef.get().getTasks().get(0).getResourceDescriptions().size());
        assertEquals(3, resultRef.get().getTasks().get(1).getResourceDescriptions().size());
        assertEquals(4, resultRef.get().getTasks().get(2).getResourceDescriptions().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, resultRef.get().getTasks().get(0).getStatus());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, resultRef.get().getTasks().get(1).getStatus());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, resultRef.get().getTasks().get(2).getStatus());
        assertEquals("The maxNoResources was decreased to 1, so the following resources were removed: [resource2]. ",
                resultRef.get().getTasks().get(0).getMessage());
        assertEquals("The maximum number of resources is satisfied. ",
                resultRef.get().getTasks().get(1).getMessage());
        assertEquals("Not enough resources are available for satisfying the maxNoResources. ", resultRef.get().getTasks().get(2).getMessage());

        assertEquals("resource1", resultRef.get().getTasks().get(0).getResourceIds().get(0));
        assertEquals("21", resultRef.get().getTasks().get(1).getResourceIds().get(0));
        assertEquals("22", resultRef.get().getTasks().get(1).getResourceIds().get(1));
        assertEquals("3", resultRef.get().getTasks().get(1).getResourceIds().get(2));
        assertEquals("31", resultRef.get().getTasks().get(2).getResourceIds().get(0));
        assertEquals("32", resultRef.get().getTasks().get(2).getResourceIds().get(1));
        assertEquals("3", resultRef.get().getTasks().get(2).getResourceIds().get(2));
        assertEquals("4", resultRef.get().getTasks().get(2).getResourceIds().get(3));

        assertEquals("resource1", resultRef.get().getTasks().get(0).getResourceDescriptions().get(0).getId());
        assertEquals("21", resultRef.get().getTasks().get(1).getResourceDescriptions().get(0).getId());
        assertEquals("22", resultRef.get().getTasks().get(1).getResourceDescriptions().get(1).getId());
        assertEquals("3", resultRef.get().getTasks().get(1).getResourceDescriptions().get(2).getId());
        assertEquals("31", resultRef.get().getTasks().get(2).getResourceDescriptions().get(0).getId());
        assertEquals("32", resultRef.get().getTasks().get(2).getResourceDescriptions().get(1).getId());
        assertEquals("3", resultRef.get().getTasks().get(2).getResourceDescriptions().get(2).getId());
        assertEquals("4", resultRef.get().getTasks().get(2).getResourceDescriptions().get(3).getId());

        while (dummyPlatformProxyListener.updateAcquisitionRequestsReceived() < 3) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(300);

        // Test what Platform Proxy receives

        taskUpdateRequestsReceivedByListener = dummyPlatformProxyListener.getUpdateAcquisitionRequestsReceivedByListener();

        assertEquals(3, dummyPlatformProxyListener.updateAcquisitionRequestsReceived());
        assertEquals(0, dummyPlatformProxyListener.startAcquisitionRequestsReceived());
        assertEquals(0, dummyPlatformProxyListener.cancelTaskRequestsReceived());

        boolean foundTask1 = false;
        boolean foundTask2 = false;
        boolean foundTask3 = false;

        for (PlatformProxyUpdateRequest request : taskUpdateRequestsReceivedByListener) {

            log.info("Task id = " + request.getTaskId());

            if (request.getTaskId().equals("1")) {

                assertEquals(1, request.getResources().size());
                assertEquals(symbIoTeCoreUrl + "/Sensors('resource1')",
                        request.getResources().get(0).getAccessURL());
                foundTask1 = true;
                continue;
            }

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
                foundTask2 = true;
                continue;
            }

            if (request.getTaskId().equals("3")) {
                Map<String, String> resourcesMap = new HashMap<>();
                resourcesMap.put("31", symbIoTeCoreUrl + "/Sensors('31')");
                resourcesMap.put("32", symbIoTeCoreUrl + "/Sensors('32')");
                resourcesMap.put("3", symbIoTeCoreUrl + "/Sensors('3')");
                resourcesMap.put("4", symbIoTeCoreUrl + "/Sensors('4')");

                assertEquals(4, request.getResources().size());
                assertEquals(resourcesMap.get(request.getResources().get(0).getResourceId()),
                        request.getResources().get(0).getAccessURL());
                assertEquals(resourcesMap.get(request.getResources().get(1).getResourceId()),
                        request.getResources().get(1).getAccessURL());
                assertEquals(resourcesMap.get(request.getResources().get(2).getResourceId()),
                        request.getResources().get(2).getAccessURL());
                assertEquals(resourcesMap.get(request.getResources().get(3).getResourceId()),
                        request.getResources().get(3).getAccessURL());
                foundTask3 = true;
                continue;
            }

            fail("The code should not reach here, because no other tasks should be received by the platform proxy");
        }

        assertTrue(foundTask1);
        assertTrue(foundTask2);
        assertTrue(foundTask3);

        log.info("changeInMaxNoResourcesTest FINISHED!");
    }
}