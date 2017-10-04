package eu.h2020.symbiote.enabler.resourcemanager.integration.utils;

import eu.h2020.symbiote.core.internal.CoreQueryRequest;
import eu.h2020.symbiote.enabler.messaging.model.*;
import eu.h2020.symbiote.enabler.resourcemanager.integration.dummyListeners.DummyEnablerLogicListener;
import eu.h2020.symbiote.enabler.resourcemanager.integration.dummyListeners.DummyPlatformProxyListener;
import eu.h2020.symbiote.enabler.resourcemanager.model.TaskInfo;
import eu.h2020.symbiote.enabler.resourcemanager.repository.TaskInfoRepository;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Created by vasgl on 7/26/2017.
 */
public class ProblematicResourcesTestHelper {

    private static Log log = LogFactory
            .getLog(ProblematicResourcesTestHelper.class);

    private ProblematicResourcesTestHelper() {
        // empty constructor
    }

    public static void enoughReplaceableResourcesTest(String routingKey, TaskInfoRepository taskInfoRepository,
                                                                         RabbitTemplate rabbitTemplate,
                                                                         DummyPlatformProxyListener dummyPlatformProxyListener,
                                                                         DummyEnablerLogicListener dummyEnablerLogicListener,
                                                                         String resourceManagerExchangeName,
                                                                         String symbIoTeCoreUrl) throws Exception {

        List<PlatformProxyUpdateRequest> updateRequestsReceivedByPlatformProxy;
        List<ResourcesUpdated> updateRequestsReceivedByEnablerLogic;

        List<String> resourceIds = new ArrayList(Arrays.asList("1", "2", "3"));
        List<String> storedResourceIds = new ArrayList(Arrays.asList("4", "5", "6", "badCRAMrespose", "noCRAMurl", "7", "8"));

        Map<String, String> resourceUrls = new HashMap<>();
        resourceUrls.put("1", symbIoTeCoreUrl + "/Sensors('1')");
        resourceUrls.put("2", symbIoTeCoreUrl + "/Sensors('2')");
        resourceUrls.put("3", symbIoTeCoreUrl + "/Sensors('3')");

        TaskInfo taskInfo = new TaskInfo("task1", 5, new CoreQueryRequest(), "P0-0-0T0:0:0.1",
                true, "P0-0-0T0:0:0.1", true,
                "testEnablerLogic", null, resourceIds,
                ResourceManagerTaskInfoResponseStatus.SUCCESS, storedResourceIds, resourceUrls);
        taskInfoRepository.save(taskInfo);

        ProblematicResourcesInfo problematicResourcesInfo = new ProblematicResourcesInfo();
        problematicResourcesInfo.setTaskId("task1");
        problematicResourcesInfo.setProblematicResourceIds(Arrays.asList("1", "3"));

        ProblematicResourcesInfo problematicResourcesInfo2 = new ProblematicResourcesInfo();
        problematicResourcesInfo2.setTaskId("task2");
        problematicResourcesInfo2.setProblematicResourceIds(Arrays.asList("resource4"));

        ProblematicResourcesMessage problematicResourcesMessage = new ProblematicResourcesMessage();
        problematicResourcesMessage.setProblematicResourcesInfoList(Arrays.asList(problematicResourcesInfo, problematicResourcesInfo2));

        log.info("Before sending the message");
        rabbitTemplate.convertAndSend(resourceManagerExchangeName, routingKey, problematicResourcesMessage);
        log.info("After sending the message");

        while(dummyPlatformProxyListener.updateAcquisitionRequestsReceived() != 1 &&
                dummyEnablerLogicListener.updateResourcesReceived()!= 1) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(100);

        // Test what is stored in the database
        taskInfo = taskInfoRepository.findByTaskId("task2");
        assertEquals(null, taskInfo);

        taskInfo = taskInfoRepository.findByTaskId("task1");
        assertEquals(5, taskInfo.getResourceIds().size());
        assertEquals(1, taskInfo.getStoredResourceIds().size());
        assertEquals(5, taskInfo.getResourceUrls().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, taskInfo.getStatus());

        assertEquals("2", taskInfo.getResourceIds().get(0));
        assertEquals("4", taskInfo.getResourceIds().get(1));
        assertEquals("5", taskInfo.getResourceIds().get(2));
        assertEquals("6", taskInfo.getResourceIds().get(3));
        assertEquals("7", taskInfo.getResourceIds().get(4));

        assertEquals("8", taskInfo.getStoredResourceIds().get(0));

        assertEquals(symbIoTeCoreUrl + "/Sensors('2')", taskInfo.getResourceUrls().get("2"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('4')", taskInfo.getResourceUrls().get("4"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('5')", taskInfo.getResourceUrls().get("5"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('6')", taskInfo.getResourceUrls().get("6"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('7')", taskInfo.getResourceUrls().get("7"));

        // Test what Platform Proxy receives
        updateRequestsReceivedByPlatformProxy = dummyPlatformProxyListener.getUpdateAcquisitionRequestsReceivedByListener();
        assertEquals(1, updateRequestsReceivedByPlatformProxy.size());
        assertEquals("task1", updateRequestsReceivedByPlatformProxy.get(0).getTaskId());
        assertEquals(5, updateRequestsReceivedByPlatformProxy.get(0).getResources().size());

        boolean foundResource2 = false;
        boolean foundResource4 = false;
        boolean foundResource5 = false;
        boolean foundResource6 = false;
        boolean foundResource7 = false;

        for (PlatformProxyResourceInfo resource : updateRequestsReceivedByPlatformProxy.get(0).getResources()) {

            log.info("Resource id = " + resource.getResourceId());

            if (resource.getResourceId().equals("2")) {
                assertEquals(symbIoTeCoreUrl + "/Sensors('2')", resource.getAccessURL());
                foundResource2 = true;
                continue;
            }

            if (resource.getResourceId().equals("4")) {
                assertEquals(symbIoTeCoreUrl + "/Sensors('4')", resource.getAccessURL());
                foundResource4 = true;
                continue;
            }

            if (resource.getResourceId().equals("5")) {
                assertEquals(symbIoTeCoreUrl + "/Sensors('5')", resource.getAccessURL());
                foundResource5 = true;
                continue;
            }

            if (resource.getResourceId().equals("6")) {
                assertEquals(symbIoTeCoreUrl + "/Sensors('6')", resource.getAccessURL());
                foundResource6 = true;
                continue;
            }

            if (resource.getResourceId().equals("7")) {
                assertEquals(symbIoTeCoreUrl + "/Sensors('7')", resource.getAccessURL());
                foundResource7 = true;
                continue;
            }

            fail("The code should not reach here, because no other resources should be present");
        }

        assertEquals(true, foundResource2);
        assertEquals(true, foundResource4);
        assertEquals(true, foundResource5);
        assertEquals(true, foundResource6);
        assertEquals(true, foundResource7);

        // Test what Enabler Logic receives
        updateRequestsReceivedByEnablerLogic = dummyEnablerLogicListener.getUpdateResourcesReceivedByListener();
        assertEquals(1, updateRequestsReceivedByEnablerLogic.size());
        assertEquals(5, updateRequestsReceivedByEnablerLogic.get(0).getNewResources().size());
        assertEquals("2", updateRequestsReceivedByEnablerLogic.get(0).getNewResources().get(0));
        assertEquals("4", updateRequestsReceivedByEnablerLogic.get(0).getNewResources().get(1));
        assertEquals("5", updateRequestsReceivedByEnablerLogic.get(0).getNewResources().get(2));
        assertEquals("6", updateRequestsReceivedByEnablerLogic.get(0).getNewResources().get(3));
        assertEquals("7", updateRequestsReceivedByEnablerLogic.get(0).getNewResources().get(4));
    }

    public static void enoughReplaceableResourcesNoCachingTest(String routingKey, TaskInfoRepository taskInfoRepository,
                                                      RabbitTemplate rabbitTemplate,
                                                      DummyPlatformProxyListener dummyPlatformProxyListener,
                                                      DummyEnablerLogicListener dummyEnablerLogicListener,
                                                      String resourceManagerExchangeName,
                                                      String symbIoTeCoreUrl) throws Exception {

        List<PlatformProxyUpdateRequest> updateRequestsReceivedByPlatformProxy;
        List<ResourcesUpdated> updateRequestsReceivedByEnablerLogic;

        CoreQueryRequest coreQueryRequest = new CoreQueryRequest.Builder()
                .locationName("Paris")
                .observedProperty(Arrays.asList("temperature", "humidity"))
                .build();

        List<String> resourceIds = new ArrayList(Arrays.asList("1", "2", "3"));
        List<String> storedResourceIds = new ArrayList(Arrays.asList("4", "5", "6", "badCRAMrespose", "noCRAMurl", "7", "8"));

        Map<String, String> resourceUrls = new HashMap<>();
        resourceUrls.put("1", symbIoTeCoreUrl + "/Sensors('1')");
        resourceUrls.put("2", symbIoTeCoreUrl + "/Sensors('2')");
        resourceUrls.put("3", symbIoTeCoreUrl + "/Sensors('3')");

        TaskInfo taskInfo = new TaskInfo("task1", 2, coreQueryRequest, "P0-0-0T0:0:0.1",
                false, "P0-0-0T0:0:0.1", true,
                "testEnablerLogic", null, resourceIds,
                ResourceManagerTaskInfoResponseStatus.SUCCESS, storedResourceIds, resourceUrls);
        taskInfoRepository.save(taskInfo);

        ProblematicResourcesInfo problematicResourcesInfo = new ProblematicResourcesInfo();
        problematicResourcesInfo.setTaskId("task1");
        problematicResourcesInfo.setProblematicResourceIds(Arrays.asList("1", "3"));

        ProblematicResourcesInfo problematicResourcesInfo2 = new ProblematicResourcesInfo();
        problematicResourcesInfo2.setTaskId("task2");
        problematicResourcesInfo2.setProblematicResourceIds(Arrays.asList("resource4"));

        ProblematicResourcesMessage problematicResourcesMessage = new ProblematicResourcesMessage();
        problematicResourcesMessage.setProblematicResourcesInfoList(Arrays.asList(problematicResourcesInfo, problematicResourcesInfo2));

        log.info("Before sending the message");
        rabbitTemplate.convertAndSend(resourceManagerExchangeName, routingKey, problematicResourcesMessage);
        log.info("After sending the message");

        while(dummyPlatformProxyListener.updateAcquisitionRequestsReceived() != 1 &&
                dummyEnablerLogicListener.updateResourcesReceived()!= 1) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(100);

        // Test what is stored in the database
        taskInfo = taskInfoRepository.findByTaskId("task2");
        assertEquals(null, taskInfo);

        taskInfo = taskInfoRepository.findByTaskId("task1");
        assertEquals(2, taskInfo.getResourceIds().size());
        assertEquals(0, taskInfo.getStoredResourceIds().size());
        assertEquals(2, taskInfo.getResourceUrls().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, taskInfo.getStatus());

        assertEquals("resource1", taskInfo.getResourceIds().get(0));
        assertEquals("resource2", taskInfo.getResourceIds().get(1));

        assertEquals(symbIoTeCoreUrl + "/Sensors('resource1')", taskInfo.getResourceUrls().get("resource1"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('resource2')", taskInfo.getResourceUrls().get("resource2"));

        // Test what Platform Proxy receives
        updateRequestsReceivedByPlatformProxy = dummyPlatformProxyListener.getUpdateAcquisitionRequestsReceivedByListener();
        assertEquals(1, updateRequestsReceivedByPlatformProxy.size());
        assertEquals("task1", updateRequestsReceivedByPlatformProxy.get(0).getTaskId());
        assertEquals(2, updateRequestsReceivedByPlatformProxy.get(0).getResources().size());

        boolean foundResource1 = false;
        boolean foundResource2 = false;

        for (PlatformProxyResourceInfo resource : updateRequestsReceivedByPlatformProxy.get(0).getResources()) {

            log.info("Resource id = " + resource.getResourceId());

            if (resource.getResourceId().equals("resource1")) {
                assertEquals(symbIoTeCoreUrl + "/Sensors('resource1')", resource.getAccessURL());
                foundResource1 = true;
                continue;
            }

            if (resource.getResourceId().equals("resource2")) {
                assertEquals(symbIoTeCoreUrl + "/Sensors('resource2')", resource.getAccessURL());
                foundResource2 = true;
                continue;
            }

            fail("The code should not reach here, because no other resources should be present");
        }

        assertEquals(true, foundResource1);
        assertEquals(true, foundResource2);

        // Test what Enabler Logic receives
        updateRequestsReceivedByEnablerLogic = dummyEnablerLogicListener.getUpdateResourcesReceivedByListener();
        assertEquals(1, updateRequestsReceivedByEnablerLogic.size());
        assertEquals(2, updateRequestsReceivedByEnablerLogic.get(0).getNewResources().size());
        assertEquals("resource1", updateRequestsReceivedByEnablerLogic.get(0).getNewResources().get(0));
        assertEquals("resource2", updateRequestsReceivedByEnablerLogic.get(0).getNewResources().get(1));
    }

    public static void enoughRemainingResourcesTest(String routingKey, TaskInfoRepository taskInfoRepository,
                                                      RabbitTemplate rabbitTemplate,
                                                      DummyPlatformProxyListener dummyPlatformProxyListener,
                                                      DummyEnablerLogicListener dummyEnablerLogicListener,
                                                      String resourceManagerExchangeName,
                                                      String symbIoTeCoreUrl) throws Exception {

        List<PlatformProxyUpdateRequest> updateRequestsReceivedByPlatformProxy;
        List<ResourcesUpdated> updateRequestsReceivedByEnablerLogic;

        List<String> resourceIds = new ArrayList(Arrays.asList("1", "2", "3"));
        List<String> storedResourceIds = new ArrayList(Arrays.asList("4", "5", "6", "badCRAMrespose", "noCRAMurl", "7", "8"));

        Map<String, String> resourceUrls = new HashMap<>();
        resourceUrls.put("1", symbIoTeCoreUrl + "/Sensors('1')");
        resourceUrls.put("2", symbIoTeCoreUrl + "/Sensors('2')");
        resourceUrls.put("3", symbIoTeCoreUrl + "/Sensors('3')");

        TaskInfo taskInfo = new TaskInfo("task1", 1, new CoreQueryRequest(), "P0-0-0T0:0:0.1",
                true, "P0-0-0T0:0:0.1", true,
                "testEnablerLogic", null, resourceIds,
                ResourceManagerTaskInfoResponseStatus.SUCCESS, storedResourceIds, resourceUrls);
        taskInfoRepository.save(taskInfo);

        ProblematicResourcesInfo problematicResourcesInfo = new ProblematicResourcesInfo();
        problematicResourcesInfo.setTaskId("task1");
        problematicResourcesInfo.setProblematicResourceIds(Arrays.asList("1", "3"));

        ProblematicResourcesInfo problematicResourcesInfo2 = new ProblematicResourcesInfo();
        problematicResourcesInfo2.setTaskId("task2");
        problematicResourcesInfo2.setProblematicResourceIds(Arrays.asList("resource4"));

        ProblematicResourcesMessage problematicResourcesMessage = new ProblematicResourcesMessage();
        problematicResourcesMessage.setProblematicResourcesInfoList(Arrays.asList(problematicResourcesInfo, problematicResourcesInfo2));

        log.info("Before sending the message");
        rabbitTemplate.convertAndSend(resourceManagerExchangeName, routingKey, problematicResourcesMessage);
        log.info("After sending the message");

        while(dummyPlatformProxyListener.updateAcquisitionRequestsReceived() != 1 &&
                dummyEnablerLogicListener.updateResourcesReceived()!= 1) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(100);

        // Test what is stored in the database
        taskInfo = taskInfoRepository.findByTaskId("task2");
        assertEquals(null, taskInfo);

        taskInfo = taskInfoRepository.findByTaskId("task1");
        assertEquals(1, taskInfo.getResourceIds().size());
        assertEquals(7, taskInfo.getStoredResourceIds().size());
        assertEquals(1, taskInfo.getResourceUrls().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, taskInfo.getStatus());

        assertEquals("2", taskInfo.getResourceIds().get(0));

        assertEquals("4", taskInfo.getStoredResourceIds().get(0));
        assertEquals("5", taskInfo.getStoredResourceIds().get(1));
        assertEquals("6", taskInfo.getStoredResourceIds().get(2));
        assertEquals("badCRAMrespose", taskInfo.getStoredResourceIds().get(3));
        assertEquals("noCRAMurl", taskInfo.getStoredResourceIds().get(4));
        assertEquals("7", taskInfo.getStoredResourceIds().get(5));
        assertEquals("8", taskInfo.getStoredResourceIds().get(6));

        assertEquals(symbIoTeCoreUrl + "/Sensors('2')", taskInfo.getResourceUrls().get("2"));

        // Test what Platform Proxy receives
        updateRequestsReceivedByPlatformProxy = dummyPlatformProxyListener.getUpdateAcquisitionRequestsReceivedByListener();
        assertEquals(1, updateRequestsReceivedByPlatformProxy.size());
        assertEquals("task1", updateRequestsReceivedByPlatformProxy.get(0).getTaskId());
        assertEquals(1, updateRequestsReceivedByPlatformProxy.get(0).getResources().size());
        assertEquals("2", updateRequestsReceivedByPlatformProxy.get(0).getResources().get(0).getResourceId());
        assertEquals(symbIoTeCoreUrl + "/Sensors('2')", updateRequestsReceivedByPlatformProxy.get(0).getResources().get(0).getAccessURL());

        // Test what Enabler Logic receives
        updateRequestsReceivedByEnablerLogic = dummyEnablerLogicListener.getUpdateResourcesReceivedByListener();
        assertEquals(1, updateRequestsReceivedByEnablerLogic.size());
        assertEquals(1, updateRequestsReceivedByEnablerLogic.get(0).getNewResources().size());
        assertEquals("2", updateRequestsReceivedByEnablerLogic.get(0).getNewResources().get(0));
    }

    public static void notEnoughResourcesTest(String routingKey, TaskInfoRepository taskInfoRepository,
                                                                            RabbitTemplate rabbitTemplate,
                                                                            DummyPlatformProxyListener dummyPlatformProxyListener,
                                                                            DummyEnablerLogicListener dummyEnablerLogicListener,
                                                                            String resourceManagerExchangeName,
                                                                            String symbIoTeCoreUrl) throws Exception {

        List<PlatformProxyUpdateRequest> updateRequestsReceivedByPlatformProxy;
        List<NotEnoughResourcesAvailable> notEnoughResourcesMessagesReceived;

        List<String> resourceIds = new ArrayList(Arrays.asList("1", "2", "3"));
        List<String> storedResourceIds = new ArrayList(Arrays.asList("4", "5", "badCRAMrespose"));

        Map<String, String> resourceUrls = new HashMap<>();
        resourceUrls.put("1", symbIoTeCoreUrl + "/Sensors('1')");
        resourceUrls.put("2", symbIoTeCoreUrl + "/Sensors('2')");
        resourceUrls.put("3", symbIoTeCoreUrl + "/Sensors('3')");

        TaskInfo taskInfo = new TaskInfo("task1", 5, new CoreQueryRequest(), "P0-0-0T0:0:0.1",
                true, "P0-0-0T0:0:0.1", true,
                "testEnablerLogic", null, resourceIds,
                ResourceManagerTaskInfoResponseStatus.SUCCESS, storedResourceIds, resourceUrls);
        taskInfoRepository.save(taskInfo);

        ProblematicResourcesInfo problematicResourcesInfo = new ProblematicResourcesInfo();
        problematicResourcesInfo.setTaskId("task1");
        problematicResourcesInfo.setProblematicResourceIds(Arrays.asList("1", "3"));

        ProblematicResourcesInfo problematicResourcesInfo2 = new ProblematicResourcesInfo();
        problematicResourcesInfo2.setTaskId("task2");
        problematicResourcesInfo2.setProblematicResourceIds(Arrays.asList("resource4"));

        ProblematicResourcesMessage problematicResourcesMessage = new ProblematicResourcesMessage();
        problematicResourcesMessage.setProblematicResourcesInfoList(Arrays.asList(problematicResourcesInfo, problematicResourcesInfo2));

        log.info("Before sending the message");
        rabbitTemplate.convertAndSend(resourceManagerExchangeName, routingKey, problematicResourcesMessage);
        log.info("After sending the message");

        while(dummyEnablerLogicListener.notEnoughResourcesMessagesReceived()!= 1) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(100);

        // Test what is stored in the database
        taskInfo = taskInfoRepository.findByTaskId("task2");
        assertEquals(null, taskInfo);

        taskInfo = taskInfoRepository.findByTaskId("task1");
        assertEquals(3, taskInfo.getResourceIds().size());
        assertEquals(0, taskInfo.getStoredResourceIds().size());
        assertEquals(3, taskInfo.getResourceUrls().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.NOT_ENOUGH_RESOURCES, taskInfo.getStatus());

        assertEquals("2", taskInfo.getResourceIds().get(0));
        assertEquals("4", taskInfo.getResourceIds().get(1));
        assertEquals("5", taskInfo.getResourceIds().get(2));

        assertEquals(symbIoTeCoreUrl + "/Sensors('2')", taskInfo.getResourceUrls().get("2"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('4')", taskInfo.getResourceUrls().get("4"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('5')", taskInfo.getResourceUrls().get("5"));

        // Test what Enabler Logic receives
        notEnoughResourcesMessagesReceived = dummyEnablerLogicListener.getNotEnoughResourcesMessagesReceivedByListener();
        assertEquals(1, notEnoughResourcesMessagesReceived.size());
        assertEquals("task1", notEnoughResourcesMessagesReceived.get(0).getTaskId());
        assertEquals(3, (int) notEnoughResourcesMessagesReceived.get(0).getNoResourcesAcquired());

        // Test what Platform Proxy receives
        updateRequestsReceivedByPlatformProxy = dummyPlatformProxyListener.getUpdateAcquisitionRequestsReceivedByListener();
        assertEquals(0, updateRequestsReceivedByPlatformProxy.size());
    }

    public static void notEnoughResourcesNoCachingTest(String routingKey, TaskInfoRepository taskInfoRepository,
                                              RabbitTemplate rabbitTemplate,
                                              DummyPlatformProxyListener dummyPlatformProxyListener,
                                              DummyEnablerLogicListener dummyEnablerLogicListener,
                                              String resourceManagerExchangeName,
                                              String symbIoTeCoreUrl) throws Exception {

        List<PlatformProxyUpdateRequest> updateRequestsReceivedByPlatformProxy;
        List<NotEnoughResourcesAvailable> notEnoughResourcesMessagesReceived;

        CoreQueryRequest coreQueryRequest = new CoreQueryRequest.Builder()
                .locationName("Athens")
                .observedProperty(Arrays.asList("temperature", "humidity"))
                .build();

        List<String> resourceIds = new ArrayList(Arrays.asList("1", "2", "3"));
        List<String> storedResourceIds = new ArrayList(Arrays.asList("4", "5", "6", "badCRAMrespose", "noCRAMurl", "7", "8"));

        Map<String, String> resourceUrls = new HashMap<>();
        resourceUrls.put("1", symbIoTeCoreUrl + "/Sensors('1')");
        resourceUrls.put("2", symbIoTeCoreUrl + "/Sensors('2')");
        resourceUrls.put("3", symbIoTeCoreUrl + "/Sensors('3')");

        TaskInfo taskInfo = new TaskInfo("task1", 3, coreQueryRequest, "P0-0-0T0:0:0.1",
                false, "P0-0-0T0:0:0.1", true,
                "testEnablerLogic", null, resourceIds,
                ResourceManagerTaskInfoResponseStatus.SUCCESS, storedResourceIds, resourceUrls);
        taskInfoRepository.save(taskInfo);

        ProblematicResourcesInfo problematicResourcesInfo = new ProblematicResourcesInfo();
        problematicResourcesInfo.setTaskId("task1");
        problematicResourcesInfo.setProblematicResourceIds(Arrays.asList("1", "3"));

        ProblematicResourcesInfo problematicResourcesInfo2 = new ProblematicResourcesInfo();
        problematicResourcesInfo2.setTaskId("task2");
        problematicResourcesInfo2.setProblematicResourceIds(Arrays.asList("resource4"));

        ProblematicResourcesMessage problematicResourcesMessage = new ProblematicResourcesMessage();
        problematicResourcesMessage.setProblematicResourcesInfoList(Arrays.asList(problematicResourcesInfo, problematicResourcesInfo2));

        log.info("Before sending the message");
        rabbitTemplate.convertAndSend(resourceManagerExchangeName, routingKey, problematicResourcesMessage);
        log.info("After sending the message");

        while(dummyEnablerLogicListener.notEnoughResourcesMessagesReceived()!= 1) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(100);

        // Test what is stored in the database
        taskInfo = taskInfoRepository.findByTaskId("task2");
        assertEquals(null, taskInfo);

        taskInfo = taskInfoRepository.findByTaskId("task1");
        assertEquals(1, taskInfo.getResourceIds().size());
        assertEquals(0, taskInfo.getStoredResourceIds().size());
        assertEquals(1, taskInfo.getResourceUrls().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.NOT_ENOUGH_RESOURCES, taskInfo.getStatus());

        assertEquals("2", taskInfo.getResourceIds().get(0));

        assertEquals(symbIoTeCoreUrl + "/Sensors('2')", taskInfo.getResourceUrls().get("2"));

        // Test what Enabler Logic receives
        notEnoughResourcesMessagesReceived = dummyEnablerLogicListener.getNotEnoughResourcesMessagesReceivedByListener();
        assertEquals(1, notEnoughResourcesMessagesReceived.size());
        assertEquals("task1", notEnoughResourcesMessagesReceived.get(0).getTaskId());
        assertEquals(1, (int) notEnoughResourcesMessagesReceived.get(0).getNoResourcesAcquired());

        // Test what Platform Proxy receives
        updateRequestsReceivedByPlatformProxy = dummyPlatformProxyListener.getUpdateAcquisitionRequestsReceivedByListener();
        assertEquals(0, updateRequestsReceivedByPlatformProxy.size());
    }

    public static void enoughStoredOnlyResourcesTest(String routingKey, TaskInfoRepository taskInfoRepository,
                                                                                   RabbitTemplate rabbitTemplate,
                                                                                   DummyPlatformProxyListener dummyPlatformProxyListener,
                                                                                   DummyEnablerLogicListener dummyEnablerLogicListener,
                                                                                   String resourceManagerExchangeName,
                                                                                   String symbIoTeCoreUrl) throws Exception {

        List<PlatformProxyUpdateRequest> updateRequestsReceivedByPlatformProxy;
        List<NotEnoughResourcesAvailable> notEnoughResourcesMessagesReceived;

        List<String> resourceIds = new ArrayList(Arrays.asList("1", "2", "3"));
        List<String> storedResourceIds = new ArrayList(Arrays.asList("4", "5", "6", "badCRAMrespose", "noCRAMurl"));

        Map<String, String> resourceUrls = new HashMap<>();
        resourceUrls.put("1", symbIoTeCoreUrl + "/Sensors('1')");
        resourceUrls.put("2", symbIoTeCoreUrl + "/Sensors('2')");
        resourceUrls.put("3", symbIoTeCoreUrl + "/Sensors('3')");

        TaskInfo taskInfo = new TaskInfo("task1", 5, new CoreQueryRequest(), "P0-0-0T0:0:0.1",
                true, "P0-0-0T0:0:0.1", true,
                "testEnablerLogic", null, resourceIds,
                ResourceManagerTaskInfoResponseStatus.SUCCESS, storedResourceIds, resourceUrls);
        taskInfoRepository.save(taskInfo);

        ProblematicResourcesInfo problematicResourcesInfo = new ProblematicResourcesInfo();
        problematicResourcesInfo.setTaskId("task1");
        problematicResourcesInfo.setProblematicResourceIds(Arrays.asList("1", "3"));

        ProblematicResourcesInfo problematicResourcesInfo2 = new ProblematicResourcesInfo();
        problematicResourcesInfo2.setTaskId("task2");
        problematicResourcesInfo2.setProblematicResourceIds(Arrays.asList("resource4"));

        ProblematicResourcesMessage problematicResourcesMessage = new ProblematicResourcesMessage();
        problematicResourcesMessage.setProblematicResourcesInfoList(Arrays.asList(problematicResourcesInfo, problematicResourcesInfo2));

        log.info("Before sending the message");
        rabbitTemplate.convertAndSend(resourceManagerExchangeName, routingKey, problematicResourcesMessage);
        log.info("After sending the message");

        while(dummyEnablerLogicListener.notEnoughResourcesMessagesReceived()!= 1) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(100);

        // Test what is stored in the database
        taskInfo = taskInfoRepository.findByTaskId("task2");
        assertEquals(null, taskInfo);

        taskInfo = taskInfoRepository.findByTaskId("task1");
        assertEquals(4, taskInfo.getResourceIds().size());
        assertEquals(0, taskInfo.getStoredResourceIds().size());
        assertEquals(4, taskInfo.getResourceUrls().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.NOT_ENOUGH_RESOURCES, taskInfo.getStatus());

        assertEquals("2", taskInfo.getResourceIds().get(0));
        assertEquals("4", taskInfo.getResourceIds().get(1));
        assertEquals("5", taskInfo.getResourceIds().get(2));
        assertEquals("6", taskInfo.getResourceIds().get(3));

        assertEquals(symbIoTeCoreUrl + "/Sensors('2')", taskInfo.getResourceUrls().get("2"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('4')", taskInfo.getResourceUrls().get("4"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('5')", taskInfo.getResourceUrls().get("5"));
        assertEquals(symbIoTeCoreUrl + "/Sensors('6')", taskInfo.getResourceUrls().get("6"));

        // Test what Enabler Logic receives
        notEnoughResourcesMessagesReceived = dummyEnablerLogicListener.getNotEnoughResourcesMessagesReceivedByListener();
        assertEquals(1, notEnoughResourcesMessagesReceived.size());
        assertEquals("task1", notEnoughResourcesMessagesReceived.get(0).getTaskId());
        assertEquals(4, (int) notEnoughResourcesMessagesReceived.get(0).getNoResourcesAcquired());

        // Test what Platform Proxy receives
        updateRequestsReceivedByPlatformProxy = dummyPlatformProxyListener.getUpdateAcquisitionRequestsReceivedByListener();
        assertEquals(0, updateRequestsReceivedByPlatformProxy.size());
    }
}
