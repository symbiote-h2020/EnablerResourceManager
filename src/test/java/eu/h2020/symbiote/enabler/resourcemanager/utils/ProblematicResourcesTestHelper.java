package eu.h2020.symbiote.enabler.resourcemanager.utils;

import eu.h2020.symbiote.core.internal.CoreQueryRequest;
import eu.h2020.symbiote.enabler.messaging.model.*;
import eu.h2020.symbiote.enabler.resourcemanager.dummyListeners.DummyEnablerLogicListener;
import eu.h2020.symbiote.enabler.resourcemanager.dummyListeners.DummyPlatformProxyListener;
import eu.h2020.symbiote.enabler.resourcemanager.model.TaskInfo;
import eu.h2020.symbiote.enabler.resourcemanager.repository.TaskInfoRepository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Created by vasgl on 7/26/2017.
 */
public class ProblematicResourcesTestHelper {

    private static Logger log = LoggerFactory
            .getLogger(ProblematicResourcesTestHelper.class);

    private ProblematicResourcesTestHelper() {
        // empty constructor
    }

    public static void problematicResourceMessageWithEnoughResourcesTest(String routingKey, TaskInfoRepository taskInfoRepository,
                                                                         RabbitTemplate rabbitTemplate,
                                                                         DummyPlatformProxyListener dummyPlatformProxyListener,
                                                                         DummyEnablerLogicListener dummyEnablerLogicListener,
                                                                         String resourceManagerExchangeName,
                                                                         String symbIoTeCoreUrl) throws Exception {

        List<PlatformProxyUpdateRequest> updateRequestsReceivedByPlatformProxy;
        List<ResourcesUpdated> updateRequestsReceivedByEnablerLogic;

        Map<String, String> resourceUrls = new HashMap<>();
        resourceUrls.put("1", symbIoTeCoreUrl + "/Sensors('1')");
        resourceUrls.put("2", symbIoTeCoreUrl + "/Sensors('2')");
        resourceUrls.put("3", symbIoTeCoreUrl + "/Sensors('3')");

        TaskInfo taskInfo = new TaskInfo();
        taskInfo.setTaskId("task1");
        taskInfo.setMinNoResources(5);
        taskInfo.setCoreQueryRequest(new CoreQueryRequest());
        taskInfo.setQueryInterval("P0-0-0T0:0:0.1");
        taskInfo.setAllowCaching(true);
        taskInfo.setCachingInterval("P0-0-0T0:0:0.1");
        taskInfo.setResourceIds(new ArrayList(Arrays.asList("1", "2", "3")));
        taskInfo.setStoredResourceIds(new ArrayList(Arrays.asList("4", "5", "6", "badCRAMrespose", "noCRAMurl", "7", "8")));
        taskInfo.setInformPlatformProxy(true);
        taskInfo.setEnablerLogicName("testEnablerLogic");
        taskInfo.setStatus(ResourceManagerTaskInfoResponseStatus.SUCCESS);
        taskInfo.setResourceUrls(resourceUrls);
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
        assertEquals(4, updateRequestsReceivedByPlatformProxy.get(0).getResources().size());
        assertEquals("4", updateRequestsReceivedByPlatformProxy.get(0).getResources().get(0).getResourceId());
        assertEquals("5", updateRequestsReceivedByPlatformProxy.get(0).getResources().get(1).getResourceId());
        assertEquals("6", updateRequestsReceivedByPlatformProxy.get(0).getResources().get(2).getResourceId());
        assertEquals("7", updateRequestsReceivedByPlatformProxy.get(0).getResources().get(3).getResourceId());

        // Test what Enabler Logic receives
        updateRequestsReceivedByEnablerLogic = dummyEnablerLogicListener.getUpdateResourcesReceivedByListener();
        assertEquals(1, updateRequestsReceivedByEnablerLogic.size());
        assertEquals(4, updateRequestsReceivedByEnablerLogic.get(0).getNewResources().size());
        assertEquals("4", updateRequestsReceivedByEnablerLogic.get(0).getNewResources().get(0));
        assertEquals("5", updateRequestsReceivedByEnablerLogic.get(0).getNewResources().get(1));
        assertEquals("6", updateRequestsReceivedByEnablerLogic.get(0).getNewResources().get(2));
        assertEquals("7", updateRequestsReceivedByEnablerLogic.get(0).getNewResources().get(3));
    }

    public static void problematicResourceMessageWithNotEnoughResourcesTest(String routingKey, TaskInfoRepository taskInfoRepository,
                                                                            RabbitTemplate rabbitTemplate,
                                                                            DummyPlatformProxyListener dummyPlatformProxyListener,
                                                                            DummyEnablerLogicListener dummyEnablerLogicListener,
                                                                            String resourceManagerExchangeName,
                                                                            String symbIoTeCoreUrl) throws Exception {

        List<PlatformProxyUpdateRequest> updateRequestsReceivedByPlatformProxy;
        List<NotEnoughResourcesAvailable> notEnoughResourcesMessagesReceived;

        Map<String, String> resourceUrls = new HashMap<>();
        resourceUrls.put("1", symbIoTeCoreUrl + "/Sensors('1')");
        resourceUrls.put("2", symbIoTeCoreUrl + "/Sensors('2')");
        resourceUrls.put("3", symbIoTeCoreUrl + "/Sensors('3')");

        TaskInfo taskInfo = new TaskInfo();
        taskInfo.setTaskId("task1");
        taskInfo.setCoreQueryRequest(new CoreQueryRequest());
        taskInfo.setMinNoResources(5);
        taskInfo.setQueryInterval("P0-0-0T0:0:0.1");
        taskInfo.setAllowCaching(true);
        taskInfo.setCachingInterval("P0-0-0T0:0:0.1");
        taskInfo.setResourceIds(new ArrayList(Arrays.asList("1", "2", "3")));
        taskInfo.setStoredResourceIds(new ArrayList(Arrays.asList("4", "5", "6")));
        taskInfo.setInformPlatformProxy(true);
        taskInfo.setEnablerLogicName("testEnablerLogic");
        taskInfo.setStatus(ResourceManagerTaskInfoResponseStatus.SUCCESS);
        taskInfo.setResourceUrls(resourceUrls);
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
        assertEquals(3, taskInfo.getStoredResourceIds().size());
        assertEquals(1, taskInfo.getResourceIds().size());
        assertEquals(ResourceManagerTaskInfoResponseStatus.NOT_ENOUGH_RESOURCES, taskInfo.getStatus());

        assertEquals("2", taskInfo.getResourceIds().get(0));

        assertEquals("4", taskInfo.getStoredResourceIds().get(0));
        assertEquals("5", taskInfo.getStoredResourceIds().get(1));
        assertEquals("6", taskInfo.getStoredResourceIds().get(2));

        assertEquals(symbIoTeCoreUrl + "/Sensors('2')", taskInfo.getResourceUrls().get("2"));


        // Test what Enabler Logic receives
        notEnoughResourcesMessagesReceived = dummyEnablerLogicListener.getNotEnoughResourcesMessagesReceivedByListener();
        assertEquals(1, notEnoughResourcesMessagesReceived.size());
        assertEquals("task1", notEnoughResourcesMessagesReceived.get(0).getTaskId());
        assertEquals(5, (int) notEnoughResourcesMessagesReceived.get(0).getMinNoResources());
        assertEquals(1, (int) notEnoughResourcesMessagesReceived.get(0).getNoResourcesAcquired());
        assertEquals(3, (int) notEnoughResourcesMessagesReceived.get(0).getMaxNoResourcesThatCanBeAcquired());

        // Test what Platform Proxy receives
        updateRequestsReceivedByPlatformProxy = dummyPlatformProxyListener.getUpdateAcquisitionRequestsReceivedByListener();
        assertEquals(0, updateRequestsReceivedByPlatformProxy.size());
    }

    public static void problematicResourceMessageWithEnoughStoredOnlyResourcesTest(String routingKey, TaskInfoRepository taskInfoRepository,
                                                                                   RabbitTemplate rabbitTemplate,
                                                                                   DummyPlatformProxyListener dummyPlatformProxyListener,
                                                                                   DummyEnablerLogicListener dummyEnablerLogicListener,
                                                                                   String resourceManagerExchangeName,
                                                                                   String symbIoTeCoreUrl) throws Exception {

        List<PlatformProxyUpdateRequest> updateRequestsReceivedByPlatformProxy;
        List<NotEnoughResourcesAvailable> notEnoughResourcesMessagesReceived;

        Map<String, String> resourceUrls = new HashMap<>();
        resourceUrls.put("1", symbIoTeCoreUrl + "/Sensors('1')");
        resourceUrls.put("2", symbIoTeCoreUrl + "/Sensors('2')");
        resourceUrls.put("3", symbIoTeCoreUrl + "/Sensors('3')");

        TaskInfo taskInfo = new TaskInfo();
        taskInfo.setTaskId("task1");
        taskInfo.setMinNoResources(5);
        taskInfo.setCoreQueryRequest(new CoreQueryRequest());
        taskInfo.setQueryInterval("P0-0-0T0:0:0.1");
        taskInfo.setAllowCaching(true);
        taskInfo.setCachingInterval("P0-0-0T0:0:0.1");
        taskInfo.setResourceIds(new ArrayList(Arrays.asList("1", "2", "3")));
        taskInfo.setStoredResourceIds(new ArrayList(Arrays.asList("4", "5", "6", "badCRAMrespose", "noCRAMurl")));
        taskInfo.setInformPlatformProxy(true);
        taskInfo.setEnablerLogicName("testEnablerLogic");
        taskInfo.setStatus(ResourceManagerTaskInfoResponseStatus.SUCCESS);
        taskInfo.setResourceUrls(resourceUrls);
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
        assertEquals(5, (int) notEnoughResourcesMessagesReceived.get(0).getMinNoResources());
        assertEquals(4, (int) notEnoughResourcesMessagesReceived.get(0).getNoResourcesAcquired());
        assertEquals(0, (int) notEnoughResourcesMessagesReceived.get(0).getMaxNoResourcesThatCanBeAcquired());

        // Test what Platform Proxy receives
        updateRequestsReceivedByPlatformProxy = dummyPlatformProxyListener.getUpdateAcquisitionRequestsReceivedByListener();
        assertEquals(0, updateRequestsReceivedByPlatformProxy.size());
    }
}
