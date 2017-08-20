package eu.h2020.symbiote.enabler.resourcemanager.integration;


import com.fasterxml.jackson.databind.ObjectMapper;

import eu.h2020.symbiote.core.internal.CoreQueryRequest;
import eu.h2020.symbiote.enabler.messaging.model.*;
import eu.h2020.symbiote.enabler.resourcemanager.dummyListeners.DummyEnablerLogicListener;
import eu.h2020.symbiote.enabler.resourcemanager.dummyListeners.DummyPlatformProxyListener;
import eu.h2020.symbiote.enabler.resourcemanager.model.TaskInfo;
import eu.h2020.symbiote.enabler.resourcemanager.repository.TaskInfoRepository;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

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
public class CancelTaskConsumerTests {

    private static Logger log = LoggerFactory
            .getLogger(CancelTaskConsumerTests.class);

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
    }

    @After
    public void clearSetup() throws Exception {
        taskInfoRepository.deleteAll();
    }

    @Test
    public void cancelTaskTest() throws Exception {
        log.info("cancelTaskTest STARTED!");

        List<CancelTaskRequest> cancelTaskRequestArrayList = null;

        TaskInfo task1 = new TaskInfo();
        CoreQueryRequest coreQueryRequest = new CoreQueryRequest.Builder()
                .locationName("Zurich")
                .observedProperty(Arrays.asList("temperature", "humidity"))
                .build();

        Map<String, String> resourceUrls1 = new HashMap<>();
        resourceUrls1.put("resource1", symbIoTeCoreUrl + "/Sensors('resource1')");
        resourceUrls1.put("resource2", symbIoTeCoreUrl + "/Sensors('resource2')");

        task1.setTaskId("1");
        task1.setMinNoResources(2);
        task1.setCoreQueryRequest(coreQueryRequest);
        task1.setResourceIds(Arrays.asList("1", "2"));
        task1.setQueryInterval("P0-0-0T0:0:0.06");
        task1.setAllowCaching(true);
        task1.setCachingInterval("P0-0-0T0:0:1");
        task1.setInformPlatformProxy(true);
        task1.setStoredResourceIds(Arrays.asList("3", "4"));
        task1.setEnablerLogicName("TestEnablerLogic");
        task1.setStatus(ResourceManagerTaskInfoResponseStatus.SUCCESS);
        task1.setResourceUrls(resourceUrls1);
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
        rabbitTemplate.convertAndSend(resourceManagerExchangeName, cancelTaskRoutingKey, cancelTaskRequest);
        log.info("After sending the message");

        while (dummyPlatformProxyListener.cancelTaskRequestsReceived() == 0) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(100);

        taskInfo = taskInfoRepository.findByTaskId("1");
        assertEquals(null, taskInfo);

        taskInfo = taskInfoRepository.findByTaskId("2");
        assertEquals(null, taskInfo);

        taskInfo = taskInfoRepository.findByTaskId("3");
        assertEquals(null, taskInfo);

        taskInfo = taskInfoRepository.findByTaskId("4");
        assertEquals(null, taskInfo);

        cancelTaskRequestArrayList = dummyPlatformProxyListener.getCancelTaskRequestsReceivedByListener();

        assertEquals(1, cancelTaskRequestArrayList.size());
        assertEquals(2, cancelTaskRequestArrayList.get(0).getTaskIdList().size());
        assertEquals("1", cancelTaskRequestArrayList.get(0).getTaskIdList().get(0));
        assertEquals("4", cancelTaskRequestArrayList.get(0).getTaskIdList().get(1));

        log.info("cancelTaskTest FINISHED!");
    }

    @Test
    public void cancelTaskEmptyResponseListTest() throws Exception {
        log.info("cancelTaskTest STARTED!");

        List<CancelTaskRequest> cancelTaskRequestArrayList = null;

        TaskInfo task1 = new TaskInfo();
        CoreQueryRequest coreQueryRequest = new CoreQueryRequest.Builder()
                .locationName("Zurich")
                .observedProperty(Arrays.asList("temperature", "humidity"))
                .build();

        Map<String, String> resourceUrls1 = new HashMap<>();
        resourceUrls1.put("resource1", symbIoTeCoreUrl + "/Sensors('resource1')");
        resourceUrls1.put("resource2", symbIoTeCoreUrl + "/Sensors('resource2')");

        task1.setTaskId("1");
        task1.setMinNoResources(2);
        task1.setCoreQueryRequest(coreQueryRequest);
        task1.setResourceIds(Arrays.asList("1", "2"));
        task1.setQueryInterval("P0-0-0T0:0:0.06");
        task1.setAllowCaching(true);
        task1.setCachingInterval("P0-0-0T0:0:1");
        task1.setInformPlatformProxy(true);
        task1.setEnablerLogicName("TestEnablerLogic");
        task1.setStoredResourceIds(Arrays.asList("3", "4"));
        task1.setStatus(ResourceManagerTaskInfoResponseStatus.SUCCESS);
        task1.setResourceUrls(resourceUrls1);
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
        rabbitTemplate.convertAndSend(resourceManagerExchangeName, cancelTaskRoutingKey, cancelTaskRequest);
        log.info("After sending the message");

        TimeUnit.MILLISECONDS.sleep(500);

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

        log.info("cancelTaskTest FINISHED!");
    }
}