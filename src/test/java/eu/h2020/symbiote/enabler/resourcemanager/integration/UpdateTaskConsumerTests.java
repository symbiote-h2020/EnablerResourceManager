package eu.h2020.symbiote.enabler.resourcemanager.integration;


import com.fasterxml.jackson.databind.ObjectMapper;
import eu.h2020.symbiote.core.internal.CoreQueryRequest;
import eu.h2020.symbiote.enabler.messaging.model.*;
import eu.h2020.symbiote.enabler.resourcemanager.dummyListeners.DummyEnablerLogicListener;
import eu.h2020.symbiote.enabler.resourcemanager.dummyListeners.DummyPlatformProxyListener;
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
        log.info("updateTaskNoChangeInCoreQueryRequest STARTED!");

        final AtomicReference<ResourceManagerAcquisitionStartResponse> resultRef = new AtomicReference<>();
        List<PlatformProxyAcquisitionStartRequest> startAcquisitionRequestsReceivedByListener;

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
        taskInfoRepository.save(task1);

        TaskInfo task2 = new TaskInfo(task1);
        task2.setTaskId("2");
        task2.setResourceIds(Arrays.asList("21", "22"));
        taskInfoRepository.save(task2);

        TaskInfo task3 = new TaskInfo(task1);
        task3.setTaskId("3");
        task3.setResourceIds(Arrays.asList("31", "32"));
        taskInfoRepository.save(task3);

        TaskInfo updatedTask1 = new TaskInfo(task1);
        updatedTask1.getCoreQueryRequest().setLocation_name("Paris");

        TaskInfo updatedTask2 = new TaskInfo(task2);
        updatedTask2.setInformPlatformProxy(false);

        TaskInfo updatedTask3 = new TaskInfo(task3);
        task3.setCoreQueryRequest(null);

        ResourceManagerAcquisitionStartRequest req = new ResourceManagerAcquisitionStartRequest();
        req.setResources(Arrays.asList(new ResourceManagerTaskInfoRequest(updatedTask1),
                new ResourceManagerTaskInfoRequest(updatedTask2),
                new ResourceManagerTaskInfoRequest(updatedTask3)));


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

        assertEquals(false, task1.equals(storedTaskInfo1));
        assertEquals(false, task2.equals(storedTaskInfo2));
        assertEquals(false, updatedTask1.equals(storedTaskInfo1));
        assertEquals(true, updatedTask2.equals(storedTaskInfo2));
        assertEquals(true, updatedTask3.equals(storedTaskInfo3));

        String responseInString = mapper.writeValueAsString(resultRef.get().getResources());
        log.info("Response String: " + responseInString);

        // Test what Enabler Logic receives
        assertEquals(3, resultRef.get().getResources().size());
        assertEquals(2, resultRef.get().getResources().get(0).getResourceIds().size());
        assertEquals(2, resultRef.get().getResources().get(1).getResourceIds().size());
        assertEquals(2, resultRef.get().getResources().get(2).getResourceIds().size());

        assertEquals("resource1", resultRef.get().getResources().get(0).getResourceIds().get(0));
        assertEquals("resource2", resultRef.get().getResources().get(0).getResourceIds().get(1));
        assertEquals("21", resultRef.get().getResources().get(1).getResourceIds().get(0));
        assertEquals("22", resultRef.get().getResources().get(1).getResourceIds().get(1));
        assertEquals("31", resultRef.get().getResources().get(2).getResourceIds().get(0));
        assertEquals("32", resultRef.get().getResources().get(2).getResourceIds().get(1));

        while(dummyPlatformProxyListener.startAcquisitionRequestsReceived() < 1) {
            log.info("startAcquisitionRequestsReceivedByListener.size(): " + dummyPlatformProxyListener.startAcquisitionRequestsReceived());
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // Added extra delay to make sure that the message is handled
        TimeUnit.MILLISECONDS.sleep(300);

        // Test what Platform Proxy receives
        startAcquisitionRequestsReceivedByListener = dummyPlatformProxyListener.getStartAcquisitionRequestsReceivedByListener();

        assertEquals(1, startAcquisitionRequestsReceivedByListener.size());
        assertEquals("resource1", startAcquisitionRequestsReceivedByListener.get(0).getResources().get(0).getResourceId());
        assertEquals("resource2", startAcquisitionRequestsReceivedByListener.get(0).getResources().get(1).getResourceId());

        log.info("updateTaskNoChangeInCoreQueryRequest FINISHED!");
    }
}