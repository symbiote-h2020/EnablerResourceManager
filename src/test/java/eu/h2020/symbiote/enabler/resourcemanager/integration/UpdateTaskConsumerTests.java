package eu.h2020.symbiote.enabler.resourcemanager.integration;


import com.fasterxml.jackson.databind.ObjectMapper;
import eu.h2020.symbiote.enabler.messaging.model.CancelTaskRequest;
import eu.h2020.symbiote.enabler.messaging.model.ResourceManagerAcquisitionStartRequest;
import eu.h2020.symbiote.enabler.messaging.model.ResourceManagerAcquisitionStartResponse;
import eu.h2020.symbiote.enabler.resourcemanager.dummyListeners.DummyEnablerLogicListener;
import eu.h2020.symbiote.enabler.resourcemanager.dummyListeners.DummyPlatformProxyListener;
import eu.h2020.symbiote.enabler.resourcemanager.model.TaskInfo;
import eu.h2020.symbiote.enabler.resourcemanager.repository.TaskInfoRepository;
import eu.h2020.symbiote.enabler.resourcemanager.utils.ListenableFutureCallbackCustom;
import eu.h2020.symbiote.enabler.resourcemanager.utils.TestHelper;
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
    public void updateTask() throws Exception {

        final AtomicReference<ResourceManagerAcquisitionStartResponse> resultRef = new AtomicReference<>();
        ResourceManagerAcquisitionStartRequest req = new ResourceManagerAcquisitionStartRequest();

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

        assertEquals(null, resultRef.get().getResources());
    }
}