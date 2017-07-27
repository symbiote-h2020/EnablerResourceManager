package eu.h2020.symbiote.enabler.resourcemanager.integration;


import eu.h2020.symbiote.enabler.resourcemanager.dummyListeners.DummyEnablerLogicListener;
import eu.h2020.symbiote.enabler.resourcemanager.dummyListeners.DummyPlatformProxyListener;
import eu.h2020.symbiote.enabler.resourcemanager.repository.TaskInfoRepository;
import eu.h2020.symbiote.enabler.resourcemanager.utils.ProblematicResourcesTestHelper;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
public class PlatformProxyConnectionProblemConsumerTests {

    private static Logger log = LoggerFactory
            .getLogger(PlatformProxyConnectionProblemConsumerTests.class);

    @Autowired
    private TaskInfoRepository taskInfoRepository;

    @Autowired
    private DummyPlatformProxyListener dummyPlatformProxyListener;

    @Autowired
    private DummyEnablerLogicListener dummyEnablerLogicListener;

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Value("${rabbit.exchange.resourceManager.name}")
    private String resourceManagerExchangeName;

    @Value("${rabbit.routingKey.resourceManager.unavailableResources}")
    private String unavailableResourcesRoutingKey;

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
    public void unavailableResourcesWithEnoughResourcesTest() throws Exception {
        log.info("unavailableResourcesWithEnoughResourcesTest STARTED!");

        ProblematicResourcesTestHelper.problematicResourceMessageWithEnoughResourcesTest(unavailableResourcesRoutingKey,
                taskInfoRepository, rabbitTemplate, dummyPlatformProxyListener, dummyEnablerLogicListener, resourceManagerExchangeName);

        log.info("unavailableResourcesWithEnoughResourcesTest FINISHED!");
    }

    @Test
    public void unavailableResourcesWithNotEnoughResourcesTest() throws Exception {
        log.info("unavailableResourcesWithNotEnoughResourcesTest STARTED!");

        ProblematicResourcesTestHelper.problematicResourceMessageWithNotEnoughResourcesTest(unavailableResourcesRoutingKey,
                taskInfoRepository, rabbitTemplate, dummyPlatformProxyListener, dummyEnablerLogicListener, resourceManagerExchangeName);

        log.info("unavailableResourcesWithNotEnoughResourcesTest FINISHED!");
    }

    @Test
    public void unavailableResourcesWithEnoughStoredOnlyResourcesTest() throws Exception {
        log.info("unavailableResourcesWithEnoughStoredOnlyResourcesTest STARTED!");

        ProblematicResourcesTestHelper.problematicResourceMessageWithEnoughStoredOnlyResourcesTest(unavailableResourcesRoutingKey,
                taskInfoRepository, rabbitTemplate, dummyPlatformProxyListener, dummyEnablerLogicListener, resourceManagerExchangeName);

        log.info("unavailableResourcesWithEnoughStoredOnlyResourcesTest FINISHED!");
    }

}