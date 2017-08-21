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
import org.springframework.beans.factory.annotation.Qualifier;
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
public class EnablerLogicWrongDataConsumerTests {

    private static Logger log = LoggerFactory
            .getLogger(EnablerLogicWrongDataConsumerTests.class);

    @Autowired
    private TaskInfoRepository taskInfoRepository;

    @Autowired
    private DummyPlatformProxyListener dummyPlatformProxyListener;

    @Autowired
    private DummyEnablerLogicListener dummyEnablerLogicListener;

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    @Qualifier("symbIoTeCoreUrl")
    private String symbIoTeCoreUrl;

    @Value("${rabbit.exchange.resourceManager.name}")
    private String resourceManagerExchangeName;

    @Value("${rabbit.routingKey.resourceManager.wrongData}")
    private String wrongDataRoutingKey;

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
    public void wrongDataResourcesWithReplaceableResourcesTest() throws Exception {
        log.info("wrongDataResourcesWithReplaceableResourcesTest STARTED!");

        ProblematicResourcesTestHelper.enoughReplaceableResourcesTest(wrongDataRoutingKey,
                taskInfoRepository, rabbitTemplate, dummyPlatformProxyListener, dummyEnablerLogicListener,
                resourceManagerExchangeName, symbIoTeCoreUrl);

        log.info("wrongDataResourcesWithReplaceableResourcesTest FINISHED!");
    }

    @Test
    public void wrongDataResourcesWithEnoughRemainingResourcesTest() throws Exception {
        log.info("wrongDataResourcesWithEnoughRemainingResourcesTest STARTED!");

        ProblematicResourcesTestHelper.enoughRemainingResourcesTest(wrongDataRoutingKey,
                taskInfoRepository, rabbitTemplate, dummyPlatformProxyListener, dummyEnablerLogicListener,
                resourceManagerExchangeName, symbIoTeCoreUrl);

        log.info("wrongDataResourcesWithEnoughRemainingResourcesTest FINISHED!");
    }

    @Test
    public void wrongDataResourcesWithNotEnoughResourcesTest() throws Exception {
        log.info("wrongDataResourcesWithNotEnoughResourcesTest STARTED!");

        ProblematicResourcesTestHelper.notEnoughResourcesTest(wrongDataRoutingKey,
                taskInfoRepository, rabbitTemplate, dummyPlatformProxyListener, dummyEnablerLogicListener,
                resourceManagerExchangeName, symbIoTeCoreUrl);

        log.info("wrongDataResourcesWithNotEnoughResourcesTest FINISHED!");
    }

    @Test
    public void wrongDataResourcesWithEnoughStoredOnlyResourcesTest() throws Exception {
        log.info("wrongDataResourcesWithEnoughStoredOnlyResourcesTest STARTED!");

        ProblematicResourcesTestHelper.enoughStoredOnlyResourcesTest(wrongDataRoutingKey,
                taskInfoRepository, rabbitTemplate, dummyPlatformProxyListener, dummyEnablerLogicListener,
                resourceManagerExchangeName, symbIoTeCoreUrl);

        log.info("wrongDataResourcesWithEnoughStoredOnlyResourcesTest FINISHED!");
    }

}