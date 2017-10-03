package eu.h2020.symbiote.enabler.resourcemanager.integration;


import eu.h2020.symbiote.enabler.resourcemanager.dummyListeners.DummyEnablerLogicListener;
import eu.h2020.symbiote.enabler.resourcemanager.dummyListeners.DummyPlatformProxyListener;
import eu.h2020.symbiote.enabler.resourcemanager.repository.TaskInfoRepository;
import eu.h2020.symbiote.enabler.resourcemanager.utils.AuthorizationManager;
import eu.h2020.symbiote.enabler.resourcemanager.utils.ProblematicResourcesTestHelper;

import eu.h2020.symbiote.enabler.resourcemanager.utils.SearchHelper;
import eu.h2020.symbiote.enabler.resourcemanager.utils.TestHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.junit.runner.RunWith;
import org.springframework.amqp.rabbit.AsyncRabbitTemplate;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.web.client.RestTemplate;


@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@ContextConfiguration
@Configuration
@EnableAutoConfiguration
@ComponentScan
@ActiveProfiles("test")
public class PlatformProxyConnectionProblemConsumerTests extends TestHelper {

    private static Log log = LogFactory
            .getLog(PlatformProxyConnectionProblemConsumerTests.class);

    @Autowired
    protected AsyncRabbitTemplate asyncRabbitTemplate;

    @Autowired
    protected TaskInfoRepository taskInfoRepository;

    @Autowired
    protected DummyPlatformProxyListener dummyPlatformProxyListener;

    @Autowired
    protected DummyEnablerLogicListener dummyEnablerLogicListener;

    @Autowired
    private AuthorizationManager authorizationManager;

    @Autowired
    private SearchHelper searchHelper;

    @Autowired
    protected RestTemplate restTemplate;

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    @Qualifier("symbIoTeCoreUrl")
    protected String symbIoTeCoreUrl;

    @Value("${rabbit.exchange.resourceManager.name}")
    protected String resourceManagerExchangeName;

    @Value("${rabbit.routingKey.resourceManager.cancelTask}")
    protected String cancelTaskRoutingKey;

    @Value("${rabbit.routingKey.resourceManager.startDataAcquisition}")
    protected String startDataAcquisitionRoutingKey;

    @Value("${rabbit.routingKey.resourceManager.updateTask}")
    private String updateTaskRoutingKey;

    @Value("${rabbit.routingKey.resourceManager.unavailableResources}")
    private String unavailableResourcesRoutingKey;


    // Execute the Setup method before the test.
    @Before
    public void setUp() throws Exception {
        TestHelper.setUp(dummyPlatformProxyListener, dummyEnablerLogicListener, authorizationManager, symbIoTeCoreUrl,
                searchHelper, restTemplate);
    }

    @After
    public void clearSetup() throws Exception {
        TestHelper.clearSetup(taskInfoRepository);
    }


    @Test
    public void enoughReplaceableResourcesTest() throws Exception {
        log.info("enoughReplaceableResourcesTest STARTED!");

        ProblematicResourcesTestHelper.enoughReplaceableResourcesTest(unavailableResourcesRoutingKey,
                taskInfoRepository, rabbitTemplate, dummyPlatformProxyListener, dummyEnablerLogicListener,
                resourceManagerExchangeName, symbIoTeCoreUrl);

        log.info("enoughReplaceableResourcesTest FINISHED!");
    }

    @Test
    public void enoughReplaceableResourcesNoCachingTest () throws Exception {
        log.info("enoughReplaceableResourcesNoCachingTest STARTED!");

        ProblematicResourcesTestHelper.enoughReplaceableResourcesNoCachingTest(unavailableResourcesRoutingKey,
                taskInfoRepository, rabbitTemplate, dummyPlatformProxyListener, dummyEnablerLogicListener,
                resourceManagerExchangeName, symbIoTeCoreUrl);

        log.info("enoughReplaceableResourcesNoCachingTest FINISHED!");
    }

    @Test
    public void enoughRemainingResourcesTest() throws Exception {
        log.info("enoughRemainingResourcesTest STARTED!");

        ProblematicResourcesTestHelper.enoughRemainingResourcesTest(unavailableResourcesRoutingKey,
                taskInfoRepository, rabbitTemplate, dummyPlatformProxyListener, dummyEnablerLogicListener,
                resourceManagerExchangeName, symbIoTeCoreUrl);

        log.info("enoughRemainingResourcesTest FINISHED!");
    }

    @Test
    public void notEnoughResourcesTest() throws Exception {
        log.info("notEnoughResourcesTest STARTED!");

        ProblematicResourcesTestHelper.notEnoughResourcesTest(unavailableResourcesRoutingKey,
                taskInfoRepository, rabbitTemplate, dummyPlatformProxyListener, dummyEnablerLogicListener,
                resourceManagerExchangeName, symbIoTeCoreUrl);

        log.info("notEnoughResourcesTest FINISHED!");
    }

    @Test
    public void notEnoughResourcesNoCachingTest() throws Exception {
        log.info("notEnoughResourcesNoCachingTest STARTED!");

        ProblematicResourcesTestHelper.notEnoughResourcesNoCachingTest(unavailableResourcesRoutingKey,
                taskInfoRepository, rabbitTemplate, dummyPlatformProxyListener, dummyEnablerLogicListener,
                resourceManagerExchangeName, symbIoTeCoreUrl);

        log.info("notEnoughResourcesNoCachingTest FINISHED!");
    }

    @Test
    public void enoughStoredOnlyResourcesTest() throws Exception {
        log.info("enoughStoredOnlyResourcesTest STARTED!");

        ProblematicResourcesTestHelper.enoughStoredOnlyResourcesTest(unavailableResourcesRoutingKey,
                taskInfoRepository, rabbitTemplate, dummyPlatformProxyListener, dummyEnablerLogicListener,
                resourceManagerExchangeName, symbIoTeCoreUrl);

        log.info("enoughStoredOnlyResourcesTest FINISHED!");
    }

}