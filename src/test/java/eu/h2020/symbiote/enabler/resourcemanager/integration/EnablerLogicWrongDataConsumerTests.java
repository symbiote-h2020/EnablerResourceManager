package eu.h2020.symbiote.enabler.resourcemanager.integration;


import eu.h2020.symbiote.enabler.resourcemanager.dummyListeners.DummyEnablerLogicListener;
import eu.h2020.symbiote.enabler.resourcemanager.dummyListeners.DummyPlatformProxyListener;
import eu.h2020.symbiote.enabler.resourcemanager.repository.TaskInfoRepository;
import eu.h2020.symbiote.enabler.resourcemanager.utils.AuthorizationManager;
import eu.h2020.symbiote.enabler.resourcemanager.utils.ProblematicResourcesTestHelper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.mockito.Mockito;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.embedded.LocalServerPort;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.context.annotation.*;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;


@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(webEnvironment = WebEnvironment.DEFINED_PORT,
        properties = {"eureka.client.enabled=false",
                "spring.sleuth.enabled=false",
                "symbiote.enabler.core.interface.url=http://localhost:8080",
                "symbiote.enabler.rm.database=symbiote-enabler-rm-database-enwdct",
                "rabbit.queueName.resourceManager.startDataAcquisition=symbIoTe-resourceManager-startDataAcquisition-enwdct",
                "rabbit.queueName.resourceManager.cancelTask=symbIoTe-resourceManager-cancelTask-enwdct",
                "rabbit.queueName.resourceManager.unavailableResources=symbIoTe-resourceManager-unavailableResources-enwdct",
                "rabbit.queueName.resourceManager.wrongData=symbIoTe-resourceManager-wrongData-enwdct",
                "rabbit.queueName.resourceManager.updateTask=symbIoTe-resourceManager-updateTask-enwdct",
                "rabbit.queueName.pl.acquisitionStartRequested=symbIoTe-pl-acquisitionStartRequested-enwdct",
                "rabbit.queueName.pl.taskUpdated=symbIoTe-pl-taskUpdated-enwdct",
                "rabbit.queueName.pl.cancelTasks=symbIoTe-pl-cancelTasks-enwdct",
                "rabbit.queueName.el.resourcesUpdated=symbIoTe-el-resourcesUpdated-enwdct",
                "rabbit.queueName.el.notEnoughResources=symbIoTe-el-notEnoughResources-enwdct"})
@ContextConfiguration
@Configuration
@ComponentScan
@EnableAutoConfiguration
@ActiveProfiles("test")
public class EnablerLogicWrongDataConsumerTests {

    private static Log log = LogFactory
            .getLog(EnablerLogicWrongDataConsumerTests.class);

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
    public void enoughReplaceableResourcesTest() throws Exception {
        log.info("enoughReplaceableResourcesTest STARTED!");

        ProblematicResourcesTestHelper.enoughReplaceableResourcesTest(wrongDataRoutingKey,
                taskInfoRepository, rabbitTemplate, dummyPlatformProxyListener, dummyEnablerLogicListener,
                resourceManagerExchangeName, symbIoTeCoreUrl);

        log.info("enoughReplaceableResourcesTest FINISHED!");
    }

    @Test
    public void enoughReplaceableResourcesNoCachingTest() throws Exception {
        log.info("enoughReplaceableResourcesNoCachingTest STARTED!");

        ProblematicResourcesTestHelper.enoughReplaceableResourcesNoCachingTest(wrongDataRoutingKey,
                taskInfoRepository, rabbitTemplate, dummyPlatformProxyListener, dummyEnablerLogicListener,
                resourceManagerExchangeName, symbIoTeCoreUrl);

        log.info("enoughReplaceableResourcesNoCachingTest FINISHED!");
    }

    @Test
    public void enoughRemainingResourcesTest() throws Exception {
        log.info("enoughRemainingResourcesTest STARTED!");

        ProblematicResourcesTestHelper.enoughRemainingResourcesTest(wrongDataRoutingKey,
                taskInfoRepository, rabbitTemplate, dummyPlatformProxyListener, dummyEnablerLogicListener,
                resourceManagerExchangeName, symbIoTeCoreUrl);

        log.info("enoughRemainingResourcesTest FINISHED!");
    }

    @Test
    public void notEnoughResourcesTest() throws Exception {
        log.info("notEnoughResourcesTest STARTED!");

        ProblematicResourcesTestHelper.notEnoughResourcesTest(wrongDataRoutingKey,
                taskInfoRepository, rabbitTemplate, dummyPlatformProxyListener, dummyEnablerLogicListener,
                resourceManagerExchangeName, symbIoTeCoreUrl);

        log.info("notEnoughResourcesTest FINISHED!");
    }

    @Test
    public void notEnoughResourcesNoCachingTest() throws Exception {
        log.info("notEnoughResourcesNoCachingTest STARTED!");

        ProblematicResourcesTestHelper.notEnoughResourcesNoCachingTest(wrongDataRoutingKey,
                taskInfoRepository, rabbitTemplate, dummyPlatformProxyListener, dummyEnablerLogicListener,
                resourceManagerExchangeName, symbIoTeCoreUrl);

        log.info("notEnoughResourcesNoCachingTest FINISHED!");
    }

    @Test
    public void enoughStoredOnlyResourcesTest() throws Exception {
        log.info("enoughStoredOnlyResourcesTest STARTED!");

        ProblematicResourcesTestHelper.enoughStoredOnlyResourcesTest(wrongDataRoutingKey,
                taskInfoRepository, rabbitTemplate, dummyPlatformProxyListener, dummyEnablerLogicListener,
                resourceManagerExchangeName, symbIoTeCoreUrl);

        log.info("enoughStoredOnlyResourcesTest FINISHED!");
    }

}