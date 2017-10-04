package eu.h2020.symbiote.enabler.resourcemanager.integration;


import eu.h2020.symbiote.enabler.resourcemanager.utils.ProblematicResourcesTestHelper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.junit.Test;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;


@EnableAutoConfiguration
public class PlatformProxyConnectionProblemConsumerTests extends AbstractTestClass {

    private static Log log = LogFactory
            .getLog(PlatformProxyConnectionProblemConsumerTests.class);

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