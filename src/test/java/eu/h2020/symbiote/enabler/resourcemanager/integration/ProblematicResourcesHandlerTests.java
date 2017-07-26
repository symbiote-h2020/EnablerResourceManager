package eu.h2020.symbiote.enabler.resourcemanager.integration;

import eu.h2020.symbiote.enabler.messaging.model.ProblematicResourcesInfo;
import eu.h2020.symbiote.enabler.resourcemanager.model.ProblematicResourcesHandlerStatus;
import eu.h2020.symbiote.enabler.resourcemanager.model.TaskInfo;
import eu.h2020.symbiote.enabler.resourcemanager.model.ProblematicResourcesHandlerResult;
import eu.h2020.symbiote.enabler.resourcemanager.utils.ProblematicResourcesHandler;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Arrays;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

/**
 * Created by vasgl on 7/20/2017.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT,
        properties = {"eureka.client.enabled=false",
                "spring.sleuth.enabled=false",
                "symbiote.core.url=http://localhost:8080",
                "symbiote.coreaam.url=http://localhost:8080"}
)
@ContextConfiguration
@Configuration
@ComponentScan
@EnableAutoConfiguration
public class ProblematicResourcesHandlerTests {

    private static Logger log = LoggerFactory
            .getLogger(ProblematicResourcesHandlerTests.class);

    @Autowired
    private ProblematicResourcesHandler problematicResourcesHandler;

    @Test
    public void replaceProblematicResourcesEnoughResourcesAvailableTest() {
        TaskInfo taskInfo = new TaskInfo();
        taskInfo.setTaskId("task1");
        taskInfo.setMinNoResources(5);
        taskInfo.setAllowCaching(true);
        taskInfo.setResourceIds(new ArrayList(Arrays.asList("1", "2", "3")));
        taskInfo.setStoredResourceIds(new ArrayList(Arrays.asList("4", "5", "6", "badCRAMrespose", "noCRAMurl", "7", "8")));
        taskInfo.setInformPlatformProxy(false);


        ProblematicResourcesInfo problematicResourcesInfo = new ProblematicResourcesInfo();
        problematicResourcesInfo.setTaskId("task1");
        problematicResourcesInfo.setProblematicResourceIds(Arrays.asList("1", "3"));

        ProblematicResourcesHandlerResult result = problematicResourcesHandler.
                replaceProblematicResourcesIfTaskExists(problematicResourcesInfo, taskInfo);

        assertEquals(ProblematicResourcesHandlerStatus.RESOURCES_REPLACED_SUCCESSFULLY, result.getStatus());
        assertEquals(5, result.getTaskInfo().getResourceIds().size());
        assertEquals(1, result.getTaskInfo().getStoredResourceIds().size());

        assertEquals("2", result.getTaskInfo().getResourceIds().get(0));
        assertEquals("4", result.getTaskInfo().getResourceIds().get(1));
        assertEquals("5", result.getTaskInfo().getResourceIds().get(2));
        assertEquals("6", result.getTaskInfo().getResourceIds().get(3));
        assertEquals("7", result.getTaskInfo().getResourceIds().get(4));

        assertEquals("8", result.getTaskInfo().getStoredResourceIds().get(0));

    }
}
