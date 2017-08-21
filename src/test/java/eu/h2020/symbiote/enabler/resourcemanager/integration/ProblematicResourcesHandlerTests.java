package eu.h2020.symbiote.enabler.resourcemanager.integration;

import eu.h2020.symbiote.core.internal.CoreQueryRequest;
import eu.h2020.symbiote.enabler.messaging.model.ProblematicResourcesInfo;
import eu.h2020.symbiote.enabler.messaging.model.ResourceManagerTaskInfoResponseStatus;
import eu.h2020.symbiote.enabler.resourcemanager.model.ProblematicResourcesHandlerStatus;
import eu.h2020.symbiote.enabler.resourcemanager.model.TaskInfo;
import eu.h2020.symbiote.enabler.resourcemanager.model.ProblematicResourcesHandlerResult;
import eu.h2020.symbiote.enabler.resourcemanager.utils.ProblematicResourcesHandler;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.*;

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

    @Autowired
    @Qualifier("symbIoTeCoreUrl")
    private String symbIoTeCoreUrl;

    @Test
    public void replaceProblematicResourcesEnoughResourcesAvailableTest() {
        log.info("replaceProblematicResourcesEnoughResourcesAvailableTest STARTED!");

        List<String> resourceIds = new ArrayList(Arrays.asList("1", "2", "3"));
        List<String> storedResourceIds = new ArrayList(Arrays.asList("4", "5", "6", "badCRAMrespose", "noCRAMurl", "7", "8"));

        Map<String, String> resourceUrls = new HashMap<>();
        resourceUrls.put("1", symbIoTeCoreUrl + "/Sensors('1')");
        resourceUrls.put("2", symbIoTeCoreUrl + "/Sensors('2')");
        resourceUrls.put("3", symbIoTeCoreUrl + "/Sensors('3')");

        TaskInfo taskInfo = new TaskInfo("task1", 5, new CoreQueryRequest(),
                "P0-0-0T0:0:1", true, "P0-0-0T0:0:1", false,
                "TestEnablerLogic", null, resourceIds,
                ResourceManagerTaskInfoResponseStatus.SUCCESS, storedResourceIds, resourceUrls);

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

        log.info("replaceProblematicResourcesEnoughResourcesAvailableTest FINISHED!");
    }
}
