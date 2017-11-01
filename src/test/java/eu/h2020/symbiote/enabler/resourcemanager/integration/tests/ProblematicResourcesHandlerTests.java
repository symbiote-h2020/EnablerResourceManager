package eu.h2020.symbiote.enabler.resourcemanager.integration.tests;

import eu.h2020.symbiote.core.ci.QueryResourceResult;
import eu.h2020.symbiote.core.internal.CoreQueryRequest;
import eu.h2020.symbiote.enabler.messaging.model.ProblematicResourcesInfo;
import eu.h2020.symbiote.enabler.messaging.model.ResourceManagerTaskInfoResponseStatus;
import eu.h2020.symbiote.enabler.resourcemanager.integration.AbstractTestClass;
import eu.h2020.symbiote.enabler.resourcemanager.model.ProblematicResourcesHandlerResult;
import eu.h2020.symbiote.enabler.resourcemanager.model.ProblematicResourcesHandlerStatus;
import eu.h2020.symbiote.enabler.resourcemanager.model.TaskInfo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.junit.Test;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;

import java.util.*;

import static org.junit.Assert.assertEquals;


/**
 * Created by vasgl on 7/20/2017.
 */

@EnableAutoConfiguration
public class ProblematicResourcesHandlerTests extends AbstractTestClass {

    private static Log log = LogFactory
            .getLog(ProblematicResourcesHandlerTests.class);


    @Test
    public void replaceProblematicResourcesEnoughResourcesAvailableTest() {
        log.info("replaceProblematicResourcesEnoughResourcesAvailableTest STARTED!");

        List<String> resourceIds = new ArrayList(Arrays.asList("1", "2", "3"));
        List<String> storedResourceIds = new ArrayList(Arrays.asList("4", "5", "6", "badCRAMrespose", "noCRAMurl", "7", "8"));

        Map<String, String> resourceUrls = new HashMap<>();
        resourceUrls.put("1", symbIoTeCoreUrl + "/Sensors('1')");
        resourceUrls.put("2", symbIoTeCoreUrl + "/Sensors('2')");
        resourceUrls.put("3", symbIoTeCoreUrl + "/Sensors('3')");

        ArrayList<QueryResourceResult> results = new ArrayList<>();
        QueryResourceResult result1 = new QueryResourceResult();
        QueryResourceResult result2 = new QueryResourceResult();
        QueryResourceResult result3 = new QueryResourceResult();
        result1.setId("1");
        result2.setId("2");
        result3.setId("3");
        results.add(result1);
        results.add(result2);
        results.add(result3);

        TaskInfo taskInfo = new TaskInfo("task1", 5, new CoreQueryRequest(),
                "P0-0-0T0:0:1", true, "P0-0-0T0:0:1", false, "TestEnablerLogic", null, resourceIds, results,
                ResourceManagerTaskInfoResponseStatus.SUCCESS, storedResourceIds, resourceUrls, "message");

        ProblematicResourcesInfo problematicResourcesInfo = new ProblematicResourcesInfo();
        problematicResourcesInfo.setTaskId("task1");
        problematicResourcesInfo.setProblematicResourceIds(Arrays.asList("1", "3"));

        ProblematicResourcesHandlerResult result = problematicResourcesHandler.
                replaceProblematicResourcesIfTaskExists(problematicResourcesInfo, taskInfo);

        assertEquals(ProblematicResourcesHandlerStatus.RESOURCES_REPLACED_SUCCESSFULLY, result.getStatus());
        assertEquals(5, result.getTaskInfo().getResourceIds().size());
        assertEquals(5, result.getTaskInfo().getResourceDescriptions().size());
        assertEquals(1, result.getTaskInfo().getStoredResourceIds().size());

        assertEquals("2", result.getTaskInfo().getResourceIds().get(0));
        assertEquals("4", result.getTaskInfo().getResourceIds().get(1));
        assertEquals("5", result.getTaskInfo().getResourceIds().get(2));
        assertEquals("6", result.getTaskInfo().getResourceIds().get(3));
        assertEquals("7", result.getTaskInfo().getResourceIds().get(4));

        assertEquals("2", result.getTaskInfo().getResourceDescriptions().get(0).getId());
        assertEquals("4", result.getTaskInfo().getResourceDescriptions().get(1).getId());
        assertEquals("5", result.getTaskInfo().getResourceDescriptions().get(2).getId());
        assertEquals("6", result.getTaskInfo().getResourceDescriptions().get(3).getId());
        assertEquals("7", result.getTaskInfo().getResourceDescriptions().get(4).getId());

        assertEquals("8", result.getTaskInfo().getStoredResourceIds().get(0));

        log.info("replaceProblematicResourcesEnoughResourcesAvailableTest FINISHED!");
    }
}
