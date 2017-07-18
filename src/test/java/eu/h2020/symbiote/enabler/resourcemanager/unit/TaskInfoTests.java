package eu.h2020.symbiote.enabler.resourcemanager.unit;

import eu.h2020.symbiote.core.ci.QueryResourceResult;
import eu.h2020.symbiote.core.ci.QueryResponse;
import eu.h2020.symbiote.enabler.messaging.model.ResourceManagerTaskInfoResponse;
import eu.h2020.symbiote.enabler.resourcemanager.model.TaskInfo;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/**
 * Created by vasgl on 7/17/2017.
 */
@RunWith(SpringJUnit4ClassRunner.class)
public class TaskInfoTests {

    private static Logger log = LoggerFactory
            .getLogger(TaskInfoTests.class);

    @Test
    public void taskInfoConstructorTest() {
        ResourceManagerTaskInfoResponse response = new ResourceManagerTaskInfoResponse();

        response.setTaskId("1");
        response.setCount(2);
        response.setLocation("Zurich");
        response.setObservesProperty(Arrays.asList("temperature", "humidity"));
        response.setResourceIds(Arrays.asList("1", "2"));
        response.setInterval(60);
        response.setAllowCaching(true);
        response.setCachingInterval(new Long(1000));
        response.setInformPlatformProxy(true);

        TaskInfo taskInfo = new TaskInfo(response);
        assertEquals(response.getTaskId(), taskInfo.getTaskId());
        assertEquals(response.getCount(), taskInfo.getCount());
        assertEquals(response.getLocation(), taskInfo.getLocation());
        assertEquals(response.getObservesProperty(), taskInfo.getObservesProperty());
        assertEquals(response.getResourceIds(), taskInfo.getResourceIds());
        assertEquals(response.getInterval(), taskInfo.getInterval());
        assertEquals(response.getAllowCaching(), taskInfo.getAllowCaching());
        assertEquals(response.getCachingInterval(), taskInfo.getCachingInterval());
        assertEquals(response.getInformPlatformProxy(), taskInfo.getInformPlatformProxy());
        assertEquals(0, taskInfo.getStoredResourceIds().size());
    }

    @Test
    public void calculateStoredResourceIdsTest() {
        QueryResponse response = new QueryResponse();
        ArrayList<QueryResourceResult> responseResources = new ArrayList<>();

        QueryResourceResult resource1 = new QueryResourceResult();
        resource1.setId("resource1");
        resource1.setPlatformId("platform1");
        responseResources.add(resource1);

        QueryResourceResult resource2 = new QueryResourceResult();
        resource2.setId("resource2");
        resource2.setPlatformId("platform2");
        responseResources.add(resource2);

        QueryResourceResult resource3 = new QueryResourceResult();
        resource3.setId("resource3");
        resource3.setPlatformId("platform3");
        responseResources.add(resource3);

        QueryResourceResult resource4 = new QueryResourceResult();
        resource4.setId("resource4");
        resource4.setPlatformId("platform4");
        responseResources.add(resource4);

        QueryResourceResult resource5 = new QueryResourceResult();
        resource5.setId("resource5");
        resource5.setPlatformId("platform5");
        responseResources.add(resource5);

        response.setResources(responseResources);

        TaskInfo taskInfo = new TaskInfo();
        taskInfo.setResourceIds(Arrays.asList("resource1", "resource2"));
        taskInfo.calculateStoredResourceIds(response);

        assertEquals(3, taskInfo.getStoredResourceIds().size());
        assertEquals("resource3", taskInfo.getStoredResourceIds().get(0));
        assertEquals("resource4", taskInfo.getStoredResourceIds().get(1));
        assertEquals("resource5", taskInfo.getStoredResourceIds().get(2));
    }

}
