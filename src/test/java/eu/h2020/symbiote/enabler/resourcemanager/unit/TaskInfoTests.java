package eu.h2020.symbiote.enabler.resourcemanager.unit;

import eu.h2020.symbiote.core.ci.QueryResourceResult;
import eu.h2020.symbiote.core.ci.QueryResponse;
import eu.h2020.symbiote.core.internal.CoreQueryRequest;
import eu.h2020.symbiote.enabler.messaging.model.ResourceManagerTaskInfoRequest;
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
    public void resourceManagerTaskInfoRequestConstructorTest() {
        ResourceManagerTaskInfoRequest request = new ResourceManagerTaskInfoRequest();
        CoreQueryRequest coreQueryRequest = new CoreQueryRequest.Builder()
                .locationName("Zurich")
                .observedProperty(Arrays.asList("temperature", "humidity"))
                .build();

        request.setTaskId("1");
        request.setMinNoResources(2);
        request.setCoreQueryRequest(coreQueryRequest);
        request.setQueryInterval_ms(60);
        request.setAllowCaching(true);
        request.setCachingInterval_ms(new Long(1000));
        request.setInformPlatformProxy(true);

        TaskInfo taskInfo = new TaskInfo(request);
        assertEquals(request.getTaskId(), taskInfo.getTaskId());
        assertEquals(request.getMinNoResources(), taskInfo.getMinNoResources());
        assertEquals(request.getCoreQueryRequest().getLocation_name(), taskInfo.getCoreQueryRequest().getLocation_name());
        assertEquals(request.getCoreQueryRequest().getObserved_property(), taskInfo.getCoreQueryRequest().getObserved_property());
        assertEquals(request.getQueryInterval_ms(), taskInfo.getQueryInterval_ms());
        assertEquals(request.getAllowCaching(), taskInfo.getAllowCaching());
        assertEquals(request.getCachingInterval_ms(), taskInfo.getCachingInterval_ms());
        assertEquals(request.getInformPlatformProxy(), taskInfo.getInformPlatformProxy());
        assertEquals(0, taskInfo.getResourceIds().size());
        assertEquals(0, taskInfo.getStoredResourceIds().size());
    }

    @Test
    public void resourceManagerTaskInfoResponseConstructorTest() {
        ResourceManagerTaskInfoResponse response = new ResourceManagerTaskInfoResponse();
        CoreQueryRequest coreQueryRequest = new CoreQueryRequest.Builder()
                .locationName("Zurich")
                .observedProperty(Arrays.asList("temperature", "humidity"))
                .build();

        response.setTaskId("1");
        response.setMinNoResources(2);
        response.setCoreQueryRequest(coreQueryRequest);
        response.setResourceIds(Arrays.asList("1", "2"));
        response.setQueryInterval_ms(60);
        response.setAllowCaching(true);
        response.setCachingInterval_ms(new Long(1000));
        response.setInformPlatformProxy(true);

        TaskInfo taskInfo = new TaskInfo(response);
        assertEquals(response.getTaskId(), taskInfo.getTaskId());
        assertEquals(response.getMinNoResources(), taskInfo.getMinNoResources());
        assertEquals(response.getCoreQueryRequest().getLocation_name(), taskInfo.getCoreQueryRequest().getLocation_name());
        assertEquals(response.getCoreQueryRequest().getObserved_property(), taskInfo.getCoreQueryRequest().getObserved_property());
        assertEquals(response.getResourceIds(), taskInfo.getResourceIds());
        assertEquals(response.getQueryInterval_ms(), taskInfo.getQueryInterval_ms());
        assertEquals(response.getAllowCaching(), taskInfo.getAllowCaching());
        assertEquals(response.getCachingInterval_ms(), taskInfo.getCachingInterval_ms());
        assertEquals(response.getInformPlatformProxy(), taskInfo.getInformPlatformProxy());
        assertEquals(0, taskInfo.getStoredResourceIds().size());
    }

    @Test
    public void taskInfoConstructorTest() {
        TaskInfo initialTaskInfo = new TaskInfo();
        CoreQueryRequest coreQueryRequest = new CoreQueryRequest.Builder()
                .locationName("Zurich")
                .observedProperty(Arrays.asList("temperature", "humidity"))
                .build();

        initialTaskInfo.setTaskId("1");
        initialTaskInfo.setMinNoResources(2);
        initialTaskInfo.setCoreQueryRequest(coreQueryRequest);
        initialTaskInfo.setResourceIds(Arrays.asList("1", "2"));
        initialTaskInfo.setQueryInterval_ms(60);
        initialTaskInfo.setAllowCaching(true);
        initialTaskInfo.setCachingInterval_ms(new Long(1000));
        initialTaskInfo.setInformPlatformProxy(true);
        initialTaskInfo.setStoredResourceIds(Arrays.asList("3", "4"));

        TaskInfo taskInfo = new TaskInfo(initialTaskInfo);
        assertEquals(initialTaskInfo.getTaskId(), taskInfo.getTaskId());
        assertEquals(initialTaskInfo.getMinNoResources(), taskInfo.getMinNoResources());
        assertEquals(initialTaskInfo.getCoreQueryRequest().getLocation_name(), taskInfo.getCoreQueryRequest().getLocation_name());
        assertEquals(initialTaskInfo.getCoreQueryRequest().getObserved_property(), taskInfo.getCoreQueryRequest().getObserved_property());
        assertEquals(initialTaskInfo.getResourceIds(), taskInfo.getResourceIds());
        assertEquals(initialTaskInfo.getQueryInterval_ms(), taskInfo.getQueryInterval_ms());
        assertEquals(initialTaskInfo.getAllowCaching(), taskInfo.getAllowCaching());
        assertEquals(initialTaskInfo.getCachingInterval_ms(), taskInfo.getCachingInterval_ms());
        assertEquals(initialTaskInfo.getInformPlatformProxy(), taskInfo.getInformPlatformProxy());
        assertEquals(initialTaskInfo.getStoredResourceIds(), taskInfo.getStoredResourceIds());

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

    @Test
    public void testEquals() {
        TaskInfo taskInfo1 = new TaskInfo();
        CoreQueryRequest coreQueryRequest = new CoreQueryRequest.Builder()
                .locationName("Zurich")
                .observedProperty(Arrays.asList("temperature", "humidity"))
                .build();

        taskInfo1.setTaskId("1");
        taskInfo1.setMinNoResources(2);
        taskInfo1.setCoreQueryRequest(coreQueryRequest);
        ArrayList<String> resourceIds = new ArrayList<>();
        resourceIds.add("3");
        resourceIds.add("4");
        taskInfo1.setResourceIds(resourceIds);
        taskInfo1.setQueryInterval_ms(60);
        taskInfo1.setAllowCaching(true);
        taskInfo1.setCachingInterval_ms(new Long(1000));
        taskInfo1.setInformPlatformProxy(true);
        taskInfo1.setEnablerLogicName("TestEnablerLogic");
        ArrayList<String> StoredResourceIds = new ArrayList<>();
        StoredResourceIds.add("3");
        StoredResourceIds.add("4");
        taskInfo1.setStoredResourceIds(StoredResourceIds);

        TaskInfo taskInfo2 = new TaskInfo(taskInfo1);
        assertEquals(true, taskInfo1.equals(taskInfo2));

        taskInfo2.setEnablerLogicName("vasilis");
        assertEquals("TestEnablerLogic", taskInfo1.getEnablerLogicName());
        assertEquals("vasilis", taskInfo2.getEnablerLogicName());

        taskInfo2.setQueryInterval_ms(70);
        assertEquals(60, (int) taskInfo1.getQueryInterval_ms());
        assertEquals(70, (int) taskInfo2.getQueryInterval_ms());

        taskInfo2.getCoreQueryRequest().setLocation_name("Athens");
        assertEquals("Zurich", taskInfo1.getCoreQueryRequest().getLocation_name());

        taskInfo2.getCoreQueryRequest().setObserved_property(Arrays.asList("temperature", "air quality"));
        assertEquals("humidity", taskInfo1.getCoreQueryRequest().getObserved_property().get(1));

        assertEquals(false, taskInfo1.equals(taskInfo2));


        TaskInfo taskInfo3 = new TaskInfo(taskInfo1);
        taskInfo3.getResourceIds().add("3");
        taskInfo3.getStoredResourceIds().add("6");

        assertEquals(2, taskInfo1.getResourceIds().size());
        assertEquals(2, taskInfo1.getStoredResourceIds().size());

        assertEquals(3, taskInfo3.getResourceIds().size());
        assertEquals(3, taskInfo3.getStoredResourceIds().size());

        assertEquals(false, taskInfo1.getStoredResourceIds().equals(taskInfo3.getStoredResourceIds()));
        assertEquals(false, taskInfo1.equals(taskInfo3));

    }
}
