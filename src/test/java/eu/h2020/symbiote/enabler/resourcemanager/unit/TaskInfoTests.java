package eu.h2020.symbiote.enabler.resourcemanager.unit;

import eu.h2020.symbiote.core.ci.QueryResourceResult;
import eu.h2020.symbiote.core.ci.QueryResponse;
import eu.h2020.symbiote.core.ci.SparqlQueryOutputFormat;
import eu.h2020.symbiote.core.ci.SparqlQueryRequest;
import eu.h2020.symbiote.core.internal.CoreQueryRequest;
import eu.h2020.symbiote.enabler.messaging.model.PlatformProxyResourceInfo;
import eu.h2020.symbiote.enabler.messaging.model.ResourceManagerTaskInfoRequest;
import eu.h2020.symbiote.enabler.messaging.model.ResourceManagerTaskInfoResponse;
import eu.h2020.symbiote.enabler.messaging.model.ResourceManagerTaskInfoResponseStatus;
import eu.h2020.symbiote.enabler.resourcemanager.model.TaskInfo;
import eu.h2020.symbiote.util.IntervalFormatter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.*;

import static org.junit.Assert.*;

/**
 * Created by vasgl on 7/17/2017.
 */
@RunWith(SpringJUnit4ClassRunner.class)
public class TaskInfoTests {

    @Test
    public void resourceManagerTaskInfoRequestConstructorTest() {

        String taskId = "1";
        int minNoResources = 2;
        int maxNoResources = ResourceManagerTaskInfoRequest.ALL_AVAILABLE_RESOURCES;
        CoreQueryRequest coreQueryRequest = new CoreQueryRequest.Builder()
                .locationName("Zurich")
                .observedProperty(Arrays.asList("temperature", "humidity"))
                .build();
        String queryInterval = "P0-0-0T0:0:0.06";
        boolean allowCaching = true;
        String cachingInterval = "P0-0-0T0:0:1";
        boolean informPlatformProxy = true;
        String enablerLogicName = "enablerLogicName";
        SparqlQueryRequest sparqlQueryRequest = null;

        ResourceManagerTaskInfoRequest request = new ResourceManagerTaskInfoRequest(
                taskId, minNoResources, maxNoResources, coreQueryRequest, queryInterval, allowCaching,
                cachingInterval, informPlatformProxy, enablerLogicName, sparqlQueryRequest

        );

        TaskInfo taskInfo = new TaskInfo(request);
        assertEquals(request.getTaskId(), taskInfo.getTaskId());
        assertEquals(request.getMinNoResources(), taskInfo.getMinNoResources());
        assertEquals(request.getCoreQueryRequest().getLocation_name(), taskInfo.getCoreQueryRequest().getLocation_name());
        assertEquals(request.getCoreQueryRequest().getObserved_property(), taskInfo.getCoreQueryRequest().getObserved_property());
        assertEquals(request.getQueryInterval(), taskInfo.getQueryInterval());
        assertEquals(request.getAllowCaching(), taskInfo.getAllowCaching());
        assertEquals(request.getCachingInterval(), taskInfo.getCachingInterval());
        assertEquals(request.getInformPlatformProxy(), taskInfo.getInformPlatformProxy());
        assertEquals(request.getEnablerLogicName(), taskInfo.getEnablerLogicName());
        assertEquals(ResourceManagerTaskInfoResponseStatus.UNKNOWN, taskInfo.getStatus());
        assertEquals(0, taskInfo.getResourceIds().size());
        assertEquals(0, taskInfo.getResourceDescriptions().size());
        assertEquals(0, taskInfo.getStoredResourceIds().size());
        assertEquals(0, taskInfo.getResourceUrls().size());
    }

    @Test
    public void resourceManagerTaskInfoResponseConstructorTest() {
        String taskId = "1";
        int minNoResources = 2;
        int maxNoResources = ResourceManagerTaskInfoRequest.ALL_AVAILABLE_RESOURCES;
        CoreQueryRequest coreQueryRequest = new CoreQueryRequest.Builder()
                .locationName("Zurich")
                .observedProperty(Arrays.asList("temperature", "humidity"))
                .build();
        String queryInterval = "P0-0-0T0:0:0.06";
        boolean allowCaching = true;
        String cachingInterval = "P0-0-0T0:0:1";
        boolean informPlatformProxy = true;
        String enablerLogicName = "enablerLogicName";
        SparqlQueryRequest sparqlQueryRequest = null;
        List<String> resourceIds = Arrays.asList("1", "2");

        HashMap<String, String> resourceUrls = new HashMap<>();
        resourceUrls.put("1", "1.com");
        resourceUrls.put("2", "2.com");

        QueryResourceResult result1 = new QueryResourceResult();
        QueryResourceResult result2 = new QueryResourceResult();
        result1.setId("1");
        result2.setId("2");
        List<QueryResourceResult> resourceDescriptions = Arrays.asList(result1, result2);
        ResourceManagerTaskInfoResponseStatus status = ResourceManagerTaskInfoResponseStatus.UNKNOWN;
        String message = "message";

        ResourceManagerTaskInfoResponse response = new ResourceManagerTaskInfoResponse(
                taskId, minNoResources, maxNoResources, coreQueryRequest, queryInterval,
                allowCaching, cachingInterval, informPlatformProxy, enablerLogicName,
                sparqlQueryRequest, resourceIds, resourceUrls, resourceDescriptions, status, message
        );

        TaskInfo taskInfo = new TaskInfo(response);
        assertEquals(response.getTaskId(), taskInfo.getTaskId());
        assertEquals(response.getMinNoResources(), taskInfo.getMinNoResources());
        assertEquals(response.getCoreQueryRequest().getLocation_name(), taskInfo.getCoreQueryRequest().getLocation_name());
        assertEquals(response.getCoreQueryRequest().getObserved_property(), taskInfo.getCoreQueryRequest().getObserved_property());
        assertEquals(response.getResourceIds(), taskInfo.getResourceIds());
        assertEquals(response.getResourceUrls(), taskInfo.getResourceUrls());
        assertEquals(response.getResourceDescriptions(), taskInfo.getResourceDescriptions());
        assertEquals(response.getQueryInterval(), taskInfo.getQueryInterval());
        assertEquals(response.getAllowCaching(), taskInfo.getAllowCaching());
        assertEquals(response.getCachingInterval(), taskInfo.getCachingInterval());
        assertEquals(response.getInformPlatformProxy(), taskInfo.getInformPlatformProxy());
        assertEquals(response.getEnablerLogicName(), taskInfo.getEnablerLogicName());
        assertEquals(response.getStatus(), taskInfo.getStatus());
        assertEquals(response.getMessage(), taskInfo.getMessage());
        assertEquals(0, taskInfo.getStoredResourceIds().size());
        assertEquals(2, taskInfo.getResourceUrls().size());
    }

    @Test
    public void taskInfoConstructorTest() {

        TaskInfo initialTaskInfo = sampleTaskInfo();

        TaskInfo taskInfo = new TaskInfo(initialTaskInfo);
        assertEquals(initialTaskInfo.getTaskId(), taskInfo.getTaskId());
        assertEquals(initialTaskInfo.getMinNoResources(), taskInfo.getMinNoResources());
        assertEquals(initialTaskInfo.getMaxNoResources(), taskInfo.getMaxNoResources());
        assertEquals(initialTaskInfo.getCoreQueryRequest().getLocation_name(), taskInfo.getCoreQueryRequest().getLocation_name());
        assertEquals(initialTaskInfo.getCoreQueryRequest().getObserved_property(), taskInfo.getCoreQueryRequest().getObserved_property());
        assertEquals(initialTaskInfo.getResourceIds(), taskInfo.getResourceIds());
        assertEquals(initialTaskInfo.getResourceDescriptions(), taskInfo.getResourceDescriptions());
        assertEquals(initialTaskInfo.getQueryInterval(), taskInfo.getQueryInterval());
        assertEquals(initialTaskInfo.getAllowCaching(), taskInfo.getAllowCaching());
        assertEquals(initialTaskInfo.getCachingInterval(), taskInfo.getCachingInterval());
        assertEquals(initialTaskInfo.getEnablerLogicName(), taskInfo.getEnablerLogicName());
        assertEquals(initialTaskInfo.getInformPlatformProxy(), taskInfo.getInformPlatformProxy());
        assertEquals(initialTaskInfo.getStatus(), taskInfo.getStatus());
        assertEquals(initialTaskInfo.getStoredResourceIds(), taskInfo.getStoredResourceIds());
        assertEquals(initialTaskInfo.getResourceUrls(), taskInfo.getResourceUrls());

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

        response.setBody(responseResources);

        TaskInfo taskInfo = sampleTaskInfo();
        taskInfo.setResourceIds(new ArrayList<>(Arrays.asList("resource1", "resource2")));
        taskInfo.setStoredResourceIds(new ArrayList<>());
        taskInfo.calculateStoredResourceIds(response);

        assertEquals(3, taskInfo.getStoredResourceIds().size());
        assertEquals("resource3", taskInfo.getStoredResourceIds().get(0));
        assertEquals("resource4", taskInfo.getStoredResourceIds().get(1));
        assertEquals("resource5", taskInfo.getStoredResourceIds().get(2));
    }

    @Test
    public void addResourceIdsMapTest() {
        TaskInfo taskInfo = sampleTaskInfo();
        Map<String, String> resourceUrls = new HashMap<>();
        resourceUrls.put("1", "http://1.com");
        resourceUrls.put("2", "http://2.com");

        taskInfo.setResourceIds(new ArrayList<>(Arrays.asList("1", "2")));
        taskInfo.setResourceUrls(resourceUrls);

        Map<String, String> newResourceUrls = new HashMap<>();
        newResourceUrls.put("1", "http://1new.com");
        newResourceUrls.put("2", "http://2.com");
        newResourceUrls.put("3", "http://3.com");
        newResourceUrls.put("4", "http://4.com");

        ArrayList<QueryResourceResult> results = new ArrayList<>();
        QueryResourceResult result1 = new QueryResourceResult();
        result1.setId("1");
        results.add(result1);
        QueryResourceResult result2 = new QueryResourceResult();
        result2.setId("2");
        results.add(result2);
        QueryResourceResult result3 = new QueryResourceResult();
        result3.setId("3");
        results.add(result3);
        QueryResourceResult result4 = new QueryResourceResult();
        result4.setId("4");
        results.add(result4);

        taskInfo.addResourceIds(newResourceUrls, results);

        assertEquals(4, taskInfo.getResourceIds().size());
        assertEquals("1", taskInfo.getResourceIds().get(0));
        assertEquals("2", taskInfo.getResourceIds().get(1));
        assertEquals("3", taskInfo.getResourceIds().get(2));
        assertEquals("4", taskInfo.getResourceIds().get(3));

        assertEquals(4, taskInfo.getResourceUrls().size());
        assertEquals("http://1new.com", taskInfo.getResourceUrls().get("1"));
        assertEquals("http://2.com", taskInfo.getResourceUrls().get("2"));
        assertEquals("http://3.com", taskInfo.getResourceUrls().get("3"));
        assertEquals("http://4.com", taskInfo.getResourceUrls().get("4"));

        assertEquals(4, taskInfo.getResourceDescriptions().size());
        assertEquals(result1, taskInfo.getResourceDescriptions().get(0));
        assertEquals(result2, taskInfo.getResourceDescriptions().get(1));
        assertEquals(result3, taskInfo.getResourceDescriptions().get(2));
        assertEquals(result4, taskInfo.getResourceDescriptions().get(3));
    }

    @Test
    public void addResourceIdsPlatformProxyResourceInfoListTest() {
        TaskInfo taskInfo = sampleTaskInfo();
        Map<String, String> resourceUrls = new HashMap<>();
        resourceUrls.put("1", "http://1.com");
        resourceUrls.put("2", "http://2.com");

        taskInfo.setResourceIds(new ArrayList<>(Arrays.asList("1", "2")));
        taskInfo.setResourceUrls(resourceUrls);

        PlatformProxyResourceInfo platformProxyResourceInfo1 = new PlatformProxyResourceInfo();
        platformProxyResourceInfo1.setResourceId("1");
        platformProxyResourceInfo1.setAccessURL("http://1new.com");

        PlatformProxyResourceInfo platformProxyResourceInfo2 = new PlatformProxyResourceInfo();
        platformProxyResourceInfo2.setResourceId("2");
        platformProxyResourceInfo2.setAccessURL("http://2.com");

        PlatformProxyResourceInfo platformProxyResourceInfo3 = new PlatformProxyResourceInfo();
        platformProxyResourceInfo3.setResourceId("3");
        platformProxyResourceInfo3.setAccessURL("http://3.com");

        PlatformProxyResourceInfo platformProxyResourceInfo4 = new PlatformProxyResourceInfo();
        platformProxyResourceInfo4.setResourceId("4");
        platformProxyResourceInfo4.setAccessURL("http://4.com");

        ArrayList<PlatformProxyResourceInfo> platformProxyResourceInfoArrayList = new ArrayList<>();
        platformProxyResourceInfoArrayList.add(platformProxyResourceInfo1);
        platformProxyResourceInfoArrayList.add(platformProxyResourceInfo2);
        platformProxyResourceInfoArrayList.add(platformProxyResourceInfo3);
        platformProxyResourceInfoArrayList.add(platformProxyResourceInfo4);

        taskInfo.addResourceIds(platformProxyResourceInfoArrayList);

        assertEquals(4, taskInfo.getResourceIds().size());
        assertEquals("1", taskInfo.getResourceIds().get(0));
        assertEquals("2", taskInfo.getResourceIds().get(1));
        assertEquals("3", taskInfo.getResourceIds().get(2));
        assertEquals("4", taskInfo.getResourceIds().get(3));

        assertEquals(4, taskInfo.getResourceUrls().size());
        assertEquals("http://1new.com", taskInfo.getResourceUrls().get("1"));
        assertEquals("http://2.com", taskInfo.getResourceUrls().get("2"));
        assertEquals("http://3.com", taskInfo.getResourceUrls().get("3"));
        assertEquals("http://4.com", taskInfo.getResourceUrls().get("4"));
    }

    @Test
    public void deleteResourceIdsTest() {
        TaskInfo taskInfo = sampleTaskInfo();
        Map<String, String> resourceUrls = new HashMap<>();
        resourceUrls.put("1", "http://1.com");
        resourceUrls.put("2", "http://2.com");

        ArrayList<QueryResourceResult> results = new ArrayList<>();
        QueryResourceResult result1 = new QueryResourceResult();
        result1.setId("1");
        QueryResourceResult result2 = new QueryResourceResult();
        result2.setId("2");
        results.add(result1);
        results.add(result2);

        taskInfo.setResourceIds(new ArrayList<>(Arrays.asList("1", "2")));
        taskInfo.setResourceUrls(resourceUrls);
        taskInfo.setResourceDescriptions(results);

        taskInfo.deleteResourceIds(new ArrayList<>(Arrays.asList("1", "3")));

        assertEquals(1, taskInfo.getResourceIds().size());
        assertEquals("2", taskInfo.getResourceIds().get(0));

        assertEquals(1, taskInfo.getResourceDescriptions().size());
        assertEquals("2", taskInfo.getResourceDescriptions().get(0).getId());

        assertEquals(1, taskInfo.getResourceUrls().size());
        assertEquals("http://2.com", taskInfo.getResourceUrls().get("2"));

    }

    @Test
    public void createPlatformProxyResourceInfoListTest() {
        TaskInfo taskInfo = sampleTaskInfo();
        Map<String, String> resourceUrls = new HashMap<>();
        resourceUrls.put("1", "http://1.com");
        resourceUrls.put("2", "http://2.com");

        taskInfo.setResourceIds(new ArrayList<>(Arrays.asList("1", "2")));
        taskInfo.setResourceUrls(resourceUrls);

        List<PlatformProxyResourceInfo> platformProxyResourceInfoList = taskInfo.createPlatformProxyResourceInfoList();

        assertEquals(2, platformProxyResourceInfoList.size());

        boolean foundResource1 = false;
        boolean foundResource2 = false;

        for (PlatformProxyResourceInfo resourceInfo : platformProxyResourceInfoList) {

            if (resourceInfo.getResourceId().equals("1")) {
                assertEquals("http://1.com", resourceInfo.getAccessURL());
                foundResource1 = true;
                continue;
            }

            if (resourceInfo.getResourceId().equals("2")) {
                assertEquals("http://2.com", resourceInfo.getAccessURL());
                foundResource2 = true;
                continue;
            }

            fail("The code should not reach here, because no other resources should be present");
        }

        assertTrue(foundResource1);
        assertTrue(foundResource2);
    }

    @Test
    public void testEquals() {
        CoreQueryRequest coreQueryRequest = new CoreQueryRequest.Builder()
                .locationName("Zurich")
                .observedProperty(Arrays.asList("temperature", "humidity"))
                .build();

        SparqlQueryRequest sparqlQueryRequest = new SparqlQueryRequest("taskInfo1",
                SparqlQueryOutputFormat.COUNT, null);

        ArrayList<String> resourceIds = new ArrayList<>();
        resourceIds.add("1");
        resourceIds.add("2");

        ArrayList<String> storedResourceIds = new ArrayList<>();
        storedResourceIds.add("3");
        storedResourceIds.add("4");

        Map<String, String> resourceUrls = new HashMap<>();
        resourceUrls.put("1", "http://1.com");
        resourceUrls.put("2", "http://2.com");

        ArrayList<QueryResourceResult> results = new ArrayList<>();
        QueryResourceResult result1 = new QueryResourceResult();
        QueryResourceResult result2 = new QueryResourceResult();
        result1.setId("1");
        result2.setId("2");
        results.add(result1);
        results.add(result2);

        TaskInfo taskInfo1 = new TaskInfo("1", 2, coreQueryRequest, "P0-0-0T0:0:0.06",
                true, "P0-0-0T0:0:1", true,
                "TestEnablerLogic", sparqlQueryRequest, resourceIds, results,
                ResourceManagerTaskInfoResponseStatus.SUCCESS, storedResourceIds, resourceUrls, "message");

        TaskInfo taskInfo2 = new TaskInfo(taskInfo1);
        assertEquals(taskInfo1, taskInfo2);

        taskInfo2.setTaskId("2");
        assertEquals("1", taskInfo1.getTaskId());
        assertNotEquals(taskInfo1, taskInfo2);
        taskInfo2.setTaskId(taskInfo1.getTaskId());
        assertEquals(taskInfo1, taskInfo2);

        taskInfo2.setMinNoResources(5);
        assertEquals(2, (int) taskInfo1.getMinNoResources());
        assertNotEquals(taskInfo1, taskInfo2);
        taskInfo2.setMinNoResources(taskInfo1.getMinNoResources());
        assertEquals(taskInfo1, taskInfo2);

        taskInfo2.getCoreQueryRequest().setLocation_name("Athens");
        assertEquals("Zurich", taskInfo1.getCoreQueryRequest().getLocation_name());
        assertNotEquals(taskInfo1, taskInfo2);
        taskInfo2.getCoreQueryRequest().setLocation_name("Zurich");
        assertEquals(taskInfo1, taskInfo2);

        taskInfo2.setQueryInterval("P0-0-0T0:0:0.07");
        assertEquals(60, (long) new IntervalFormatter(taskInfo1.getQueryInterval()).getMillis());
        assertNotEquals(taskInfo1, taskInfo2);
        taskInfo2.setQueryInterval(taskInfo1.getQueryInterval());
        assertEquals(taskInfo1, taskInfo2);

        taskInfo2.setAllowCaching(false);
        assertEquals(true, taskInfo1.getAllowCaching());
        assertNotEquals(taskInfo1, taskInfo2);
        taskInfo2.setAllowCaching(taskInfo1.getAllowCaching());
        assertEquals(taskInfo1, taskInfo2);

        taskInfo2.setCachingInterval("P0-0-0T0:0:0.07");
        assertEquals(1000, (long) new IntervalFormatter(taskInfo1.getCachingInterval()).getMillis());
        assertNotEquals(taskInfo1, taskInfo2);
        taskInfo2.setCachingInterval(taskInfo1.getCachingInterval());
        assertEquals(taskInfo1, taskInfo2);

        taskInfo2.setInformPlatformProxy(false);
        assertEquals(true, taskInfo1.getInformPlatformProxy());
        assertNotEquals(taskInfo1, taskInfo2);
        taskInfo2.setInformPlatformProxy(taskInfo1.getInformPlatformProxy());
        assertEquals(taskInfo1, taskInfo2);

        taskInfo2.setEnablerLogicName("Test");
        assertEquals("TestEnablerLogic", taskInfo1.getEnablerLogicName());
        assertNotEquals(taskInfo1, taskInfo2);
        taskInfo2.setEnablerLogicName(taskInfo1.getEnablerLogicName());
        assertEquals(taskInfo1, taskInfo2);

        taskInfo2.getSparqlQueryRequest().setSparqlQuery("taskInfo2");
        assertEquals("taskInfo1", taskInfo1.getSparqlQueryRequest().getSparqlQuery());
        assertNotEquals(taskInfo1, taskInfo2);
        taskInfo2.getSparqlQueryRequest().setSparqlQuery(taskInfo1.getSparqlQueryRequest().getSparqlQuery());
        assertEquals(taskInfo1, taskInfo2);

        taskInfo2.getSparqlQueryRequest().setOutputFormat(SparqlQueryOutputFormat.CSV);
        assertEquals(SparqlQueryOutputFormat.COUNT, taskInfo1.getSparqlQueryRequest().getOutputFormat());
        assertNotEquals(taskInfo1, taskInfo2);
        taskInfo2.getSparqlQueryRequest().setOutputFormat(taskInfo1.getSparqlQueryRequest().getOutputFormat());
        assertEquals(taskInfo1, taskInfo2);

        taskInfo2.getResourceIds().add("3");
        assertEquals(2, taskInfo1.getResourceIds().size());
        assertNotEquals(taskInfo1, taskInfo2);
        taskInfo2.setResourceIds(taskInfo1.getResourceIds());
        assertEquals(taskInfo1, taskInfo2);

        taskInfo2.setStatus(ResourceManagerTaskInfoResponseStatus.FAILED);
        assertEquals(ResourceManagerTaskInfoResponseStatus.SUCCESS, taskInfo1.getStatus());
        assertNotEquals(taskInfo1, taskInfo2);
        taskInfo2.setStatus(taskInfo1.getStatus());
        assertEquals(taskInfo1, taskInfo2);

        taskInfo2.getStoredResourceIds().add("5");
        assertEquals(2, taskInfo1.getStoredResourceIds().size());
        assertNotEquals(taskInfo1, taskInfo2);
        taskInfo2.setStoredResourceIds(taskInfo1.getStoredResourceIds());
        assertEquals(taskInfo1, taskInfo2);

        taskInfo2.getResourceUrls().put("5", "http://5.com");
        assertEquals(2, taskInfo1.getResourceUrls().size());
        assertNotEquals(taskInfo1, taskInfo2);
        taskInfo2.setResourceUrls(taskInfo1.getResourceUrls());
        assertEquals(taskInfo1, taskInfo2);

        taskInfo2.setMessage("blah");
        assertEquals("message", taskInfo1.getMessage());
        assertNotEquals(taskInfo1, taskInfo2);
        taskInfo2.setMessage(taskInfo1.getMessage());
        assertEquals(taskInfo1, taskInfo2);
    }

    private TaskInfo sampleTaskInfo() {
        String taskId = "1";
        int minNoResources = 2;
        int maxNoResources = ResourceManagerTaskInfoRequest.ALL_AVAILABLE_RESOURCES;
        CoreQueryRequest coreQueryRequest = new CoreQueryRequest.Builder()
                .locationName("Zurich")
                .observedProperty(Arrays.asList("temperature", "humidity"))
                .build();
        String queryInterval = "P0-0-0T0:0:0.06";
        boolean allowCaching = true;
        String cachingInterval = "P0-0-0T0:0:1";
        boolean informPlatformProxy = true;
        String enablerLogicName = "enablerLogicName";
        SparqlQueryRequest sparqlQueryRequest = null;
        List<String> resourceIds = Arrays.asList("1", "2");

        Map<String, String> resourceUrls = new HashMap<>();
        resourceUrls.put("1", "http://1.com");
        resourceUrls.put("2", "http://2.com");

        QueryResourceResult result1 = new QueryResourceResult();
        QueryResourceResult result2 = new QueryResourceResult();
        result1.setId("1");
        result2.setId("2");
        List<QueryResourceResult> resourceDescriptions = Arrays.asList(result1, result2);
        ResourceManagerTaskInfoResponseStatus status = ResourceManagerTaskInfoResponseStatus.UNKNOWN;
        String message = "message";
        List<String> storedResourceIds = new ArrayList<>(Arrays.asList("3", "4"));

        return new TaskInfo(
                taskId, minNoResources, maxNoResources, coreQueryRequest, queryInterval, allowCaching,
                cachingInterval, informPlatformProxy, enablerLogicName, sparqlQueryRequest, resourceIds,
                resourceDescriptions, status, storedResourceIds, resourceUrls, message
        );
    }
}
