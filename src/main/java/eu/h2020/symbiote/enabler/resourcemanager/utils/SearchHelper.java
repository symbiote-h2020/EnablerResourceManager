package eu.h2020.symbiote.enabler.resourcemanager.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.h2020.symbiote.core.ci.QueryResourceResult;
import eu.h2020.symbiote.core.ci.QueryResponse;
import eu.h2020.symbiote.core.ci.SparqlQueryRequest;
import eu.h2020.symbiote.core.internal.CoreQueryRequest;
import eu.h2020.symbiote.core.internal.ResourceUrlsResponse;
import eu.h2020.symbiote.enabler.messaging.model.*;
import eu.h2020.symbiote.enabler.resourcemanager.model.QueryAndProcessSearchResponseResult;
import eu.h2020.symbiote.enabler.resourcemanager.model.ScheduledTaskInfoUpdate;
import eu.h2020.symbiote.enabler.resourcemanager.model.TaskInfo;
import eu.h2020.symbiote.enabler.resourcemanager.model.TaskResponseToComponents;
import eu.h2020.symbiote.enabler.resourcemanager.repository.TaskInfoRepository;
import eu.h2020.symbiote.security.commons.exceptions.custom.SecurityHandlerException;
import eu.h2020.symbiote.util.IntervalFormatter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;

/**
 * Created by vasgl on 7/20/2017.
 */
@Component
public class SearchHelper {

    private static Log log = LogFactory.getLog(SearchHelper.class);

    private String symbIoTeCoreUrl;
    private RestTemplate restTemplate;
    private AuthorizationManager authorizationManager;
    private Timer timer;
    private Map<String, ScheduledTaskInfoUpdate> scheduledTaskInfoUpdateMap = new HashMap<>();
    private TaskInfoRepository taskInfoRepository;

    @Autowired
    private SearchHelper(@Qualifier("symbIoTeCoreUrl") String symbIoTeCoreUrl, RestTemplate restTemplate,
                         AuthorizationManager authorizationManager, TaskInfoRepository taskInfoRepository) {

        Assert.notNull(symbIoTeCoreUrl,"symbIoTeCoreUrl can not be null!");
        this.symbIoTeCoreUrl = symbIoTeCoreUrl;

        Assert.notNull(restTemplate,"RestTemplate can not be null!");
        this.restTemplate = restTemplate;

        Assert.notNull(authorizationManager,"AuthorizationManager can not be null!");
        this.authorizationManager = authorizationManager;

        Assert.notNull(taskInfoRepository,"TaskInfoRepository can not be null!");
        this.taskInfoRepository = taskInfoRepository;

        this.timer = new Timer();
        startTimer();

        loadTaskInfo();
    }

    public String querySingleResource (String resourceId)  {
        // Query the core for a single resource

        ObjectMapper mapper = new ObjectMapper();

        // ToDo: Consider Connection timeouts or errors
        try {
            HttpHeaders httpHeaders = new HttpHeaders();
            httpHeaders.set("Accept", MediaType.APPLICATION_JSON_VALUE);
            httpHeaders.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<String> entity = new HttpEntity<>(httpHeaders);

            ResponseEntity<QueryResponse> queryResponseEntity = restTemplate.exchange(
                    buildRequestUrl(resourceId), HttpMethod.GET, entity, QueryResponse.class);

            try {
                log.info("SymbIoTe Core Response: " + mapper.writeValueAsString(queryResponseEntity));
            } catch (JsonProcessingException e) {
                log.info("Cannot deserialize SymbIoTe Core Response", e);
            }

            QueryResponse queryResponse = queryResponseEntity.getBody();
            if (queryResponse == null)
                return null;

            if (queryResponse.getResources().size() == 1) {
                TaskResponseToComponents taskResponseToComponents = getUrlsFromCram(queryResponse.getResources().get(0));

                if (taskResponseToComponents.getPlatformProxyResourceInfoList().size() == 1)
                    return taskResponseToComponents.getPlatformProxyResourceInfoList().get(0).getAccessURL();
                else
                    return null;

            } else
                return null;

        } catch (HttpClientErrorException | HttpServerErrorException e) {
            log.info("Exception in querySingleResource", e);
            return null;
        }
    }

    public QueryAndProcessSearchResponseResult queryAndProcessSearchResponse (ResourceManagerTaskInfoRequest taskInfoRequest)  {
        // Query the core for each task

        ObjectMapper mapper = new ObjectMapper();
        QueryAndProcessSearchResponseResult queryAndProcessSearchResponseResult = new QueryAndProcessSearchResponseResult();
        QueryResponse queryResponse = null;
        TaskResponseToComponents taskResponseToComponents = null;

        // Always request ranked results
        taskInfoRequest.getCoreQueryRequest().setShould_rank(true);
        ResourceManagerTaskInfoResponse taskInfoResponse = new ResourceManagerTaskInfoResponse(taskInfoRequest);

        // ToDo: Consider Connection timeouts or errors
        try {
            HttpHeaders httpHeaders = new HttpHeaders();
            httpHeaders.set("Accept", MediaType.APPLICATION_JSON_VALUE);
            httpHeaders.setContentType(MediaType.APPLICATION_JSON);
            ResponseEntity<QueryResponse> queryResponseEntity;
            SparqlQueryRequest sparqlQueryRequest = taskInfoRequest.getSparqlQueryRequest();

            if (sparqlQueryRequest == null) {
                HttpEntity<String> entity = new HttpEntity<>(httpHeaders);
                queryResponseEntity = restTemplate.exchange(
                        buildRequestUrl(taskInfoRequest), HttpMethod.GET, entity, QueryResponse.class);
            }
            else {
                // ToDo: Consider adding Security Request in the headers

                HttpEntity<SparqlQueryRequest> entity = new HttpEntity<>(sparqlQueryRequest, httpHeaders);
                queryResponseEntity = restTemplate.exchange(
                        buildRequestUrl(taskInfoRequest), HttpMethod.POST, entity, QueryResponse.class);
            }

            try {
                log.info("SymbIoTe Core Response: " + mapper.writeValueAsString(queryResponseEntity));
            } catch (JsonProcessingException e) {
                log.info("Cannot deserialize SymbIoTe Core Response", e);
            }

            queryResponse = queryResponseEntity.getBody();
            taskResponseToComponents  = processSearchResponse(queryResponse, taskInfoRequest);

            // Finalizing task response to EnablerLogic
            taskInfoResponse.setResourceIds(taskResponseToComponents.getResourceIdsForEnablerLogic());
            if (taskInfoResponse.getResourceIds().size() >= taskInfoResponse.getMinNoResources())
                taskInfoResponse.setStatus(ResourceManagerTaskInfoResponseStatus.SUCCESS);
            else
                taskInfoResponse.setStatus(ResourceManagerTaskInfoResponseStatus.NOT_ENOUGH_RESOURCES);

            // Finalizing request to PlatformProxy
            if (taskInfoResponse.getInformPlatformProxy() &&
                    taskInfoResponse.getStatus() == ResourceManagerTaskInfoResponseStatus.SUCCESS) {
                PlatformProxyTaskInfo requestToPlatformProxy = new PlatformProxyTaskInfo();

                requestToPlatformProxy.setTaskId(taskInfoResponse.getTaskId());
                requestToPlatformProxy.setQueryInterval_ms(new IntervalFormatter(taskInfoResponse.getQueryInterval()).getMillis());
                requestToPlatformProxy.setEnablerLogicName(taskInfoResponse.getEnablerLogicName());
                requestToPlatformProxy.setResources(taskResponseToComponents.getPlatformProxyResourceInfoList());

                // Store all requests that need to be forwarded to PlatformProxy
                queryAndProcessSearchResponseResult.setPlatformProxyTaskInfo(requestToPlatformProxy);

            }
        } catch (SecurityException | HttpClientErrorException | HttpServerErrorException e) {
            log.info("", e);
            taskInfoResponse.setStatus(ResourceManagerTaskInfoResponseStatus.FAILED);
            // Todo: Add message in taskInfoResponse
        }

        TaskInfo taskInfo = new TaskInfo(taskInfoResponse);
        if (taskInfoRequest.getAllowCaching()) {
            taskInfo.calculateStoredResourceIds(queryResponse);
            configureTaskTimer(taskInfo);
        }

        if (taskResponseToComponents != null &&
                (taskInfoResponse.getStatus() == ResourceManagerTaskInfoResponseStatus.SUCCESS ||
                        taskInfoResponse.getStatus() == ResourceManagerTaskInfoResponseStatus.NOT_ENOUGH_RESOURCES))
            taskInfo.addResourceIds(taskResponseToComponents.getPlatformProxyResourceInfoList());

        queryAndProcessSearchResponseResult.setResourceManagerTaskInfoResponse(taskInfoResponse);
        queryAndProcessSearchResponseResult.setTaskInfo(taskInfo);

        return queryAndProcessSearchResponseResult;
    }

    public void restartTimer() {
        cancelTimer();
        startTimer();
    }

    public void startTimer() {
        timer = new Timer();
    }

    public void cancelTimer() {
        timer.cancel();
        timer.purge();

        for (Map.Entry<String, ScheduledTaskInfoUpdate> entry : scheduledTaskInfoUpdateMap.entrySet()) {
            entry.getValue().cancel();
        }

        scheduledTaskInfoUpdateMap.clear();
    }

    public void configureTaskTimer(TaskInfo taskInfo) {
        ScheduledTaskInfoUpdate oldScheduledTaskInfoUpdate = scheduledTaskInfoUpdateMap.get(taskInfo.getTaskId());
        if (oldScheduledTaskInfoUpdate != null) {
            oldScheduledTaskInfoUpdate.cancel();
            scheduledTaskInfoUpdateMap.remove(taskInfo.getTaskId());
        }

        ScheduledTaskInfoUpdate newScheduledTaskInfoUpdate = new ScheduledTaskInfoUpdate(taskInfoRepository,
                this, taskInfo);
        long delay = new IntervalFormatter(taskInfo.getCachingInterval()).getMillis();
        timer.schedule(newScheduledTaskInfoUpdate, delay);

        scheduledTaskInfoUpdateMap.put(taskInfo.getTaskId(), newScheduledTaskInfoUpdate);
    }

    public void removeTaskTimer(String taskInfoId) {
        if (scheduledTaskInfoUpdateMap.get(taskInfoId) != null) {
            scheduledTaskInfoUpdateMap.get(taskInfoId).cancel();
            scheduledTaskInfoUpdateMap.remove(taskInfoId);
        }
    }

    public Timer getTimer() { return timer; }
    public void setTimer(Timer timer) { this.timer = timer; }

    public Map<String, ScheduledTaskInfoUpdate> getScheduledTaskInfoUpdateMap() {
        return scheduledTaskInfoUpdateMap;
    }

    public void setScheduledTaskInfoUpdateMap(Map<String, ScheduledTaskInfoUpdate> scheduledTaskInfoUpdateMap) {
        this.scheduledTaskInfoUpdateMap = scheduledTaskInfoUpdateMap;
    }


    private void loadTaskInfo() {
        // Todo: load taskInfo at startup
    }

    private TaskResponseToComponents processSearchResponse(QueryResponse queryResponse, ResourceManagerTaskInfoRequest taskInfoRequest) {

        List<QueryResourceResult> queryResultLists = queryResponse.getResources();
        TaskResponseToComponents taskResponseToComponents = new TaskResponseToComponents();

        // Process the response for each task
        for (QueryResourceResult queryResourceResult : queryResultLists) {

            if (taskResponseToComponents.getCount() >= taskInfoRequest.getMinNoResources())
                break;

            TaskResponseToComponents newTaskResponseToComponents = getUrlsFromCram(queryResourceResult);
            taskResponseToComponents.add(newTaskResponseToComponents);

        }

        return taskResponseToComponents;
    }

    private TaskResponseToComponents getUrlsFromCram(QueryResourceResult queryResourceResult) {
        TaskResponseToComponents taskResponseToComponents = new TaskResponseToComponents();

        try {
            Map<String, String> securityRequestHeaders =  authorizationManager
                    .requestHomeToken(queryResourceResult.getPlatformId());
            log.debug("SecurityRequest from platform " + queryResourceResult.getPlatformId() + " acquired: " + securityRequestHeaders);

            // Request resourceUrl from CRAM
            String cramRequestUrl = symbIoTeCoreUrl + "/resourceUrls?id=" + queryResourceResult.getId();
            HttpHeaders cramHttpHeaders = new HttpHeaders();

            // Add Security Request Headers
            for (Map.Entry<String, String> entry : securityRequestHeaders.entrySet()) {
                cramHttpHeaders.add(entry.getKey(), entry.getValue());
            }
            cramHttpHeaders.set("Accept", MediaType.APPLICATION_JSON_VALUE);
            cramHttpHeaders.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<String> cramEntity = new HttpEntity<>(cramHttpHeaders);

            ResponseEntity<ResourceUrlsResponse> cramResponseEntity = restTemplate.exchange(
                    cramRequestUrl, HttpMethod.GET, cramEntity, ResourceUrlsResponse.class);

            if (!authorizationManager.verifyServiceResponse(cramResponseEntity.getHeaders(), queryResourceResult.getPlatformId()))
                throw new SecurityHandlerException("Service Response was not verified");

            Map<String, String> cramResponse = cramResponseEntity.getBody().getBody();

            log.debug("CRAM Response: " + cramResponse);

            if (cramResponse != null) {
                String resourceUrl = cramResponse.get(queryResourceResult.getId());
                if (resourceUrl != null) {
                    // Building the request to PlatformProxy
                    PlatformProxyResourceInfo platformProxyResourceInfo = new PlatformProxyResourceInfo();
                    platformProxyResourceInfo.setResourceId(queryResourceResult.getId());
                    platformProxyResourceInfo.setAccessURL(resourceUrl);
                    taskResponseToComponents.addToPlatformProxyResourceInfoList(platformProxyResourceInfo);
                    taskResponseToComponents.addToCount(1);

                    // Save the id for returning it to the EnablerLogic
                    taskResponseToComponents.addToResourceIdsForEnablerLogic(queryResourceResult.getId());
                }
            }
        } catch (SecurityHandlerException | HttpClientErrorException | HttpServerErrorException e) {
            log.info("Exception in getUrlsFromCram()", e);
        }

        return taskResponseToComponents;
    }

    private String buildRequestUrl(ResourceManagerTaskInfoRequest taskInfoRequest) {
        // Building the query url for each task
        String url;
        SparqlQueryRequest sparqlQueryRequest = taskInfoRequest.getSparqlQueryRequest();

        if (sparqlQueryRequest == null || sparqlQueryRequest.getSparqlQuery() == null ||
                sparqlQueryRequest.getOutputFormat() == null)
            url = taskInfoRequest.getCoreQueryRequest().buildQuery(symbIoTeCoreUrl);
        else
            url = symbIoTeCoreUrl + "/sparqlQuery";

        log.info("url = " + url);

        return url;
    }

    private String buildRequestUrl(String resourceId){
        CoreQueryRequest coreQueryRequest = new CoreQueryRequest.Builder()
                .id(resourceId)
                .shouldRank(true)
                .build();
        String url = coreQueryRequest.buildQuery(symbIoTeCoreUrl);
        log.info("url= " + url);

        return url;
    }
}
