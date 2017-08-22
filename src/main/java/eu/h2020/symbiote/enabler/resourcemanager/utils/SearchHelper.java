package eu.h2020.symbiote.enabler.resourcemanager.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.h2020.symbiote.core.ci.SparqlQueryRequest;
import eu.h2020.symbiote.enabler.messaging.model.*;
import eu.h2020.symbiote.util.IntervalFormatter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.web.client.HttpClientErrorException;

import eu.h2020.symbiote.core.internal.CoreQueryRequest;
import eu.h2020.symbiote.core.ci.QueryResourceResult;
import eu.h2020.symbiote.core.ci.QueryResponse;
import eu.h2020.symbiote.enabler.resourcemanager.model.TaskInfo;
import eu.h2020.symbiote.enabler.resourcemanager.model.TaskResponseToComponents;
import eu.h2020.symbiote.enabler.resourcemanager.model.QueryAndProcessSearchResponseResult;
import eu.h2020.symbiote.security.constants.AAMConstants;
import eu.h2020.symbiote.security.token.Token;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.Map;

/**
 * Created by vasgl on 7/20/2017.
 */
@Component
public class SearchHelper {

    private static Log log = LogFactory.getLog(SearchHelper.class);

    private String symbIoTeCoreUrl;
    private RestTemplate restTemplate;
    private SecurityManager securityManager;

    @Autowired
    private SearchHelper(@Qualifier("symbIoTeCoreUrl") String symbIoTeCoreUrl, RestTemplate restTemplate,
                         SecurityManager securityManager) {

        Assert.notNull(symbIoTeCoreUrl,"symbIoTeCoreUrl can not be null!");
        this.symbIoTeCoreUrl = symbIoTeCoreUrl;

        Assert.notNull(restTemplate,"RestTemplate can not be null!");
        this.restTemplate = restTemplate;

        Assert.notNull(securityManager,"SecurityManager can not be null!");
        this.securityManager = securityManager;
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
                log.info(e);
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
            log.info(e.toString());
            return null;
        }
    }

    public QueryAndProcessSearchResponseResult queryAndProcessSearchResponse (ResourceManagerTaskInfoRequest taskInfoRequest)  {
        // Query the core for each task

        ObjectMapper mapper = new ObjectMapper();
        QueryAndProcessSearchResponseResult queryAndProcessSearchResponseResult = new QueryAndProcessSearchResponseResult();
        ResourceManagerTaskInfoResponse taskInfoResponse = new ResourceManagerTaskInfoResponse(taskInfoRequest);
        QueryResponse queryResponse = null;
        TaskResponseToComponents taskResponseToComponents = null;

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
                Token token = securityManager.requestCoreToken();
                log.info("Core Token acquired: " + token);

                httpHeaders.set(AAMConstants.TOKEN_HEADER_NAME, token.getToken());
                HttpEntity<SparqlQueryRequest> entity = new HttpEntity<>(sparqlQueryRequest, httpHeaders);
                queryResponseEntity = restTemplate.exchange(
                        buildRequestUrl(taskInfoRequest), HttpMethod.POST, entity, QueryResponse.class);
            }

            try {
                log.info("SymbIoTe Core Response: " + mapper.writeValueAsString(queryResponseEntity));
            } catch (JsonProcessingException e) {
                log.info(e);
            }

            queryResponse = queryResponseEntity.getBody();
            taskResponseToComponents  = processSearchResponse(queryResponse, taskInfoRequest);

            // Finalizing task response to EnablerLogic
            taskInfoResponse.setResourceIds(taskResponseToComponents.getResourceIdsForEnablerLogic());
            if (taskInfoResponse.getResourceIds().size() >= taskInfoResponse.getMinNoResources())
                taskInfoResponse.setStatus(ResourceManagerTaskInfoResponseStatus.SUCCESS);
            else
                taskInfoResponse.setStatus(ResourceManagerTaskInfoResponseStatus.NOT_ENOUGH_RESOURCES);

            // Finallizing request to PlatformProxy
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
            log.info(e.toString());
            taskInfoResponse.setStatus(ResourceManagerTaskInfoResponseStatus.FAILED);
        }

        TaskInfo taskInfo = new TaskInfo(taskInfoResponse);
        if (taskInfoRequest.getAllowCaching())
            taskInfo.calculateStoredResourceIds(queryResponse);
        if (taskResponseToComponents != null &&
                (taskInfoResponse.getStatus() == ResourceManagerTaskInfoResponseStatus.SUCCESS ||
                taskInfoResponse.getStatus() == ResourceManagerTaskInfoResponseStatus.NOT_ENOUGH_RESOURCES))
            taskInfo.addResourceIds(taskResponseToComponents.getPlatformProxyResourceInfoList());

        queryAndProcessSearchResponseResult.setResourceManagerTaskInfoResponse(taskInfoResponse);
        queryAndProcessSearchResponseResult.setTaskInfo(taskInfo);

        return queryAndProcessSearchResponseResult;
    }

    public TaskResponseToComponents processSearchResponse(QueryResponse queryResponse, ResourceManagerTaskInfoRequest taskInfoRequest) {

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

    public TaskResponseToComponents getUrlsFromCram(QueryResourceResult queryResourceResult) {
        TaskResponseToComponents taskResponseToComponents = new TaskResponseToComponents();

        try {
            Token token = securityManager.requestPlatformToken(queryResourceResult.getPlatformId());
            log.info("Platform Token from platform " + queryResourceResult.getPlatformId() + " acquired: " + token);

            // Request resourceUrl from CRAM
            String cramRequestUrl = symbIoTeCoreUrl + "/resourceUrls?id=" + queryResourceResult.getId();
            HttpHeaders cramHttpHeaders = new HttpHeaders();
            cramHttpHeaders.set("Accept", MediaType.APPLICATION_JSON_VALUE);
            cramHttpHeaders.set(AAMConstants.TOKEN_HEADER_NAME, token.getToken());
            cramHttpHeaders.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<String> cramEntity = new HttpEntity<>(cramHttpHeaders);
            ParameterizedTypeReference<Map<String, String>> typeRef = new ParameterizedTypeReference<Map<String, String>>() {};
            ResponseEntity<Map<String, String>> cramResponseEntity = restTemplate.exchange(
                    cramRequestUrl, HttpMethod.GET, cramEntity, typeRef);
            Map<String, String> cramResponse = cramResponseEntity.getBody();

            log.info("CRAM Response: " + cramResponse);

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
        } catch (SecurityException | HttpClientErrorException | HttpServerErrorException e) {
            log.info(e.toString());
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
