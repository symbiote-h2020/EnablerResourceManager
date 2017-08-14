package eu.h2020.symbiote.enabler.resourcemanager.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

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

    public String buildRequestUrl(ResourceManagerTaskInfoRequest taskInfoRequest) {
        // Building the query url for each task
        String url = taskInfoRequest.getCoreQueryRequest().buildQuery(symbIoTeCoreUrl);
        log.info("url= " + url);

        return url;
    }

    public String buildRequestUrl(String resourceId){
        CoreQueryRequest coreQueryRequest = new CoreQueryRequest.Builder()
                .id(resourceId)
                .shouldRank(true)
                .build();
        String url = coreQueryRequest.buildQuery(symbIoTeCoreUrl);
        log.info("url= " + url);

        return url;
    }

    public QueryAndProcessSearchResponseResult queryAndProcessSearchResponse (String queryUrl,
                                                                              ResourceManagerTaskInfoRequest taskInfoRequest)  {

        ObjectMapper mapper = new ObjectMapper();

        // Query the core for each task
        QueryAndProcessSearchResponseResult queryAndProcessSearchResponseResult = new QueryAndProcessSearchResponseResult();
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.set("Accept", MediaType.APPLICATION_JSON_VALUE);
        httpHeaders.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> entity = new HttpEntity<>(httpHeaders);

        ResourceManagerTaskInfoResponse taskInfoResponse = new ResourceManagerTaskInfoResponse(taskInfoRequest);
        PlatformProxyTaskInfo requestToPlatformProxy = new PlatformProxyTaskInfo();
        QueryResponse queryResponse = null;

        // FIX ME: Consider Connection timeouts or errors
        try {
            ResponseEntity<QueryResponse> queryResponseEntity = restTemplate.exchange(
                    queryUrl, HttpMethod.GET, entity, QueryResponse.class);

            try {
                log.info("SymbIoTe Core Response: " + mapper.writeValueAsString(queryResponseEntity));
            } catch (JsonProcessingException e) {
                log.info(e);
            }

            queryResponse = queryResponseEntity.getBody();
            TaskResponseToComponents taskResponseToComponents  = processSearchResponse(queryResponse, taskInfoRequest);

            // Finalizing task response to EnablerLogic
            taskInfoResponse.setResourceIds(taskResponseToComponents.getResourceIdsForEnablerLogic());

            // Finallizing request to PlatformProxy
            if (taskInfoRequest.getInformPlatformProxy()) {
                requestToPlatformProxy.setTaskId(taskInfoResponse.getTaskId());
                requestToPlatformProxy.setQueryInterval_ms(new IntervalFormatter(taskInfoResponse.getQueryInterval()).getMillis());
                requestToPlatformProxy.setEnablerLogicName(taskInfoResponse.getEnablerLogicName());
                requestToPlatformProxy.setResources(taskResponseToComponents.getPlatformProxyResourceInfoList());

                // Store all requests that need to be forwarded to PlatformProxy
                queryAndProcessSearchResponseResult.setPlatformProxyTaskInfo(requestToPlatformProxy);

            }
        } catch (HttpClientErrorException | HttpServerErrorException e) {
            log.info(e.toString());
        }

        TaskInfo taskInfo = new TaskInfo(taskInfoResponse);
        if (taskInfoRequest.getAllowCaching())
            taskInfo.calculateStoredResourceIds(queryResponse);

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
}
