package eu.h2020.symbiote.enabler.resourcemanager.integration;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.h2020.symbiote.core.ci.QueryResourceResult;
import eu.h2020.symbiote.core.ci.QueryResponse;
import eu.h2020.symbiote.core.ci.SparqlQueryRequest;
import eu.h2020.symbiote.core.internal.CoreQueryRequest;
import eu.h2020.symbiote.enabler.messaging.model.ResourceManagerAcquisitionStartRequest;
import eu.h2020.symbiote.enabler.messaging.model.ResourceManagerTaskInfoRequest;
import eu.h2020.symbiote.enabler.messaging.model.ResourceManagerUpdateRequest;
import eu.h2020.symbiote.enabler.resourcemanager.integration.dummyListeners.DummyEnablerLogicListener;
import eu.h2020.symbiote.enabler.resourcemanager.integration.dummyListeners.DummyPlatformProxyListener;
import eu.h2020.symbiote.enabler.resourcemanager.repository.TaskInfoRepository;
import eu.h2020.symbiote.enabler.resourcemanager.utils.AuthorizationManager;
import eu.h2020.symbiote.enabler.resourcemanager.utils.ProblematicResourcesHandler;
import eu.h2020.symbiote.enabler.resourcemanager.utils.SearchHelper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.amqp.rabbit.AsyncRabbitTemplate;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.*;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ContextConfiguration
@Configuration
@ComponentScan
@ActiveProfiles("test")
public abstract class AbstractTestClass {

    private static Log log = LogFactory
            .getLog(AbstractTestClass.class);

    @Autowired
    protected AsyncRabbitTemplate asyncRabbitTemplate;

    @Autowired
    protected TaskInfoRepository taskInfoRepository;

    @Autowired
    protected DummyPlatformProxyListener dummyPlatformProxyListener;

    @Autowired
    protected DummyEnablerLogicListener dummyEnablerLogicListener;

    @Autowired
    protected AuthorizationManager authorizationManager;

    @Autowired
    protected SearchHelper searchHelper;

    @Autowired
    protected ProblematicResourcesHandler problematicResourcesHandler;

    @Autowired
    protected RestTemplate restTemplate;

    @Autowired
    protected RabbitTemplate rabbitTemplate;

    @Autowired
    @Qualifier("symbIoTeCoreUrl")
    protected String symbIoTeCoreUrl;

    @Value("${rabbit.exchange.resourceManager.name}")
    protected String resourceManagerExchangeName;

    @Value("${rabbit.routingKey.resourceManager.cancelTask}")
    protected String cancelTaskRoutingKey;

    @Value("${rabbit.routingKey.resourceManager.startDataAcquisition}")
    protected String startDataAcquisitionRoutingKey;

    @Value("${rabbit.routingKey.resourceManager.updateTask}")
    protected String updateTaskRoutingKey;

    @Value("${rabbit.routingKey.resourceManager.wrongData}")
    protected String wrongDataRoutingKey;

    @Value("${rabbit.routingKey.resourceManager.unavailableResources}")
    protected String unavailableResourcesRoutingKey;

    protected ObjectMapper mapper = new ObjectMapper();

    // Execute the Setup method before the test.
    @Before
    public void setUp() throws Exception {

        searchHelper.restartTimer();
        dummyPlatformProxyListener.clearRequestsReceivedByListener();
        dummyEnablerLogicListener.clearRequestsReceivedByListener();

        doReturn(new HashMap<>()).when(authorizationManager).requestHomeToken(any());

        doAnswer(new RestTemplateAnswer()).when(restTemplate)
                .exchange(any(String.class), any(HttpMethod.class), any(HttpEntity.class), any(Class.class));
        doAnswer(new RestTemplateAnswer()).when(restTemplate)
                .exchange(any(String.class), any(HttpMethod.class), any(HttpEntity.class), any(ParameterizedTypeReference.class));
        doThrow(new HttpServerErrorException(HttpStatus.INTERNAL_SERVER_ERROR)).when(restTemplate)
                .exchange(eq(symbIoTeCoreUrl + "/resourceUrls?id=badCRAMrespose"), any(HttpMethod.class),
                        any(HttpEntity.class), any(ParameterizedTypeReference.class));
        doThrow(new HttpServerErrorException(HttpStatus.BAD_REQUEST)).when(restTemplate)
                .exchange(eq(symbIoTeCoreUrl + "/query?location_name=Zurich&observed_property=temperature,humidity&should_rank=true"),
                        any(HttpMethod.class), any(HttpEntity.class), eq(QueryResponse.class));

    }

    @After
    public void clearSetup() throws Exception {
        taskInfoRepository.deleteAll();
    }

    protected ResourceManagerAcquisitionStartRequest createValidQueryToResourceManager(int noTasks) {
        ArrayList<ResourceManagerTaskInfoRequest> resources = new ArrayList<>();
        ResourceManagerAcquisitionStartRequest request = new ResourceManagerAcquisitionStartRequest();

        CoreQueryRequest coreQueryRequest1 = new CoreQueryRequest.Builder()
                .locationName("Paris")
                .observedProperty(Arrays.asList("temperature", "humidity"))
                .build();

        ResourceManagerTaskInfoRequest request1 = new ResourceManagerTaskInfoRequest("1", 2,
                coreQueryRequest1, "P0-0-0T0:0:0.06", false, "P0-0-0T0:0:1",
                true, "enablerLogicName", null);

        resources.add(request1);

        if (noTasks > 1) {
            CoreQueryRequest coreQueryRequest2 = new CoreQueryRequest.Builder()
                    .locationName("Athens")
                    .observedProperty(Arrays.asList("air quality"))
                    .build();

            ResourceManagerTaskInfoRequest request2 = new ResourceManagerTaskInfoRequest("2", 1,
                    coreQueryRequest2, "P0-0-0T0:0:0.06", false, "P0-0-0T0:0:1",
                    true, "enablerLogicName2", null);

            resources.add(request2);
        }

        request.setTasks(resources);
        return request;
    }

    protected ResourceManagerUpdateRequest createValidUpdateQueryToResourceManager(int noTasks) {
        ResourceManagerUpdateRequest updateRequest = new ResourceManagerUpdateRequest();
        updateRequest.setTasks(createValidQueryToResourceManager(noTasks).getTasks());
        return updateRequest;
    }

    protected ResourceManagerAcquisitionStartRequest createBadQueryToResourceManager() {
        ArrayList<ResourceManagerTaskInfoRequest> resources = new ArrayList<>();
        ResourceManagerAcquisitionStartRequest request = new ResourceManagerAcquisitionStartRequest();

        CoreQueryRequest coreQueryRequest1 = new CoreQueryRequest.Builder()
                .locationName("Zurich")
                .observedProperty(Arrays.asList("temperature", "humidity"))
                .build();

        ResourceManagerTaskInfoRequest request1 = new ResourceManagerTaskInfoRequest("1", 2,
                coreQueryRequest1, "P0-0-0T0:0:0.06", false, "P0-0-0T0:0:1",
                true, "enablerLogicName", null);

        resources.add(request1);
        request.setTasks(resources);
        return request;
    }


    private class RestTemplateAnswer implements Answer<ResponseEntity> {

        public ResponseEntity<?> answer(InvocationOnMock invocation) {
            String url = (String) invocation.getArguments()[0];
            HttpMethod httpMethod = (HttpMethod) invocation.getArguments()[1];
            HttpEntity httpEntity = (HttpEntity) invocation.getArguments()[2];

            Map<String, String> queryPairs = new HashMap<>();

            String[] pairs = url.substring(url.indexOf('?') + 1).split("&");

            for (String pair : pairs) {
                String[] values = pair.split("=");
                if (values.length > 1)
                    queryPairs.put(values[0], values[1]);
            }

            if (url.contains("/query") && httpMethod == HttpMethod.GET)
                return search(queryPairs);
            else if (url.contains("/resourceUrls") && httpMethod == HttpMethod.GET)
                return getResourceUrls(queryPairs);
            else if (url.contains("/sparqlQuery") && httpMethod == HttpMethod.POST)
                return search((SparqlQueryRequest) httpEntity.getBody());

            return new ResponseEntity<>("Request Not Found", HttpStatus.BAD_REQUEST);
        }

        private ResponseEntity search(Map<String, String> queryPairs) {

            String location = queryPairs.get("location_name");
            String id = queryPairs.get("id");
            boolean should_rank = Boolean.parseBoolean(queryPairs.get("should_rank"));

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            if (!should_rank)
                return new ResponseEntity<>(headers, HttpStatus.BAD_REQUEST);

            ObjectMapper mapper = new ObjectMapper();
            QueryResponse response = new QueryResponse();

            if (id == null) {
                switch (location) {
                    case "Paris": {
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

                        response.setResources(responseResources);

                        try {
                            String responseInString = mapper.writeValueAsString(response);

                            log.info("Server received request for sensors in Paris");
                            log.info("Server woke up and will answer with " + responseInString);
                        } catch (JsonProcessingException e) {
                            log.info(e.toString());
                            return new ResponseEntity<>(headers, HttpStatus.BAD_REQUEST);
                        }

                        break;
                    }
                    case "Athens": {

                        ArrayList<QueryResourceResult> responseResources = new ArrayList<>();

                        QueryResourceResult resource4 = new QueryResourceResult();
                        resource4.setId("resource4");
                        resource4.setPlatformId("platform4");
                        responseResources.add(resource4);

                        QueryResourceResult resource5 = new QueryResourceResult();
                        resource5.setId("resource5");
                        resource5.setPlatformId("platform5");
                        responseResources.add(resource5);

                        response.setResources(responseResources);

                        try {
                            String responseInString = mapper.writeValueAsString(response);

                            log.info("Server received request for sensors in Athens");
                            log.info("Server woke up and will answer with " + responseInString);
                        } catch (JsonProcessingException e) {
                            log.info(e.toString());
                            return new ResponseEntity<>(headers, HttpStatus.BAD_REQUEST);
                        }

                        break;
                    }
                    case "Zurich": {
                        return new ResponseEntity<>(headers, HttpStatus.BAD_REQUEST);
                    }
                }
            } else {
                ArrayList<QueryResourceResult> responseResources = new ArrayList<>();
                QueryResourceResult resource = new QueryResourceResult();
                resource.setId(id);
                resource.setPlatformId("TestPlatform");
                responseResources.add(resource);

                response.setResources(responseResources);
            }

            return new ResponseEntity<>(response, headers, HttpStatus.OK);
        }

        private ResponseEntity search(SparqlQueryRequest sparqlQuery) {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            ObjectMapper mapper = new ObjectMapper();
            QueryResponse response = new QueryResponse();

            if (sparqlQuery != null && sparqlQuery.isValid()) {
                switch (sparqlQuery.getSparqlQuery()) {
                    case "Paris": {
                        ArrayList<QueryResourceResult> responseResources = new ArrayList<>();

                        QueryResourceResult resource1 = new QueryResourceResult();
                        resource1.setId("sparqlResource1");
                        resource1.setPlatformId("platform1");
                        responseResources.add(resource1);

                        QueryResourceResult resource2 = new QueryResourceResult();
                        resource2.setId("sparqlResource2");
                        resource2.setPlatformId("platform2");
                        responseResources.add(resource2);

                        QueryResourceResult resource3 = new QueryResourceResult();
                        resource3.setId("sparqlResource3");
                        resource3.setPlatformId("platform3");
                        responseResources.add(resource3);

                        response.setResources(responseResources);

                        try {
                            String responseInString = mapper.writeValueAsString(response);

                            log.info("Server received a SPARQL request for sensors in Paris");
                            log.info("Server woke up and will answer with " + responseInString);
                        } catch (JsonProcessingException e) {
                            log.info(e.toString());
                            return new ResponseEntity<>(headers, HttpStatus.BAD_REQUEST);
                        }

                        break;
                    }
                    case "Athens": {

                        ArrayList<QueryResourceResult> responseResources = new ArrayList<>();

                        QueryResourceResult resource4 = new QueryResourceResult();
                        resource4.setId("sparqlResource4");
                        resource4.setPlatformId("platform4");
                        responseResources.add(resource4);

                        QueryResourceResult resource5 = new QueryResourceResult();
                        resource5.setId("sparqlResource5");
                        resource5.setPlatformId("platform5");
                        responseResources.add(resource5);

                        response.setResources(responseResources);

                        try {
                            String responseInString = mapper.writeValueAsString(response);

                            log.info("Server received a SPARQL request for sensors in Athens");
                            log.info("Server woke up and will answer with " + responseInString);
                        } catch (JsonProcessingException e) {
                            log.info(e.toString());
                            return new ResponseEntity<>(headers, HttpStatus.BAD_REQUEST);
                        }

                        break;
                    }
                    case "Zurich": {
                        return new ResponseEntity<>(headers, HttpStatus.BAD_REQUEST);
                    }
                }
            } else {
                return new ResponseEntity<>(headers, HttpStatus.BAD_REQUEST);
            }

            return new ResponseEntity<>(response, headers, HttpStatus.OK);
        }

        private ResponseEntity getResourceUrls(Map<String, String> queryPairs) {

            String resourceId = queryPairs.get("id");
            HashMap<String, String> response = new HashMap<>();
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            if (!resourceId.equals("badCRAMrespose")) {
                if (!resourceId.equals("noCRAMurl"))
                    response.put(resourceId, symbIoTeCoreUrl + "/Sensors('" + resourceId + "')");
            } else {
                return new ResponseEntity<>("", headers, HttpStatus.INTERNAL_SERVER_ERROR);
            }

            return new ResponseEntity<>(response, headers, HttpStatus.OK);
        }
    }
}