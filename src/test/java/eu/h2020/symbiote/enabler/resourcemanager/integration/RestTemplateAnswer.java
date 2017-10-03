package eu.h2020.symbiote.enabler.resourcemanager.integration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.h2020.symbiote.core.ci.QueryResourceResult;
import eu.h2020.symbiote.core.ci.QueryResponse;
import eu.h2020.symbiote.core.ci.SparqlQueryRequest;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.http.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class RestTemplateAnswer implements Answer<ResponseEntity> {

    private static Log log = LogFactory
            .getLog(StartDataAcquisitionConsumerTests.class);

    private String symbIoTeCoreUrl;

    public RestTemplateAnswer(String symbIoTeCoreUrl) {
        this.symbIoTeCoreUrl = symbIoTeCoreUrl;
    }

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