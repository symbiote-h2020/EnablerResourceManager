package eu.h2020.symbiote.enabler.resourcemanager.dummyListeners;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.h2020.symbiote.core.ci.QueryResourceResult;
import eu.h2020.symbiote.core.ci.QueryResponse;
import eu.h2020.symbiote.core.ci.SparqlQueryRequest;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;


/**
 * Dummy REST service mimicking exposed CoreInterface in the Core.
 *
 * @author Vasilis Glykantzis (ICOM)
 */
@RestController
@WebAppConfiguration
public class DummyCoreInterfaceRestListeners {
    private static final Log log = LogFactory.getLog(DummyCoreInterfaceRestListeners.class);

    @Autowired
    @Qualifier("symbIoTeCoreUrl")
    private String symbIoTeCoreUrl;


    @RequestMapping(method = RequestMethod.GET, value = "/query")
    public ResponseEntity search(@RequestParam(value = "location_name", required = false) String location,
                                 @RequestParam(value = "id", required = false) String id,
                                 @RequestParam(value = "should_rank") Boolean should_rank) {
        
        log.info("Search request");

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

    @RequestMapping(method = RequestMethod.POST, value = "/sparqlQuery")
    public ResponseEntity search(@RequestBody SparqlQueryRequest sparqlQuery,
                                 @RequestHeader("X-Auth-Token") String token) {

        log.info("SPARQL Search request");

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        ObjectMapper mapper = new ObjectMapper();
        QueryResponse response = new QueryResponse();

        if (sparqlQuery != null && sparqlQuery.isValid() && token != null ) {
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

    @RequestMapping(method = RequestMethod.GET, value = "/resourceUrls")
    public ResponseEntity getResourceUrls(@RequestParam("id") String resourceId, @RequestHeader("X-Auth-Token") String token) {
        
        log.info("Requesting resource url for resource with id: " + resourceId);

        HashMap<String, String> response = new HashMap<String, String>();
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        if (token != null && !resourceId.equals("badCRAMrespose")) {
            if (!resourceId.equals("noCRAMurl"))
                response.put(resourceId, symbIoTeCoreUrl + "/Sensors('" + resourceId + "')");
        } else {
            return new ResponseEntity<>("", headers, HttpStatus.INTERNAL_SERVER_ERROR);
        }

        return new ResponseEntity<>(response, headers, HttpStatus.OK);
    }

    static public class DateUtil
    {
        public static Date addDays(Date date, int days)
        {
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            cal.add(Calendar.DATE, days); //minus number would decrement the days
            return cal.getTime();
        }
    }
}

