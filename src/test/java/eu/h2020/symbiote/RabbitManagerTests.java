package eu.h2020.symbiote;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.Before;

import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.HttpMethod;

import org.springframework.amqp.rabbit.AsyncRabbitTemplate;
import org.springframework.amqp.rabbit.AsyncRabbitTemplate.RabbitConverterFuture;

import org.springframework.web.client.AsyncRestTemplate;
import org.springframework.web.client.RestTemplate;
import org.springframework.test.web.client.MockRestServiceServer;
import static org.springframework.test.web.client.ExpectedCount.*;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import eu.h2020.symbiote.messaging.RabbitManager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes={EnablerResourceManagerApplication.class})
@SpringBootTest({"eureka.client.enabled=false", "spring.sleuth.enabled=false"})
public class RabbitManagerTests {

    private static Logger log = LoggerFactory
                          .getLogger(RabbitManagerTests.class);

    @Autowired
    private RabbitManager rabbitManager;

    @Autowired    
    private AsyncRabbitTemplate asyncRabbitTemplate;

    @Autowired    
    AsyncRestTemplate asyncRestTemplate;

    @Autowired    
    RestTemplate restTemplate;

    @Autowired
    @Qualifier("symbIoTeCoreUrl")
    private String symbIoTeCoreUrl;

    @Value("${rabbit.exchange.resourceManager.name}")
    private String resourceManagerExchangeName;
    @Value("${rabbit.exchange.resourceManager.type}")
    private String resourceManagerExchangeType;
    @Value("${rabbit.exchange.resourceManager.durable}")
    private boolean resourceManagerExchangeDurable;
    @Value("${rabbit.exchange.resourceManager.autodelete}")
    private boolean resourceManagerExchangeAutodelete;
    @Value("${rabbit.exchange.resourceManager.internal}")
    private boolean resourceManagerExchangeInternal;
    @Value("${rabbit.routingKey.resourceManager.startDataAcquisition}")
    private String startDataAcquisitionRoutingKey;

    private MockRestServiceServer mockServer;


	// Execute the Setup method before the test.
	@Before
	public void setUp() throws Exception {
        mockServer = MockRestServiceServer.createServer(restTemplate);
        
    }

    @Test
    public void testResourceManagerGetResourceDetails() throws Exception {

        String url;
        String message = "search_resources";
        final AtomicReference<JSONObject> resultRef = new AtomicReference<JSONObject>();
        JSONObject query = new JSONObject();
        JSONArray resources = new JSONArray();

        JSONObject object1 = new JSONObject();
        JSONArray observesProperty1 = new JSONArray();
        object1.put("taskID", "1");
        object1.put("count", "2");
        object1.put("location", "Paris");
        observesProperty1.add("temperature");
        observesProperty1.add("humidity");
        object1.put("observesProperty", observesProperty1);
        object1.put("queryInterval", "60");
        resources.add(object1);

        JSONObject object2 = new JSONObject();
        JSONArray observesProperty2 = new JSONArray();
        object2.put("taskID", "2");
        object2.put("count", "1");
        object2.put("location", "Athens");
        observesProperty2.add("air quality");
        object2.put("observesProperty", observesProperty2);
        object2.put("queryInterval", "60");
        resources.add(object2);

        query.put("resources", resources);
        
        url = "http://www.example.com/v1/query?location=Paris&observed_property=temperature,humidity";
        mockServer.expect(requestTo(url)).andExpect(method(HttpMethod.GET))
                .andRespond(request -> {
                    try {
                        Thread.sleep(TimeUnit.SECONDS.toMillis(2)); // Delay
                    } catch (InterruptedException ignored) {}

                    // JSONParser parser = new JSONParser();
                    JSONArray response = new JSONArray();

                    JSONObject resource1 = new JSONObject();
                    resource1.put("id", "1");
                    resource1.put("interworkingServiceURL", "http://www.example/1");
                    response.add(resource1);

                    JSONObject resource2 = new JSONObject();
                    resource2.put("id", "2");
                    resource2.put("interworkingServiceURL", "http://www.example/2");
                    response.add(resource2);

                    JSONObject resource3 = new JSONObject();
                    resource3.put("id", "3");
                    resource3.put("interworkingServiceURL", "http://www.example/3");
                    response.add(resource3);

                    // try {
                    //     response = (JSONObject) parser.parse(request.getBody().toString());

                    // } catch (Exception ignored) {}

                    // response.put("status", "ok");
                    log.info(message + "_test: Server received " + request.getBody().toString());
                    log.info(message + "_test: Server woke up and will answer with " + response);

                    return withStatus(HttpStatus.OK).body(response.toString()).contentType(MediaType.APPLICATION_JSON).createResponse(request);
        });

        url = "http://www.example.com/v1/query?location=Athens&observed_property=air%20quality";
        mockServer.expect(requestTo(url)).andExpect(method(HttpMethod.GET))
                .andRespond(request -> {
                    try {
                        Thread.sleep(TimeUnit.SECONDS.toMillis(2)); // Delay
                    } catch (InterruptedException ignored) {}

                    // JSONParser parser = new JSONParser();
                    JSONArray response = new JSONArray();

                    JSONObject resource4 = new JSONObject();
                    resource4.put("id", "4");
                    resource4.put("interworkingServiceURL", "http://www.example/4");
                    response.add(resource4);

                    JSONObject resource5 = new JSONObject();
                    resource5.put("id", "5");
                    resource5.put("interworkingServiceURL", "http://www.example/5");
                    response.add(resource5);

                    // try {
                    //     response = (JSONObject) parser.parse(request.getBody().toString());

                    // } catch (Exception ignored) {}

                    // response.put("status", "ok");
                    log.info(message + "_test: Server received " + request.getBody().toString());
                    log.info(message + "_test: Server woke up and will answer with " + response);

                    return withStatus(HttpStatus.OK).body(response.toString()).contentType(MediaType.APPLICATION_JSON).createResponse(request);
        });

        log.info("Before sending the message");

        RabbitConverterFuture<JSONObject> future = asyncRabbitTemplate.convertSendAndReceive(resourceManagerExchangeName, startDataAcquisitionRoutingKey, query);

        log.info("After sending the message");

        future.addCallback(new ListenableFutureCallback<JSONObject>() {

            @Override
            public void onSuccess(JSONObject result) {
                log.info("Successully received response: " + result);
                resultRef.set(result);

            }

            @Override
            public void onFailure(Throwable ex) {
                fail("Accessed the element which does not exist");
            }

        });

        while(!future.isDone()) {
        	log.info("Sleeping!!!!!!");
            TimeUnit.SECONDS.sleep(1);
        }
      
        JSONParser parser = new JSONParser();
        try {

            Object obj = parser.parse(resultRef.get().toString());
            JSONObject response = (JSONObject) obj;
            JSONArray responseArray = (JSONArray) response.get("resources");
            JSONObject firstRequest = (JSONObject) responseArray.get(0);
            JSONObject secondRequest = (JSONObject) responseArray.get(1);


            assertEquals(2, ((JSONArray) firstRequest.get("resourceIds")).size());
            assertEquals(1, ((JSONArray) secondRequest.get("resourceIds")).size());

            assertEquals("1", (String) ((JSONArray) firstRequest.get("resourceIds")).get(0));
            assertEquals("2", (String) ((JSONArray) firstRequest.get("resourceIds")).get(1));
            assertEquals("4", (String) ((JSONArray) secondRequest.get("resourceIds")).get(0));


        } 
        catch (ParseException e) {}

    }

}
