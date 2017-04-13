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
import java.util.ArrayList;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.core.ExchangeTypes;

import eu.h2020.symbiote.messaging.RabbitManager;
import eu.h2020.symbiote.model.*;

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
        final AtomicReference<EnablerLogicAcquisitionStartResponse> resultRef = new AtomicReference<EnablerLogicAcquisitionStartResponse>();
        EnablerLogicAcquisitionStartRequest query = new EnablerLogicAcquisitionStartRequest();
        ArrayList<EnablerLogicTaskInfoRequest> resources = new ArrayList<EnablerLogicTaskInfoRequest>();


        EnablerLogicTaskInfoRequest request1 = new EnablerLogicTaskInfoRequest();
        ArrayList<String> observesProperty1 = new ArrayList<String>();
        request1.setTaskId("1");
        request1.setCount(2);
        request1.setLocation("Paris");
        observesProperty1.add("temperature");
        observesProperty1.add("humidity");
        request1.setObservesProperty(observesProperty1);
        request1.setInterval(60);
        resources.add(request1);

        EnablerLogicTaskInfoRequest request2 = new EnablerLogicTaskInfoRequest();
        ArrayList<String> observesProperty2 = new ArrayList<String>();
        request2.setTaskId("2");
        request2.setCount(1);
        request2.setLocation("Athens");
        observesProperty2.add("air quality");
        request2.setObservesProperty(observesProperty2);
        request2.setInterval(60);
        resources.add(request2);

        query.setResources(resources);
        
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

        RabbitConverterFuture<EnablerLogicAcquisitionStartResponse> future = asyncRabbitTemplate.convertSendAndReceive(resourceManagerExchangeName, startDataAcquisitionRoutingKey, query);

        log.info("After sending the message");

        future.addCallback(new ListenableFutureCallback<EnablerLogicAcquisitionStartResponse>() {

            @Override
            public void onSuccess(EnablerLogicAcquisitionStartResponse result) {
                log.info("Successfully received response: " + result);
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
      

        assertEquals(2, resultRef.get().getResources().get(0).getResourceIds().size());
        assertEquals(1, resultRef.get().getResources().get(1).getResourceIds().size());

        assertEquals("1", resultRef.get().getResources().get(0).getResourceIds().get(0));
        assertEquals("2", resultRef.get().getResources().get(0).getResourceIds().get(1));
        assertEquals("4", resultRef.get().getResources().get(1).getResourceIds().get(0));

    }

    @RabbitListener(bindings = @QueueBinding(
        value = @Queue(value = "symbIoTe-rap-writeResource", durable = "false", autoDelete = "true", exclusive = "true"),
        exchange = @Exchange(value = "symbIoTe.enablerPlatformProxy", ignoreDeclarationExceptions = "true", type = ExchangeTypes.TOPIC),
        key = "symbIoTe.enablerPlatformProxy.acquisitionStartRequested")
    )
    public void platformProxyListener(PlatformProxyAcquisitionStartRequest request) {
       
        log.info("Received message (taskID, ResourceIds.size(), interval): (" + 
                  request.getTaskId() + ", " + request.getResources().size() + ", " +
                  request.getInterval() + ")");
        // return "YES";
    }

}
