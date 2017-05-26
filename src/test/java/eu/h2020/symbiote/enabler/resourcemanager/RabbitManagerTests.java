package eu.h2020.symbiote.enabler.resourcemanager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.Before;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
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
// import org.springframework.test.web.client.MockRestServiceServer;
// import static org.springframework.test.web.client.ExpectedCount.*;
// import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
// import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
// import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.ArrayList;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.core.ExchangeTypes;

import eu.h2020.symbiote.enabler.resourcemanager.messaging.RabbitManager;
import eu.h2020.symbiote.enabler.messaging.model.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;


@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(webEnvironment = WebEnvironment.DEFINED_PORT, 
                properties = {"eureka.client.enabled=false", 
                              "spring.sleuth.enabled=false",
                              "symbiote.core.url=http://localhost:8080",
                              "symbiote.coreaam.url=http://localhost:8080"}
                              )
@ContextConfiguration
@Configuration
@ComponentScan
@EnableAutoConfiguration
public class RabbitManagerTests {

    private static Logger log = LoggerFactory
                          .getLogger(RabbitManagerTests.class);

    @Autowired
    private RabbitManager rabbitManager;

    @Autowired    
    private AsyncRabbitTemplate asyncRabbitTemplate;

    @Autowired    
    private AsyncRestTemplate asyncRestTemplate;

    @Autowired    
    private RestTemplate restTemplate;

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

    // private MockRestServiceServer mockServer;
    private ObjectMapper mapper = new ObjectMapper();

	// Execute the Setup method before the test.
	@Before
	public void setUp() throws Exception {
        // mockServer = MockRestServiceServer.createServer(restTemplate);
        
    }

    @Test
    public void testResourceManagerGetResourceDetails() throws Exception {
        
        String url;
        String message = "search_resources";
        final AtomicReference<ResourceManagerAcquisitionStartResponse> resultRef = new AtomicReference<ResourceManagerAcquisitionStartResponse>();
        ResourceManagerAcquisitionStartRequest query = new ResourceManagerAcquisitionStartRequest();
        ArrayList<ResourceManagerTaskInfoRequest> resources = new ArrayList<ResourceManagerTaskInfoRequest>();


        ResourceManagerTaskInfoRequest request1 = new ResourceManagerTaskInfoRequest();
        ArrayList<String> observesProperty1 = new ArrayList<String>();
        request1.setTaskId("1");
        request1.setCount(2);
        request1.setLocation("Paris");
        observesProperty1.add("temperature");
        observesProperty1.add("humidity");
        request1.setObservesProperty(observesProperty1);
        request1.setInterval(60);
        resources.add(request1);

        ResourceManagerTaskInfoRequest request2 = new ResourceManagerTaskInfoRequest();
        ArrayList<String> observesProperty2 = new ArrayList<String>();
        request2.setTaskId("2");
        request2.setCount(1);
        request2.setLocation("Athens");
        observesProperty2.add("air quality");
        request2.setObservesProperty(observesProperty2);
        request2.setInterval(60);
        resources.add(request2);

        query.setResources(resources);
        
        // url = symbIoTeCoreUrl + "/query?location=Paris&observed_property=temperature,humidity";
        // mockServer.expect(requestTo(url)).andExpect(method(HttpMethod.GET))
        //         .andRespond(request -> {
        //             try {
        //                 Thread.sleep(TimeUnit.SECONDS.toMillis(2)); // Delay
        //             } catch (InterruptedException ignored) {}

        //             QueryResponse response = new QueryResponse();
        //             ArrayList<QueryResourceResult> responseResources = new ArrayList<QueryResourceResult>();

        //             QueryResourceResult resource1 = new QueryResourceResult();
        //             resource1.setId("resource1");
        //             resource1.setPlatformId("platform1");
        //             responseResources.add(resource1);

        //             QueryResourceResult resource2 = new QueryResourceResult();
        //             resource2.setId("resource2");
        //             resource2.setPlatformId("platform2");
        //             responseResources.add(resource2);

        //             QueryResourceResult resource3 = new QueryResourceResult();
        //             resource3.setId("resource3");
        //             resource3.setPlatformId("platform3");
        //             responseResources.add(resource3);

        //             response.setResources(responseResources);
        //             String responseInString = mapper.writeValueAsString(response);
                    
        //             // try {
        //             //     response = (JSONObject) parser.parse(request.getBody().toString());

        //             // } catch (Exception ignored) {}

        //             // response.put("status", "ok");
        //             log.info(message + "_test: Server received " + request.getBody().toString());
        //             log.info(message + "_test: Server woke up and will answer with " + responseInString);

        //             return withStatus(HttpStatus.OK).body(responseInString).contentType(MediaType.APPLICATION_JSON).createResponse(request);
        // });

        // url = symbIoTeCoreUrl + "/query?location=Athens&observed_property=air%20quality";
        // mockServer.expect(requestTo(url)).andExpect(method(HttpMethod.GET))
        //         .andRespond(request -> {
        //             try {
        //                 Thread.sleep(TimeUnit.SECONDS.toMillis(2)); // Delay
        //             } catch (InterruptedException ignored) {}

        //             QueryResponse response = new QueryResponse();
        //             ArrayList<QueryResourceResult> responseResources = new ArrayList<QueryResourceResult>();

        //             QueryResourceResult resource4 = new QueryResourceResult();
        //             resource4.setId("resource4");
        //             resource4.setPlatformId("platform4");
        //             responseResources.add(resource4);

        //             QueryResourceResult resource5 = new QueryResourceResult();
        //             resource5.setId("resource5");
        //             resource5.setPlatformId("platform5");
        //             responseResources.add(resource5);

        //             response.setResources(responseResources);
        //             String responseInString = mapper.writeValueAsString(response);

        //             // try {
        //             //     response = (JSONObject) parser.parse(request.getBody().toString());

        //             // } catch (Exception ignored) {}

        //             // response.put("status", "ok");
        //             log.info(message + "_test: Server received " + request.getBody().toString());
        //             log.info(message + "_test: Server woke up and will answer with " + responseInString);

        //             return withStatus(HttpStatus.OK).body(responseInString).contentType(MediaType.APPLICATION_JSON).createResponse(request);
        // });

        log.info("Before sending the message");

        RabbitConverterFuture<ResourceManagerAcquisitionStartResponse> future = asyncRabbitTemplate.convertSendAndReceive(resourceManagerExchangeName, startDataAcquisitionRoutingKey, query);

        log.info("After sending the message");

        future.addCallback(new ListenableFutureCallback<ResourceManagerAcquisitionStartResponse>() {

            @Override
            public void onSuccess(ResourceManagerAcquisitionStartResponse result) {
                try {
                    log.info("Successfully received response: " + mapper.writeValueAsString(result));
                } catch (JsonProcessingException e) {
                    log.info(e.toString());
                }
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
        
        String responseInString = mapper.writeValueAsString(resultRef.get().getResources());
        log.info("Response String: " + responseInString);

        assertEquals(2, resultRef.get().getResources().get(0).getResourceIds().size());
        assertEquals(1, resultRef.get().getResources().get(1).getResourceIds().size());

        assertEquals("resource1", resultRef.get().getResources().get(0).getResourceIds().get(0));
        assertEquals("resource2", resultRef.get().getResources().get(0).getResourceIds().get(1));
        assertEquals("resource4", resultRef.get().getResources().get(1).getResourceIds().get(0));

    }

    // @Test
    public void testResourceManagerGetResourceDetailsNoResponse() throws Exception {
        
        String url;
        String message = "search_resources";
        final AtomicReference<ResourceManagerAcquisitionStartResponse> resultRef = new AtomicReference<ResourceManagerAcquisitionStartResponse>();
        ResourceManagerAcquisitionStartRequest query = new ResourceManagerAcquisitionStartRequest();
        ArrayList<ResourceManagerTaskInfoRequest> resources = new ArrayList<ResourceManagerTaskInfoRequest>();


        ResourceManagerTaskInfoRequest request1 = new ResourceManagerTaskInfoRequest();
        ArrayList<String> observesProperty1 = new ArrayList<String>();
        request1.setTaskId("1");
        request1.setCount(2);
        request1.setLocation("Paris");
        observesProperty1.add("temperature");
        observesProperty1.add("humidity");
        request1.setObservesProperty(observesProperty1);
        request1.setInterval(60);
        resources.add(request1);

        query.setResources(resources);

        log.info("Before sending the message");

        RabbitConverterFuture<ResourceManagerAcquisitionStartResponse> future = asyncRabbitTemplate.convertSendAndReceive(resourceManagerExchangeName, startDataAcquisitionRoutingKey, query);

        log.info("After sending the message");

        future.addCallback(new ListenableFutureCallback<ResourceManagerAcquisitionStartResponse>() {

            @Override
            public void onSuccess(ResourceManagerAcquisitionStartResponse result) {
                try {
                    log.info("Successfully received response: " + mapper.writeValueAsString(result));
                } catch (JsonProcessingException e) {
                    log.info(e.toString());
                }
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
      
    }

    @Test
    public void testResourceManagerGetResourceDetailsBadRequest() throws Exception {
        
        String url;
        String message = "search_resources";
        final AtomicReference<ResourceManagerAcquisitionStartResponse> resultRef = new AtomicReference<ResourceManagerAcquisitionStartResponse>();
        ResourceManagerAcquisitionStartRequest query = new ResourceManagerAcquisitionStartRequest();
        ArrayList<ResourceManagerTaskInfoRequest> resources = new ArrayList<ResourceManagerTaskInfoRequest>();


        ResourceManagerTaskInfoRequest request1 = new ResourceManagerTaskInfoRequest();
        ArrayList<String> observesProperty1 = new ArrayList<String>();
        request1.setTaskId("1");
        request1.setCount(2);
        request1.setLocation("Zurich");
        observesProperty1.add("temperature");
        observesProperty1.add("humidity");
        request1.setObservesProperty(observesProperty1);
        request1.setInterval(60);
        resources.add(request1);

        query.setResources(resources);

        // url = symbIoTeCoreUrl + "/query?location=Zurich&observed_property=temperature,humidity";
        // mockServer.expect(requestTo(url)).andExpect(method(HttpMethod.GET))
        //         .andRespond(request -> {
        //             try {
        //                 Thread.sleep(TimeUnit.SECONDS.toMillis(2)); // Delay
        //             } catch (InterruptedException ignored) {}

        //             log.info(message + "_test: Server received " + request.getBody().toString());
        //             log.info(message + "_test: Server woke up and will answer with BAD_REQUEST");

        //             return withStatus(HttpStatus.BAD_REQUEST).contentType(MediaType.APPLICATION_JSON).createResponse(request);
        // });

        log.info("Before sending the message");

        RabbitConverterFuture<ResourceManagerAcquisitionStartResponse> future = asyncRabbitTemplate.convertSendAndReceive(resourceManagerExchangeName, startDataAcquisitionRoutingKey, query);

        log.info("After sending the message");

        future.addCallback(new ListenableFutureCallback<ResourceManagerAcquisitionStartResponse>() {

            @Override
            public void onSuccess(ResourceManagerAcquisitionStartResponse result) {
                try {
                    log.info("Successfully received response: " + mapper.writeValueAsString(result));
                } catch (JsonProcessingException e) {
                    log.info(e.toString());
                }
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

        assertEquals(null, resultRef.get().getResources().get(0).getResourceIds());

    }


    @RabbitListener(bindings = @QueueBinding(
        value = @Queue(value = "symbIoTe-rap-writeResource", durable = "false", autoDelete = "true", exclusive = "true"),
        exchange = @Exchange(value = "symbIoTe.enablerPlatformProxy", ignoreDeclarationExceptions = "true", type = ExchangeTypes.TOPIC),
        key = "symbIoTe.enablerPlatformProxy.acquisitionStartRequested")
    )
    public void platformProxyListener(PlatformProxyAcquisitionStartRequest request) {
        
        try {
            String responseInString = mapper.writeValueAsString(request);
            log.info("PlatformProxyListener received request: " + responseInString);
        } catch (JsonProcessingException e) {
            log.info(e.toString());
        }
    }
}
