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
import org.springframework.test.web.client.MockRestServiceServer;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.json.simple.JSONObject;

import eu.h2020.symbiote.messaging.RabbitManager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes={EnablerResourceManagerApplication.class})
@SpringBootTest({"eureka.client.enabled=false"})
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
    @Value("${rabbit.routingKey.resourceManager.getResourceDetails}")
    private String getResourceDetailsRoutingKey;

    private MockRestServiceServer mockServer;


	// Execute the Setup method before the test.
	@Before
	public void setUp() throws Exception {
        mockServer = MockRestServiceServer.createServer(asyncRestTemplate);
        
    }

    @Test
    public void testResourceManagerGetResourceDetails() throws Exception {

        String url = symbIoTeCoreUrl;
        String message = "search_resources";
        JSONObject query = new JSONObject();
        final AtomicReference<JSONObject> resultRef = new AtomicReference<JSONObject>();
        query.put("test", "test");

        mockServer.expect(requestTo(url)).andExpect(method(HttpMethod.POST))
                .andRespond(request -> {
                    try {
                        Thread.sleep(TimeUnit.SECONDS.toMillis(2)); // Delay
                    } catch (InterruptedException ignored) {}

                    // JSONParser parser = new JSONParser();
                    JSONObject response = new JSONObject();

                    // try {
                    //     response = (JSONObject) parser.parse(request.getBody().toString());

                    // } catch (Exception ignored) {}

                    response.put("status", "ok");
                    log.info(message + "_test: Server received " + request.getBody().toString());
                    log.info(message + "_test: Server woke up and will answer with " + response);

                    return withStatus(HttpStatus.OK).body(response.toString()).contentType(MediaType.APPLICATION_JSON).createResponse(request);
        });

        log.info("Before sending the message");

        RabbitConverterFuture<JSONObject> future = asyncRabbitTemplate.convertSendAndReceive(resourceManagerExchangeName, getResourceDetailsRoutingKey, query);

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
      
        
        assertEquals("ok", resultRef.get().get("status"));

    }

}
