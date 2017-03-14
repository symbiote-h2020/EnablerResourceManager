package eu.h2020.symbiote;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.boot.test.context.SpringBootTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.amqp.rabbit.AsyncRabbitTemplate;
import org.springframework.amqp.rabbit.AsyncRabbitTemplate.RabbitConverterFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;


import eu.h2020.symbiote.messaging.RabbitManager;
import org.json.simple.JSONObject;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.AMQP;
import java.util.UUID;


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


    @Test
    public void testResourceManagerGetResourceDetails() throws Exception {

        JSONObject query = new JSONObject();
        final AtomicReference<JSONObject> resultRef = new AtomicReference<JSONObject>();
        query.put("test", "test");

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
        // String corrId = UUID.randomUUID().toString();
        // String message = "I am Bill";

        // Channel channel = null;
        // try {
        //     channel = rabbitManager.connection.createChannel();
        //     AMQP.BasicProperties props = new AMQP.BasicProperties
        //             .Builder()
        //             .correlationId(corrId)
        //             .replyTo("amq.rabbitmq.reply-to")
        //             .build();

        //     channel.basicPublish(resourceManagerExchangeName, getResourceDetailsRoutingKey, props, message.getBytes("UTF-8"));

        //     final BlockingQueue<String> response = new ArrayBlockingQueue<String>(1); 

        //     channel.basicConsume("amq.rabbitmq.reply-to", true, new DefaultConsumer(channel) {
        //         @Override
        //         public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        //             log.info("Received message!!! ");

        //             if (properties.getCorrelationId().equals(corrId)) {
        //                 response.offer(new String(body, "UTF-8"));
        //             }
        //         }
        //     });

        //     log.info("After sleep! " + response.take());

        // } catch (Exception e) {
        //     e.printStackTrace();
        // }


        

        
        assertEquals("ok", resultRef.get().get("status"));

    }

}
