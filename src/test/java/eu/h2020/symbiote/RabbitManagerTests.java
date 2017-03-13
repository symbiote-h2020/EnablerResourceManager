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

import eu.h2020.symbiote.messaging.RabbitManager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.*;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes={EnablerResourceManagerApplication.class})
@SpringBootTest({"eureka.client.enabled=false"})
public class RabbitManagerTests {

    private static Logger log = LoggerFactory
                          .getLogger(RabbitManagerTests.class);

    @Autowired
    private RabbitManager rabbitManager;

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
    public void testResourceManagerGetResourceDetails() {

        rabbitManager.sendCustomMessage(resourceManagerExchangeName, getResourceDetailsRoutingKey, "{test : 'test'}");

    }

}
