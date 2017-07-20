package eu.h2020.symbiote.enabler.resourcemanager.messaging;

import com.rabbitmq.client.*;
import eu.h2020.symbiote.enabler.resourcemanager.messaging.consumers.CancelTaskConsumer;
import eu.h2020.symbiote.enabler.resourcemanager.messaging.consumers.PlatformProxyConnectionProblemConsumer;
import eu.h2020.symbiote.enabler.resourcemanager.messaging.consumers.StartDataAcquisitionConsumer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Bean used to manage internal communication using RabbitMQ.
 * It is responsible for declaring exchanges and using routing keys from centralized config server.
 * <p>
 * Created by mateuszl
 */
@Component
public class RabbitManager {

    private static Log log = LogFactory.getLog(RabbitManager.class);

    @Value("${rabbit.host}")
    private String rabbitHost;
    @Value("${rabbit.username}")
    private String rabbitUsername;
    @Value("${rabbit.password}")
    private String rabbitPassword;

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
    @Value("${rabbit.routingKey.resourceManager.cancelTask}")
    private String cancelTaskRoutingKey;
    @Value("${rabbit.routingKey.resourceManager.unavailableResources}")
    private String unavailableResourcesRoutingKey;

    private Connection connection;

    @Autowired 
    private AutowireCapableBeanFactory beanFactory;

    public RabbitManager() {
    }

    /**
     * Initiates connection with Rabbit server using parameters from ConfigProperties
     *
     * @throws IOException
     * @throws TimeoutException
     */
    public Connection getConnection() throws IOException, TimeoutException {
        if (connection == null) {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(this.rabbitHost);
            factory.setUsername(this.rabbitUsername);
            factory.setPassword(this.rabbitPassword);
            this.connection = factory.newConnection();
        }
        return this.connection;
    }

    /**
     * Method creates channel and declares Rabbit exchanges.
     * It triggers start of all consumers used in Registry communication.
     */
    public void init() {
        Channel channel = null;
        log.info("Rabbit is being initialized!");

        try {
            getConnection();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }

        if (connection != null) {
            try {
                channel = this.connection.createChannel();

                channel.exchangeDeclare(this.resourceManagerExchangeName,
                        this.resourceManagerExchangeType,
                        this.resourceManagerExchangeDurable,
                        this.resourceManagerExchangeAutodelete,
                        this.resourceManagerExchangeInternal,
                        null);

                startConsumers();

            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                closeChannel(channel);
            }
        }
    }

    /**
     * Cleanup method for rabbit - set on pre destroy
     */
    @PreDestroy
    public void cleanup() {
        //FIXME check if there is better exception handling in @predestroy method
        log.info("Rabbit cleaned!");
        try {
            Channel channel;
            if (this.connection != null && this.connection.isOpen()) {
                channel = connection.createChannel();
                channel.queueUnbind("resourceManagerStartDataAcquisition", this.resourceManagerExchangeName, this.startDataAcquisitionRoutingKey);
                channel.queueDelete("resourceManagerStartDataAcquisition");
                channel.queueUnbind("resourceManagerCancelTaskRequest", this.resourceManagerExchangeName, this.cancelTaskRoutingKey);
                channel.queueDelete("resourceManagerCancelTaskRequest");
                channel.queueUnbind("resourceManagerUnavailableResources", this.resourceManagerExchangeName, this.unavailableResourcesRoutingKey);
                channel.queueDelete("resourceManagerUnavailableResources");

                closeChannel(channel);
                this.connection.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Method gathers all of the rabbit consumer starter methods
     */
    public void startConsumers() {
        try {
            startConsumerOfStartDataAcquisition();
            startConsumerOfCancelTaskRequest();
            startConsumerOfPlatformProxyConnectionProblem();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // public void sendPlaceholderMessage(String placeholder) { // arg should be object instead of String, e.g. Resource
    //     Gson gson = new Gson();
    //     String message = gson.toJson(placeholder);
    //     sendMessage(this.placeholderExchangeName, this.placeholderRoutingKey, message);
    //     log.info("- placeholder message sent");
    // }

    public void sendCustomMessage(String exchange, String routingKey, String objectInJson) {
        sendMessage(exchange, routingKey, objectInJson);
        log.info("- Custom message sent");
    }

    /**
     * Method creates queue and binds it globally available exchange and adequate Routing Key.
     * It also creates a consumer for messages incoming to this queue, regarding to StartDataAcquisition requests.
     *
     * @throws InterruptedException
     * @throws IOException
     */
    private void startConsumerOfStartDataAcquisition() throws InterruptedException, IOException {
       
        String queueName = "resourceManagerStartDataAcquisition";
        Channel channel;


        try {
            channel = this.connection.createChannel();
            channel.queueDeclare(queueName, true, false, false, null);
            channel.queueBind(queueName, this.resourceManagerExchangeName, this.startDataAcquisitionRoutingKey);
//            channel.basicQos(1); // to spread the load over multiple servers we set the prefetchCount setting

            log.info("Receiver waiting for StartDataAcquisition messages....");

            Consumer consumer = new StartDataAcquisitionConsumer(channel);
            beanFactory.autowireBean(consumer);
            channel.basicConsume(queueName, false, consumer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Method creates queue and binds it globally available exchange and adequate Routing Key.
     * It also creates a consumer for messages incoming to this queue, regarding to CancelTaskRequests.
     *
     * @throws InterruptedException
     * @throws IOException
     */
    private void startConsumerOfCancelTaskRequest() throws InterruptedException, IOException {

        String queueName = "resourceManagerCancelTaskRequest";
        Channel channel;


        try {
            channel = this.connection.createChannel();
            channel.queueDeclare(queueName, true, false, false, null);
            channel.queueBind(queueName, this.resourceManagerExchangeName, this.cancelTaskRoutingKey);
//            channel.basicQos(1); // to spread the load over multiple servers we set the prefetchCount setting

            log.info("Receiver waiting for CancelTaskRequests....");

            Consumer consumer = new CancelTaskConsumer(channel);
            beanFactory.autowireBean(consumer);
            channel.basicConsume(queueName, false, consumer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Method creates queue and binds it globally available exchange and adequate Routing Key.
     * It also creates a consumer for messages incoming to this queue, regarding to CancelTaskRequests.
     *
     * @throws InterruptedException
     * @throws IOException
     */
    private void startConsumerOfPlatformProxyConnectionProblem() throws InterruptedException, IOException {

        String queueName = "resourceManagerUnavailableResources";
        Channel channel;


        try {
            channel = this.connection.createChannel();
            channel.queueDeclare(queueName, true, false, false, null);
            channel.queueBind(queueName, this.resourceManagerExchangeName, this.unavailableResourcesRoutingKey);
//            channel.basicQos(1); // to spread the load over multiple servers we set the prefetchCount setting

            log.info("Receiver waiting for UnavailableResources....");

            Consumer consumer = new PlatformProxyConnectionProblemConsumer(channel);
            beanFactory.autowireBean(consumer);
            channel.basicConsume(queueName, false, consumer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Method publishes given message to the given exchange and routing key.
     * Props are set for correct message handle on the receiver side.
     *
     * @param exchange   name of the proper Rabbit exchange, adequate to topic of the communication
     * @param routingKey name of the proper Rabbit routing key, adequate to topic of the communication
     * @param message    message content in JSON String format
     */
    private void sendMessage(String exchange, String routingKey, String message) {
        AMQP.BasicProperties props;
        Channel channel = null;
        try {
            channel = this.connection.createChannel();
            props = new AMQP.BasicProperties()
                    .builder()
                    .contentType("application/json")
                    .build();

            channel.basicPublish(exchange, routingKey, props, message.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            closeChannel(channel);
        }
    }

    /**
     * Closes given channel if it exists and is open.
     *
     * @param channel rabbit channel to close
     */
    private void closeChannel(Channel channel) {
        try {
            if (channel != null && channel.isOpen())
                channel.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }
}