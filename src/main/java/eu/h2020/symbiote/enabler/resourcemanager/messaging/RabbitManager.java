package eu.h2020.symbiote.enabler.resourcemanager.messaging;

import com.rabbitmq.client.*;

import eu.h2020.symbiote.enabler.resourcemanager.messaging.consumers.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.stereotype.Component;

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

    @Value("${rabbit.exchange.enablerLogic.name}")
    private String enablerLogicExchangeName;
    @Value("${rabbit.exchange.enablerLogic.type}")
    private String enablerLogicExchangeType;
    @Value("${rabbit.exchange.enablerLogic.durable}")
    private boolean enablerLogicExchangeDurable;
    @Value("${rabbit.exchange.enablerLogic.autodelete}")
    private boolean enablerLogicExchangeAutodelete;
    @Value("${rabbit.exchange.enablerLogic.internal}")
    private boolean enablerLogicExchangeInternal;

    @Value("${rabbit.exchange.enablerPlatformProxy.name}")
    private String platformProxyExchangeName;
    @Value("${rabbit.exchange.enablerPlatformProxy.type}")
    private String platformProxyExchangeType;
    @Value("${rabbit.exchange.enablerPlatformProxy.durable}")
    private boolean platformProxyExchangeDurable;
    @Value("${rabbit.exchange.enablerPlatformProxy.autodelete}")
    private boolean platformProxyExchangeAutodelete;
    @Value("${rabbit.exchange.enablerPlatformProxy.internal}")
    private boolean platformProxyExchangeInternal;
    
    @Value("${rabbit.queueName.resourceManager.startDataAcquisition}")
    private String startDataAcquisitionQueueName;
    @Value("${rabbit.routingKey.resourceManager.startDataAcquisition}")
    private String startDataAcquisitionRoutingKey;
    
    @Value("${rabbit.queueName.resourceManager.cancelTask}")
    private String cancelTaskQueueName;
    @Value("${rabbit.routingKey.resourceManager.cancelTask}")
    private String cancelTaskRoutingKey;
    
    @Value("${rabbit.queueName.resourceManager.unavailableResources}")
    private String unavailableResourcesQueueName;
    @Value("${rabbit.routingKey.resourceManager.unavailableResources}")
    private String unavailableResourcesRoutingKey;
    
    @Value("${rabbit.queueName.resourceManager.wrongData}")
    private String wrongDataQueueName;
    @Value("${rabbit.routingKey.resourceManager.wrongData}")
    private String wrongDataRoutingKey;

    @Value("${rabbit.queueName.resourceManager.updateTask}")
    private String updateTaskQueueName;
    @Value("${rabbit.routingKey.resourceManager.updateTask}")
    private String updateTaskRoutingKey;

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

                channel.exchangeDeclare(this.enablerLogicExchangeName,
                        this.enablerLogicExchangeType,
                        this.enablerLogicExchangeDurable,
                        this.enablerLogicExchangeAutodelete,
                        this.enablerLogicExchangeInternal,
                        null);

                channel.exchangeDeclare(this.platformProxyExchangeName,
                        this.platformProxyExchangeType,
                        this.platformProxyExchangeDurable,
                        this.platformProxyExchangeAutodelete,
                        this.platformProxyExchangeInternal,
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

                channel.queueUnbind(this.startDataAcquisitionQueueName, this.resourceManagerExchangeName, this.startDataAcquisitionRoutingKey);
                channel.queueDelete(this.startDataAcquisitionQueueName);

                channel.queueUnbind(this.cancelTaskQueueName, this.resourceManagerExchangeName, this.cancelTaskRoutingKey);
                channel.queueDelete(this.cancelTaskQueueName);

                channel.queueUnbind(this.unavailableResourcesQueueName, this.resourceManagerExchangeName, this.unavailableResourcesRoutingKey);
                channel.queueDelete(this.unavailableResourcesQueueName);

                channel.queueUnbind(this.wrongDataQueueName, this.resourceManagerExchangeName, this.updateTaskRoutingKey);
                channel.queueDelete(this.wrongDataQueueName);

                channel.queueUnbind(this.updateTaskQueueName, this.resourceManagerExchangeName, this.updateTaskRoutingKey);
                channel.queueDelete(this.updateTaskQueueName);

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
            startConsumerOfEnablerLogicWrongData();
            startConsumerOfUpdateTask();
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
       
        String queueName = startDataAcquisitionQueueName;
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

        String queueName = cancelTaskQueueName;
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
     * It also creates a consumer for messages incoming to this queue, regarding to Platform Proxy Connections problems.
     *
     * @throws InterruptedException
     * @throws IOException
     */
    private void startConsumerOfPlatformProxyConnectionProblem() throws InterruptedException, IOException {

        String queueName = unavailableResourcesQueueName;
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
     * Method creates queue and binds it globally available exchange and adequate Routing Key.
     * It also creates a consumer for messages incoming to this queue, regarding to Enabler Logic Wrong Data messages.
     *
     * @throws InterruptedException
     * @throws IOException
     */
    private void startConsumerOfEnablerLogicWrongData() throws InterruptedException, IOException {

        String queueName = wrongDataQueueName;
        Channel channel;

        try {
            channel = this.connection.createChannel();
            channel.queueDeclare(queueName, true, false, false, null);
            channel.queueBind(queueName, this.resourceManagerExchangeName, this.wrongDataRoutingKey);
//            channel.basicQos(1); // to spread the load over multiple servers we set the prefetchCount setting

            log.info("Receiver waiting for WrongData messages....");

            Consumer consumer = new EnablerLogicWrongDataConsumer(channel);
            beanFactory.autowireBean(consumer);
            channel.basicConsume(queueName, false, consumer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Method creates queue and binds it globally available exchange and adequate Routing Key.
     * It also creates a consumer for messages incoming to this queue, regarding to Enabler Logic Wrong Data messages.
     *
     * @throws InterruptedException
     * @throws IOException
     */
    private void startConsumerOfUpdateTask() throws InterruptedException, IOException {

        String queueName = updateTaskQueueName;
        Channel channel;

        try {
            channel = this.connection.createChannel();
            channel.queueDeclare(queueName, true, false, false, null);
            channel.queueBind(queueName, this.resourceManagerExchangeName, this.updateTaskRoutingKey);
//            channel.basicQos(1); // to spread the load over multiple servers we set the prefetchCount setting

            log.info("Receiver waiting for UpdateTask messages....");

            Consumer consumer = new UpdateTaskConsumer(channel);
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