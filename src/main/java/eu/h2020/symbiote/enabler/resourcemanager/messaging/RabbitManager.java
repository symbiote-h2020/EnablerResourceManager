package eu.h2020.symbiote.enabler.resourcemanager.messaging;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import eu.h2020.symbiote.enabler.resourcemanager.messaging.consumers.*;
import eu.h2020.symbiote.enabler.resourcemanager.repository.TaskInfoRepository;
import eu.h2020.symbiote.enabler.resourcemanager.utils.ProblematicResourcesHandler;
import eu.h2020.symbiote.enabler.resourcemanager.utils.SearchHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
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
    @Value("${rabbit.routingKey.enablerPlatformProxy.cancelTasks}")
    private String platformProxyCancelTasksRoutingKey;
    @Value("${rabbit.routingKey.enablerPlatformProxy.acquisitionStartRequested}")
    private String platformProxyAcquisitionStartRequestedRoutingKey;
    @Value("${rabbit.routingKey.enablerPlatformProxy.taskUpdated}")
    private String platformProxyTaskUpdatedKey;

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
    private Map<String, Object> queueArgs;

    private AutowireCapableBeanFactory beanFactory;
    private TaskInfoRepository taskInfoRepository;
    private RabbitTemplate rabbitTemplate;
    private SearchHelper searchHelper;
    private ProblematicResourcesHandler problematicResourcesHandler;

    @Autowired
    public RabbitManager(AutowireCapableBeanFactory beanFactory,
                         TaskInfoRepository taskInfoRepository,
                         RabbitTemplate rabbitTemplate,
                         SearchHelper searchHelper,
                         ProblematicResourcesHandler problematicResourcesHandler,
                         @Value("${spring.rabbitmq.template.reply-timeout}") Long rabbitTimeout) {
        queueArgs = new HashMap<>();
        queueArgs.put("x-message-ttl", rabbitTimeout);

        this.beanFactory = beanFactory;
        this.taskInfoRepository = taskInfoRepository;
        this.rabbitTemplate = rabbitTemplate;
        this.searchHelper = searchHelper;
        this.problematicResourcesHandler = problematicResourcesHandler;
    }

    /**
     * Initiates connection with Rabbit server using parameters from ConfigProperties
     *
     * @throws IOException if it cannot create the ConnectionFactory
     */
    private void getConnection() throws IOException, TimeoutException {
        if (connection == null) {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(this.rabbitHost);
            factory.setUsername(this.rabbitUsername);
            factory.setPassword(this.rabbitPassword);
            this.connection = factory.newConnection();
        }
    }

    /**
     * Method creates channel and declares Rabbit exchanges.
     * It triggers start of all consumers used in Registry communication.
     */
    public void init() throws IOException, TimeoutException{
        Channel channel = null;
        log.info("Rabbit is being initialized!");

        try {
            getConnection();
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
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
    private void startConsumers() throws IOException {

        startConsumerOfStartDataAcquisition();
        startConsumerOfCancelTaskRequest();
        startConsumerOfPlatformProxyConnectionProblem();
        startConsumerOfEnablerLogicWrongData();
        startConsumerOfUpdateTask();

    }


    /**
     * Method creates queue and binds it globally available exchange and adequate Routing Key.
     * It also creates a consumer for messages incoming to this queue, regarding to StartDataAcquisition requests.
     *
     * @throws IOException if it cannot create the Channel
     */
    private void startConsumerOfStartDataAcquisition() throws IOException {
       
        String queueName = startDataAcquisitionQueueName;
        Channel channel;


        channel = this.connection.createChannel();
        channel.queueDeclare(queueName, true, false, false, queueArgs);
        channel.queueBind(queueName, this.resourceManagerExchangeName, this.startDataAcquisitionRoutingKey);
        // channel.basicQos(1); // to spread the load over multiple servers we set the prefetchCount setting

        log.info("Receiver waiting for StartDataAcquisition messages....");

        Consumer consumer = new StartDataAcquisitionConsumer(
                channel, taskInfoRepository, rabbitTemplate, searchHelper,
                platformProxyExchangeName, platformProxyAcquisitionStartRequestedRoutingKey);
        beanFactory.autowireBean(consumer);
        channel.basicConsume(queueName, false, consumer);

    }

    /**
     * Method creates queue and binds it globally available exchange and adequate Routing Key.
     * It also creates a consumer for messages incoming to this queue, regarding to CancelTaskRequests.
     *
     * @throws IOException if it cannot create the Channel
     */
    private void startConsumerOfCancelTaskRequest() throws IOException {

        String queueName = cancelTaskQueueName;
        Channel channel;

        channel = this.connection.createChannel();
        channel.queueDeclare(queueName, true, false, false, queueArgs);
        channel.queueBind(queueName, this.resourceManagerExchangeName, this.cancelTaskRoutingKey);
        // channel.basicQos(1); // to spread the load over multiple servers we set the prefetchCount setting

        log.info("Receiver waiting for CancelTaskRequests....");

        Consumer consumer = new CancelTaskConsumer(
                channel, taskInfoRepository, rabbitTemplate, searchHelper,
                platformProxyExchangeName, platformProxyCancelTasksRoutingKey );
        beanFactory.autowireBean(consumer);
        channel.basicConsume(queueName, false, consumer);

    }

    /**
     * Method creates queue and binds it globally available exchange and adequate Routing Key.
     * It also creates a consumer for messages incoming to this queue, regarding to Platform Proxy Connections problems.
     *
     * @throws IOException if it cannot create the Channel
     */
    private void startConsumerOfPlatformProxyConnectionProblem() throws IOException {

        String queueName = unavailableResourcesQueueName;
        Channel channel;

        channel = this.connection.createChannel();
        channel.queueDeclare(queueName, true, false, false, queueArgs);
        channel.queueBind(queueName, this.resourceManagerExchangeName, this.unavailableResourcesRoutingKey);
        // channel.basicQos(1); // to spread the load over multiple servers we set the prefetchCount setting

        log.info("Receiver waiting for UnavailableResources....");

        Consumer consumer = new PlatformProxyConnectionProblemConsumer(channel, taskInfoRepository, problematicResourcesHandler);
        beanFactory.autowireBean(consumer);
        channel.basicConsume(queueName, false, consumer);

    }

    /**
     * Method creates queue and binds it globally available exchange and adequate Routing Key.
     * It also creates a consumer for messages incoming to this queue, regarding to Enabler Logic Wrong Data messages.
     *
     * @throws IOException if it cannot create the Channel
     */
    private void startConsumerOfEnablerLogicWrongData() throws IOException {

        String queueName = wrongDataQueueName;
        Channel channel;

        channel = this.connection.createChannel();
        channel.queueDeclare(queueName, true, false, false, queueArgs);
        channel.queueBind(queueName, this.resourceManagerExchangeName, this.wrongDataRoutingKey);
        // channel.basicQos(1); // to spread the load over multiple servers we set the prefetchCount setting

        log.info("Receiver waiting for WrongData messages....");

        Consumer consumer = new EnablerLogicWrongDataConsumer(channel, taskInfoRepository, problematicResourcesHandler);
        beanFactory.autowireBean(consumer);
        channel.basicConsume(queueName, false, consumer);

    }

    /**
     * Method creates queue and binds it globally available exchange and adequate Routing Key.
     * It also creates a consumer for messages incoming to this queue, regarding to Enabler Logic Wrong Data messages.
     *
     * @throws IOException if it cannot create the Channel
     */
    private void startConsumerOfUpdateTask() throws IOException {

        String queueName = updateTaskQueueName;
        Channel channel;

        channel = this.connection.createChannel();
        channel.queueDeclare(queueName, true, false, false, queueArgs);
        channel.queueBind(queueName, this.resourceManagerExchangeName, this.updateTaskRoutingKey);
        // channel.basicQos(1); // to spread the load over multiple servers we set the prefetchCount setting

        log.info("Receiver waiting for UpdateTask messages....");

        Consumer consumer = new UpdateTaskConsumer(
                channel, taskInfoRepository, rabbitTemplate, searchHelper, platformProxyExchangeName,
                platformProxyAcquisitionStartRequestedRoutingKey, platformProxyTaskUpdatedKey, platformProxyCancelTasksRoutingKey);
        beanFactory.autowireBean(consumer);
        channel.basicConsume(queueName, false, consumer);

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
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }
}