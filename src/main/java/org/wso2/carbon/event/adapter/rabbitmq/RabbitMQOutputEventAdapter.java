package org.wso2.carbon.event.adapter.rabbitmq;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.context.PrivilegedCarbonContext;
import org.wso2.carbon.event.adapter.rabbitmq.internal.util.RabbitMQOutputEventAdapterConnectionConfiguration;
import org.wso2.carbon.event.adapter.rabbitmq.internal.util.RabbitMQOutputEventAdapterConstants;
import org.wso2.carbon.event.adapter.rabbitmq.internal.util.RabbitMQOutputEventAdapterPublisher;
import org.wso2.carbon.event.output.adapter.core.EventAdapterUtil;
import org.wso2.carbon.event.output.adapter.core.OutputEventAdapter;
import org.wso2.carbon.event.output.adapter.core.OutputEventAdapterConfiguration;
import org.wso2.carbon.event.output.adapter.core.exception.OutputEventAdapterException;
import org.wso2.carbon.event.output.adapter.core.exception.TestConnectionNotSupportedException;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Output MQTTEventAdapter will be used to publish events with MQTT protocol to specified broker and topic.
 */
public class RabbitMQOutputEventAdapter implements OutputEventAdapter {

    private OutputEventAdapterConfiguration eventAdapterConfiguration;
    private Map<String, String> globalProperties;
    private RabbitMQOutputEventAdapterPublisher rabbitmqAdapterPublisher;
    private static ThreadPoolExecutor threadPoolExecutor;
    private static final Log LOGGER = LogFactory.getLog(RabbitMQOutputEventAdapter.class);
    private int tenantId;

    public RabbitMQOutputEventAdapter(OutputEventAdapterConfiguration eventAdapterConfiguration, Map<String, String> globalProperties) {
        this.eventAdapterConfiguration = eventAdapterConfiguration;
        this.globalProperties = globalProperties;
    }

    /**
     * This method is called when initiating event publisher bundle.
     * Relevant code segments which are needed when loading OSGI bundle can be included in this method.
     *
     * @throws OutputEventAdapterException
     */
    @Override
    public void init() throws OutputEventAdapterException {

        LOGGER.debug("*** DEBUG RabbitMQOutputEventAdapter().init() start.");
        LOGGER.info("이닛 실행했다");

        //ThreadPoolExecutor will be assigned  if it is null
        if (threadPoolExecutor == null) {
            int minThread;
            int maxThread;
            int jobQueSize;
            long defaultKeepAliveTime;

            if (globalProperties.get(RabbitMQOutputEventAdapterConstants.RABBITMQ_TENANT_NAME) != null) {
                PrivilegedCarbonContext.getThreadLocalCarbonContext().setTenantId(Integer.parseInt(globalProperties.get(RabbitMQOutputEventAdapterConstants.RABBITMQ_TENANT_NAME)));
                tenantId = PrivilegedCarbonContext.getThreadLocalCarbonContext().getTenantId();
            } else {
                tenantId = PrivilegedCarbonContext.getThreadLocalCarbonContext().getTenantId();
            }

            //If global properties are available those will be assigned else constant values will be assigned
            if (globalProperties.get(RabbitMQOutputEventAdapterConstants.ADAPTER_MIN_THREAD_POOL_SIZE_NAME) != null) {
                minThread = Integer.parseInt(globalProperties.get(RabbitMQOutputEventAdapterConstants.ADAPTER_MIN_THREAD_POOL_SIZE_NAME));
            } else {
                minThread = RabbitMQOutputEventAdapterConstants.DEFAULT_MIN_THREAD_POOL_SIZE;
            }

            if (globalProperties.get(RabbitMQOutputEventAdapterConstants.ADAPTER_MAX_THREAD_POOL_SIZE_NAME) != null) {
                maxThread = Integer.parseInt(globalProperties.get(RabbitMQOutputEventAdapterConstants.ADAPTER_MAX_THREAD_POOL_SIZE_NAME));
            } else {
                maxThread = RabbitMQOutputEventAdapterConstants.DEFAULT_MAX_THREAD_POOL_SIZE;
            }

            if (globalProperties.get(RabbitMQOutputEventAdapterConstants.ADAPTER_KEEP_ALIVE_TIME_NAME) != null) {
                defaultKeepAliveTime = Integer.parseInt(globalProperties.get(
                        RabbitMQOutputEventAdapterConstants.ADAPTER_KEEP_ALIVE_TIME_NAME));
            } else {
                defaultKeepAliveTime = RabbitMQOutputEventAdapterConstants.DEFAULT_KEEP_ALIVE_TIME_IN_MILLIS;
            }

            if (globalProperties.get(RabbitMQOutputEventAdapterConstants.ADAPTER_EXECUTOR_JOB_QUEUE_SIZE_NAME) != null) {
                jobQueSize = Integer.parseInt(globalProperties.get(
                        RabbitMQOutputEventAdapterConstants.ADAPTER_EXECUTOR_JOB_QUEUE_SIZE_NAME));
            } else {
                jobQueSize = RabbitMQOutputEventAdapterConstants.DEFAULT_EXECUTOR_JOB_QUEUE_SIZE;
            }

            if(LOGGER.isDebugEnabled()) {
                LOGGER.debug("*** DEBUG ThreadPoolExecutor Init MinThread: " + minThread + ", MaxThread: " + maxThread);
            }

            threadPoolExecutor = new ThreadPoolExecutor(minThread, maxThread, defaultKeepAliveTime, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(jobQueSize));
        }
    }

    /**
     * This method is used to test the connection of the publishing server.
     *
     * @throws TestConnectionNotSupportedException
     */
    @Override
    public void testConnect() throws TestConnectionNotSupportedException {
        throw new TestConnectionNotSupportedException("Test connection is not available");
//        tenantId = Integer.parseInt(eventAdapterConfiguration.getStaticProperties().get(RabbitMQOutputEventAdapterConstants.RABBITMQ_TENANT_NAME));
//        RabbitMQOutputEventAdapterConnectionConfiguration rabbitMQOutputEventAdapterConnectionConfiguration
//                = new RabbitMQOutputEventAdapterConnectionConfiguration(eventAdapterConfiguration);
//
//        RabbitMQOutputEventAdapterPublisher rabbitMQOutputEventAdapterPublisher = new RabbitMQOutputEventAdapterPublisher(rabbitMQOutputEventAdapterConnectionConfiguration, eventAdapterConfiguration, tenantId);
//
//        Channel channel = rabbitMQOutputEventAdapterPublisher.getChannel();
//
//        try {
//            channel.basicQos(1);
//        } catch (IOException e) {
//            throw new RabbitMQException(e);
//        }
    }

    /**
     * Can be called to connect to back end before events are published.
     *
     */
    @Override
    public void connect() {

        LOGGER.debug("*** DEBUG RabbitMQOutputEventAdapter().connect() trying to connect.");

        RabbitMQOutputEventAdapterConnectionConfiguration configuration = new RabbitMQOutputEventAdapterConnectionConfiguration(eventAdapterConfiguration);

        rabbitmqAdapterPublisher = new RabbitMQOutputEventAdapterPublisher(configuration, eventAdapterConfiguration);
        LOGGER.info("커넥트함수 실행했다");
    }

    /**
     * Publish events. Throws ConnectionUnavailableException if it cannot connect to the back end.
     *
     * @param message
     * @param dynamicProperties
     */
    @Override
    public void publish(Object message, Map<String, String> dynamicProperties) {

        String exchange = dynamicProperties.get(RabbitMQOutputEventAdapterConstants.RABBITMQ_EXCHANGE_NAME);//default = "exchange_dlm"
        LOGGER.info("퍼블리쉬 실행했다");

        try {
            threadPoolExecutor.submit(new RabbitMQSender(exchange, message));
        } catch (RejectedExecutionException e) {
            EventAdapterUtil.logAndDrop(eventAdapterConfiguration.getName(), message, "Job queue is full", e, LOGGER, tenantId);
        }
    }

    /**
     * Will be called after publishing is done, or when ConnectionUnavailableException is thrown.
     */
    @Override
    public void disconnect() {

        LOGGER.info("디스커넥트 실행했다");

        try {
            if (rabbitmqAdapterPublisher != null) {
                rabbitmqAdapterPublisher.close();
                rabbitmqAdapterPublisher = null;
            }
        } catch (OutputEventAdapterException e) {
            LOGGER.error("Exception when closing the Rabbitmq publisher connection on Output Event Adapter '" + eventAdapterConfiguration.getName() + "'", e);
        }
    }

    /**
     * The method can be used to clean all the resources consumed.
     */
    @Override
    public void destroy() {
        //not required
    }

    /**
     * Checks whether events get accumulated at the adapter and clients connect to it to collect events.
     *
     * @return
     */
    @Override
    public boolean isPolled() {
        return false;
    }

    /**
     * Create to Rabbitmq Sender.
     *
     * @return
     */
    class RabbitMQSender implements Runnable {

        String exchange;
        Object message;

        RabbitMQSender(String exchange, Object message) {
            this.exchange = exchange;
            this.message = message;
            LOGGER.info("래빗엠큐센더 실행했다");
        }

        @Override
        public void run() {
            try {
                LOGGER.info("래빗엠큐센더의 런을 실행했다");
//                if (qos == null) {
                    rabbitmqAdapterPublisher.publish(message.toString());
//                } else {
//                    rabbitmqAdapterPublisher.publish(Integer.parseInt(qos), message.toString(), topic);
//                }
            } catch (Throwable t) {
                EventAdapterUtil.logAndDrop(eventAdapterConfiguration.getName(), message, null, t, LOGGER, tenantId);
            }
        }
    }
}