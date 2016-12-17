/*
*  Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.wso2.carbon.event.adapter.rabbitmq.output;

import com.rabbitmq.client.Channel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.context.PrivilegedCarbonContext;
import org.wso2.carbon.event.adapter.rabbitmq.internal.exception.RabbitAdapterMQException;
import org.wso2.carbon.event.adapter.rabbitmq.internal.util.RabbitMQOutputEventAdapterConnectionConfiguration;
import org.wso2.carbon.event.adapter.rabbitmq.internal.util.RabbitMQOutputEventAdapterConstants;
import org.wso2.carbon.event.adapter.rabbitmq.internal.util.RabbitMQOutputEventAdapterPublisher;
import org.wso2.carbon.event.output.adapter.core.EventAdapterUtil;
import org.wso2.carbon.event.output.adapter.core.OutputEventAdapter;
import org.wso2.carbon.event.output.adapter.core.OutputEventAdapterConfiguration;
import org.wso2.carbon.event.output.adapter.core.exception.OutputEventAdapterException;
import org.wso2.carbon.event.output.adapter.core.exception.TestConnectionNotSupportedException;

import java.io.IOException;
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
    private RabbitMQOutputEventAdapterPublisher mqttAdapterPublisher;
    private static ThreadPoolExecutor threadPoolExecutor;
    private static final Log log = LogFactory.getLog(RabbitMQOutputEventAdapter.class);
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

        tenantId = PrivilegedCarbonContext.getThreadLocalCarbonContext().getTenantId();

        //ThreadPoolExecutor will be assigned  if it is null
        if (threadPoolExecutor == null) {
            int minThread;
            int maxThread;
            int jobQueSize;
            long defaultKeepAliveTime;

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

            threadPoolExecutor = new ThreadPoolExecutor(minThread, maxThread, defaultKeepAliveTime,
                    TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(jobQueSize));
        }
    }

    /**
     * This method is used to test the connection of the publishing server.
     *
     * @throws TestConnectionNotSupportedException
     */
    @Override
    public void testConnect() throws TestConnectionNotSupportedException {
//        throw new TestConnectionNotSupportedException("Test connection is not available");
        tenantId = Integer.parseInt(eventAdapterConfiguration.getStaticProperties().get(RabbitMQOutputEventAdapterConstants.RABBITMQ_TENANT_NAME));
        RabbitMQOutputEventAdapterConnectionConfiguration rabbitMQOutputEventAdapterConnectionConfiguration
                = new RabbitMQOutputEventAdapterConnectionConfiguration(
                eventAdapterConfiguration.getStaticProperties().get(RabbitMQOutputEventAdapterConstants.RABBITMQ_SERVER_HOST_NAME),
                eventAdapterConfiguration.getStaticProperties().get(RabbitMQOutputEventAdapterConstants.RABBITMQ_SERVER_VIRTUAL_HOST),
                eventAdapterConfiguration.getStaticProperties().get(RabbitMQOutputEventAdapterConstants.RABBITMQ_SERVER_PORT),
                eventAdapterConfiguration.getStaticProperties().get(RabbitMQOutputEventAdapterConstants.RABBITMQ_SERVER_USERNAME),
                eventAdapterConfiguration.getStaticProperties().get(RabbitMQOutputEventAdapterConstants.RABBITMQ_SERVER_PASSWORD),
                eventAdapterConfiguration.getStaticProperties().get(RabbitMQOutputEventAdapterConstants.RABBITMQ_QUEUE_NAME)
        );

        RabbitMQOutputEventAdapterPublisher rabbitMQOutputEventAdapterPublisher = new RabbitMQOutputEventAdapterPublisher(rabbitMQOutputEventAdapterConnectionConfiguration, tenantId);

        Channel channel = rabbitMQOutputEventAdapterPublisher.getChannel();

        try {
            channel.basicQos(1);
        } catch (IOException e) {
            throw new RabbitAdapterMQException(e);
        }
    }

    /**
     * Can be called to connect to back end before events are published.
     *
     */
    @Override
    public void connect() {
        tenantId = Integer.parseInt(eventAdapterConfiguration.getStaticProperties().get(RabbitMQOutputEventAdapterConstants.RABBITMQ_TENANT_NAME));
        RabbitMQOutputEventAdapterConnectionConfiguration rabbitMQOutputEventAdapterConnectionConfiguration
                = new RabbitMQOutputEventAdapterConnectionConfiguration(
                eventAdapterConfiguration.getStaticProperties().get(RabbitMQOutputEventAdapterConstants.RABBITMQ_SERVER_HOST_NAME),
                eventAdapterConfiguration.getStaticProperties().get(RabbitMQOutputEventAdapterConstants.RABBITMQ_SERVER_VIRTUAL_HOST),
                eventAdapterConfiguration.getStaticProperties().get(RabbitMQOutputEventAdapterConstants.RABBITMQ_SERVER_PORT),
                eventAdapterConfiguration.getStaticProperties().get(RabbitMQOutputEventAdapterConstants.RABBITMQ_SERVER_USERNAME),
                eventAdapterConfiguration.getStaticProperties().get(RabbitMQOutputEventAdapterConstants.RABBITMQ_SERVER_PASSWORD),
                eventAdapterConfiguration.getStaticProperties().get(RabbitMQOutputEventAdapterConstants.RABBITMQ_QUEUE_NAME)
        );

        mqttAdapterPublisher = new RabbitMQOutputEventAdapterPublisher(rabbitMQOutputEventAdapterConnectionConfiguration, tenantId);
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

        try {
            threadPoolExecutor.submit(new RabbitMQSender(exchange, message));
        } catch (RejectedExecutionException e) {
            EventAdapterUtil.logAndDrop(eventAdapterConfiguration.getName(), message, "Job queue is full", e, log, tenantId);
        }
    }

    /**
     * Will be called after publishing is done, or when ConnectionUnavailableException is thrown.
     */
    @Override
    public void disconnect() {

        int tenantID = Integer.parseInt(eventAdapterConfiguration.getStaticProperties().get(RabbitMQOutputEventAdapterConstants.RABBITMQ_TENANT_NAME));

        try {
            if (mqttAdapterPublisher != null) {
                mqttAdapterPublisher.close(tenantID);
                mqttAdapterPublisher = null;
            }
        } catch (OutputEventAdapterException e) {
            log.error("Exception when closing the mqtt publisher connection on Output MQTT Adapter '" + eventAdapterConfiguration.getName() + "'", e);
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

    class RabbitMQSender implements Runnable {

        String exchange;
        Object message;

        RabbitMQSender(String exchange, Object message) {
            this.exchange = exchange;
            this.message = message;
        }

        @Override
        public void run() {
            try {
//                if (qos == null) {
                    mqttAdapterPublisher.publish(message.toString(), exchange);
//                } else {
//                    mqttAdapterPublisher.publish(Integer.parseInt(qos), message.toString(), topic);
//                }
            } catch (Throwable t) {
                EventAdapterUtil.logAndDrop(eventAdapterConfiguration.getName(), message, null, t, log, tenantId);

            }
        }
    }
}