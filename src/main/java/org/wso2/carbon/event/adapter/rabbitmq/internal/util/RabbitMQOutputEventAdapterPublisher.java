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
package org.wso2.carbon.event.adapter.rabbitmq.internal.util;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.event.adapter.rabbitmq.internal.exception.RabbitAdapterMQException;
import org.wso2.carbon.event.output.adapter.core.exception.OutputEventAdapterException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class RabbitMQOutputEventAdapterPublisher {

    private static final Log LOGGER = LogFactory.getLog(RabbitMQOutputEventAdapterPublisher.class);
    private int tenantId;
    private RabbitMQOutputEventAdapterConnectionConfiguration eventAdapterConfiguration;
    private HashMap<Integer, Channel> cache = new HashMap<Integer, Channel>();
    private Connection connection;
    private Channel channel;
    private String queue;

    public RabbitMQOutputEventAdapterPublisher(RabbitMQOutputEventAdapterConnectionConfiguration outputEventAdapterConfiguration, int _tenantId) {

        LOGGER.debug("*** DEBUG RabbitMQOutputEventAdapterPublisher()");

        eventAdapterConfiguration = outputEventAdapterConfiguration;
        queue = eventAdapterConfiguration.getQueueName();
        tenantId = _tenantId;
        channel = getChannel();

        LOGGER.debug("*** DEBUG: RabbitMQOutputEventAdapterType() tenantId=" + tenantId);
        LOGGER.debug("*** DEBUG: RabbitMQOutputEventAdapterType() channel=" + channel);
        LOGGER.debug("*** DEBUG: RabbitMQOutputEventAdapterType() queue=" + queue);
    }

//    public void publish(int qos, String payload, String topic) {
//        try {
//            // Create and configure a message
//            MqttMessage message = new MqttMessage(payload.getBytes());
//            message.setQos(qos);
//
//            // Send the message to the server, control is not returned until
//            // it has been delivered to the server meeting the specified
//            // quality of service.
//            mqttClient.publish(topic, message);
//
//        } catch (MqttException e) {
//            log.error("Error occurred when publishing message for MQTT server : "
//                    + mqttClient.getServerURI(), e);
//            handleException(e);
//        }
//    }

    public void publish(String payload, String exchange) {

        LOGGER.debug("*** DEBUG RabbitMQOutputEventAdapterPublisher().publish()");

        try {
            //create Queue
            createQueue(exchange);
            // Create and configure a message
            if (payload instanceof String) {
                channel.basicPublish("", queue, MessageProperties.PERSISTENT_TEXT_PLAIN, ((String)payload).getBytes());
            } else {
                channel.basicPublish("", queue, MessageProperties.PERSISTENT_TEXT_PLAIN, payload.toString().getBytes());
            }
            LOGGER.debug("*** DEBUG: [x] Sent " + payload.getClass() + " type, '" + payload + "'");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close(int tenantId) throws OutputEventAdapterException {
        LOGGER.debug("*** DEBUG RabbitMQOutputEventAdapterPublisher().close()");

        Channel channel = cache.remove(tenantId);

        if (channel != null) {
            try {
                channel.close();
                //channel.getConnection().close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                LOGGER.warn("RabbitMQ connection was closed already", e);
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * <pre>
     * get channel
     * </pre>
     *
     * @return
     */
    public Channel getChannel() {
        LOGGER.debug("*** DEBUG RabbitMQOutputEventAdaptorType.getChannel() start.");

        Channel channel = cache.get(tenantId);

        if (channel == null || !channel.isOpen()) {
            String hostName = eventAdapterConfiguration.getHostName();
            String port = eventAdapterConfiguration.getPort();
            String password = eventAdapterConfiguration.getPassword();
            String userName = eventAdapterConfiguration.getUsername();
            String virtualHost = eventAdapterConfiguration.getVirtualHostName();

            ConnectionFactory factory = this.getConnectionFactory(hostName, Integer.parseInt(port), userName, password, virtualHost);

            LOGGER.debug("*** DEBUG: RabbitMQOutputEventAdapterType.getChannel() hostName=" + hostName);
            LOGGER.debug("*** DEBUG: RabbitMQOutputEventAdapterType.getChannel() port=" + port);
            LOGGER.debug("*** DEBUG: RabbitMQOutputEventAdapterType.getChannel() userName=" + userName);
            LOGGER.debug("*** DEBUG: RabbitMQOutputEventAdapterType.getChannel() password=" + password);
            LOGGER.debug("*** DEBUG: RabbitMQOutputEventAdapterType.getChannel() virtualHost=" + virtualHost);
            LOGGER.debug("*** DEBUG: RabbitMQOutputEventAdapterType.getChannel() tenantId=" + tenantId);

            try {
                if (connection == null || !connection.isOpen()) {
                    connection = factory.newConnection();
                }

                /**
                 * Multiple operations into Channel instance are serialized and thread-safe
                 */
                channel = connection.createChannel();
                channel.basicQos(1);
                cache.put(tenantId, channel);
            } catch (IOException e) {
                LOGGER.warn("Failed to create communication channel", e);
                throw new RabbitAdapterMQException(e);
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
        }

        return channel;
    }
    //end of getChannel()

    /**
     * <pre>
     * Create a rabbitmq ConnectionFactory instance
     * </pre>
     * @param hostName
     * @param port
     * @param userName
     * @param password
     * @param virtualHost
     * @return
     */
    private synchronized static ConnectionFactory getConnectionFactory(String hostName, int port, String userName, String password, String virtualHost) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(hostName);
        factory.setPort(port);
        factory.setUsername(userName);
        factory.setPassword(password);
        factory.setVirtualHost(virtualHost);

        /**
         * Add connection recovery logic
         * @author Sang-Cheon Park
         * @date 2015.07.16
         */
        /**
         * connection that will recover automatically
         */
        factory.setAutomaticRecoveryEnabled(true);
        /**
         * attempt recovery every 5 seconds
         */
        factory.setNetworkRecoveryInterval(5*1000);
        /**
         * wait maximum 10 seconds to connect(if connection failed, will be retry to connect after 5 seconds)
         */
        factory.setConnectionTimeout(10*1000);

        return factory;
    }
    //end of getConnectionFactory()

    private void createQueue(String exchange){
        try {
            boolean isExist = false;
            try {
                channel.queueDeclarePassive(queue);
                isExist = true;
            } catch (IOException e) {
                LOGGER.info("*** INFO : [" + queue + "] does not exist.");
            }

            if (!isExist) {
//                String dlmExchangeName = "exchange_dlm";
                String dlmExchangeName = exchange;
                String routingKey = queue;

                if (!channel.isOpen()) {
                    channel = getChannel();
                }

                /**
                 *  Add configuration for DLM
                 */
                Map<String, Object> arg = new HashMap<String, Object>();
                arg.put("x-dead-letter-exchange", dlmExchangeName);
                arg.put("x-dead-letter-routing-key", routingKey);

                /**
                 *  Create a queue and binding with DLM
                 */
                channel.queueDeclare(queue, true, false, false, arg);
                channel.queueBind(queue, dlmExchangeName, routingKey);
            }
        } catch (IOException e) {
            throw new RabbitAdapterMQException(e);
        }
    }
}