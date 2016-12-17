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
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.context.PrivilegedCarbonContext;
import org.wso2.carbon.event.output.adapter.core.OutputEventAdapterConfiguration;
import org.wso2.carbon.event.output.adapter.core.exception.OutputEventAdapterException;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.*;
import java.security.cert.CertificateException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class RabbitMQOutputEventAdapterPublisher {

    private static final Log LOGGER = LogFactory.getLog(RabbitMQOutputEventAdapterPublisher.class);
    private RabbitMQOutputEventAdapterConnectionConfiguration eventAdapterConfiguration;

    private ConnectionFactory connectionFactory;
    private Connection connection;

    private int tenantId;
    private Channel channel = null;
    private String queueName, routeKey, exchangeName;
    private String exchangeType;
    private String adapterName;
    private boolean connectionSucceeded = false;

    private int retryInterval = RabbitMQOutputEventAdapterConstants.DEFAULT_RETRY_INTERVAL;
    private int retryCountMax = RabbitMQOutputEventAdapterConstants.DEFAULT_RETRY_COUNT;

    private final int STATE_STOPPED = 0;
    private final int STATE_STARTED;
    private int workerState;

    public RabbitMQOutputEventAdapterPublisher(RabbitMQOutputEventAdapterConnectionConfiguration outputEventAdapterConfiguration,
                                               OutputEventAdapterConfiguration eventAdapterConfiguration) {

        LOGGER.debug("*** DEBUG RabbitMQOutputEventAdapterPublisher() connection init.");

        connectionFactory = new ConnectionFactory();
        this.eventAdapterConfiguration = outputEventAdapterConfiguration;
        this.queueName = eventAdapterConfiguration.getStaticProperties().get(RabbitMQOutputEventAdapterConstants.RABBITMQ_QUEUE_NAME);
        this.exchangeName = eventAdapterConfiguration.getStaticProperties().get(RabbitMQOutputEventAdapterConstants.RABBITMQ_EXCHANGE_NAME);
        this.exchangeType = eventAdapterConfiguration.getStaticProperties().get(RabbitMQOutputEventAdapterConstants.RABBITMQ_EXCHANGE_TYPE);
        this.routeKey = eventAdapterConfiguration.getStaticProperties().get(RabbitMQOutputEventAdapterConstants.RABBITMQ_QUEUE_ROUTING_KEY);
        this.adapterName = eventAdapterConfiguration.getName();

        if (eventAdapterConfiguration.getStaticProperties().get(RabbitMQOutputEventAdapterConstants.RABBITMQ_TENANT_NAME) != null) {
            PrivilegedCarbonContext.getThreadLocalCarbonContext().setTenantId(Integer.parseInt(eventAdapterConfiguration.getStaticProperties().get(RabbitMQOutputEventAdapterConstants.RABBITMQ_TENANT_NAME)));
            tenantId = PrivilegedCarbonContext.getThreadLocalCarbonContext().getTenantId();
        } else {
            tenantId = PrivilegedCarbonContext.getThreadLocalCarbonContext().getTenantId();
        }

        workerState = STATE_STOPPED;
        STATE_STARTED = 1;

        LOGGER.debug("*** DEBUG: RabbitMQOutputEventAdapterType() tenantId=" + tenantId);
        LOGGER.debug("*** DEBUG: RabbitMQOutputEventAdapterType() queueName=" + queueName);

        if (routeKey == null) {
            routeKey = queueName;
        }
        if (!eventAdapterConfiguration.getStaticProperties().
                get(RabbitMQOutputEventAdapterConstants.RABBITMQ_CONNECTION_SSL_ENABLED).equals("false")) {
            try {
                boolean sslEnabled = Boolean.parseBoolean(eventAdapterConfiguration.getStaticProperties().
                        get(RabbitMQOutputEventAdapterConstants.RABBITMQ_CONNECTION_SSL_ENABLED));
                if (sslEnabled) {
                    String keyStoreLocation = eventAdapterConfiguration.getStaticProperties().
                            get(RabbitMQOutputEventAdapterConstants.RABBITMQ_CONNECTION_SSL_KEYSTORE_LOCATION);
                    String keyStoreType = eventAdapterConfiguration.getStaticProperties().
                            get(RabbitMQOutputEventAdapterConstants.RABBITMQ_CONNECTION_SSL_KEYSTORE_TYPE);
                    String keyStorePassword = eventAdapterConfiguration.getStaticProperties().
                            get(RabbitMQOutputEventAdapterConstants.RABBITMQ_CONNECTION_SSL_KEYSTORE_PASSWORD);
                    String trustStoreLocation = eventAdapterConfiguration.getStaticProperties().
                            get(RabbitMQOutputEventAdapterConstants.RABBITMQ_CONNECTION_SSL_TRUSTSTORE_LOCATION);
                    String trustStoreType = eventAdapterConfiguration.getStaticProperties().
                            get(RabbitMQOutputEventAdapterConstants.RABBITMQ_CONNECTION_SSL_TRUSTSTORE_TYPE);
                    String trustStorePassword = eventAdapterConfiguration.getStaticProperties().
                            get(RabbitMQOutputEventAdapterConstants.RABBITMQ_CONNECTION_SSL_TRUSTSTORE_PASSWORD);
                    String sslVersion = eventAdapterConfiguration.getStaticProperties().
                            get(RabbitMQOutputEventAdapterConstants.RABBITMQ_CONNECTION_SSL_VERSION);

                    if (StringUtils.isEmpty(keyStoreLocation) || StringUtils.isEmpty(keyStoreType) ||
                            StringUtils.isEmpty(keyStorePassword) || StringUtils.isEmpty(trustStoreLocation) ||
                            StringUtils.isEmpty(trustStoreType) || StringUtils.isEmpty(trustStorePassword)) {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("Truststore and keystore information is not provided");
                        }
                        if (StringUtils.isNotEmpty(sslVersion)) {
                            connectionFactory.useSslProtocol(sslVersion);
                        } else {
                            LOGGER.info("Proceeding with default SSL configuration");
                            connectionFactory.useSslProtocol();
                        }
                    } else {
                        char[] keyPassphrase = keyStorePassword.toCharArray();
                        KeyStore ks = KeyStore.getInstance(keyStoreType);
                        ks.load(new FileInputStream(keyStoreLocation), keyPassphrase);

                        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                        kmf.init(ks, keyPassphrase);

                        char[] trustPassphrase = trustStorePassword.toCharArray();
                        KeyStore tks = KeyStore.getInstance(trustStoreType);
                        tks.load(new FileInputStream(trustStoreLocation), trustPassphrase);

                        TrustManagerFactory tmf = TrustManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                        tmf.init(tks);

                        SSLContext context = SSLContext.getInstance(sslVersion);
                        context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);

                        connectionFactory.useSslProtocol(context);
                    }
                }
            } catch (IOException e) {
                handleException("TrustStore or KeyStore File path is incorrect. Specify KeyStore location or " +
                        "TrustStore location Correctly.", e);
            } catch (CertificateException e) {
                handleException("TrustStore or keyStore is not specified. So Security certificate" +
                        " Exception happened.  ", e);
            } catch (NoSuchAlgorithmException e) {
                handleException("Algorithm is not available in KeyManagerFactory class.", e);
            } catch (UnrecoverableKeyException e) {
                handleException("Unable to recover Key", e);
            } catch (KeyStoreException e) {
                handleException("Error in KeyStore or TrustStore Type", e);
            } catch (KeyManagementException e) {
                handleException("Error in Key Management", e);
            }
        }

        if (!StringUtils.isEmpty(eventAdapterConfiguration.getStaticProperties().get(RabbitMQOutputEventAdapterConstants.
                RABBITMQ_FACTORY_HEARTBEAT))) {
            try {
                int heartbeatValue = Integer.parseInt(eventAdapterConfiguration.getStaticProperties().
                        get(RabbitMQOutputEventAdapterConstants.RABBITMQ_FACTORY_HEARTBEAT));
                connectionFactory.setRequestedHeartbeat(heartbeatValue);
            } catch (NumberFormatException e) {
                LOGGER.warn("Number format error in reading heartbeat value. Proceeding with default");
            }
        }

        connectionFactory.setHost(outputEventAdapterConfiguration.getHostName());
        try {
            int port = Integer.parseInt(outputEventAdapterConfiguration.getPort());
            if (port > 0) {
                connectionFactory.setPort(port);
            }
        } catch (NumberFormatException e) {
            handleException("Number format error in port number", e);
        }
        connectionFactory.setUsername(outputEventAdapterConfiguration.getUsername());
        connectionFactory.setPassword(outputEventAdapterConfiguration.getPassword());
        if (!StringUtils.isEmpty(eventAdapterConfiguration.getStaticProperties().get(RabbitMQOutputEventAdapterConstants.RABBITMQ_SERVER_VIRTUAL_HOST))) {
            connectionFactory.setVirtualHost(eventAdapterConfiguration.getStaticProperties().get(RabbitMQOutputEventAdapterConstants.RABBITMQ_SERVER_VIRTUAL_HOST));
        }
        if (!StringUtils.isEmpty(eventAdapterConfiguration.getStaticProperties().get(RabbitMQOutputEventAdapterConstants.RABBITMQ_CONNECTION_RETRY_COUNT))) {
            try {
                retryCountMax = Integer.parseInt(eventAdapterConfiguration.getStaticProperties().get(RabbitMQOutputEventAdapterConstants.RABBITMQ_CONNECTION_RETRY_COUNT));
            } catch (NumberFormatException e) {
                LOGGER.warn("Number format error in reading retry count value. Proceeding with default value (3)", e);
            }
        }
        if (!StringUtils.isEmpty(eventAdapterConfiguration.getStaticProperties().get(RabbitMQOutputEventAdapterConstants.RABBITMQ_CONNECTION_RETRY_INTERVAL))) {
            try {
                retryInterval = Integer.parseInt(eventAdapterConfiguration.getStaticProperties().get(RabbitMQOutputEventAdapterConstants.RABBITMQ_CONNECTION_RETRY_INTERVAL));
            } catch (NumberFormatException e) {
                LOGGER.warn("Number format error in reading retry interval value. Proceeding with default value" + " (30000ms)", e);
            }
        }
    }

    public void publish(String payload) throws IOException {

        LOGGER.debug("*** DEBUG RabbitMQOutputEventAdapterPublisher().publish()");

        /**
         *  Add configuration for DLM
         */
        Map<String, Object> arg = new HashMap<String, Object>();
        arg.put("x-dead-letter-exchange", exchangeName);
        arg.put("x-dead-letter-routing-key", routeKey);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Initializing RabbitMQ publisher for Event Adapter" + adapterName);
        }

        try {
            connection = getConnection();
            channel = openChannel();

            //declaring queue
            RabbitMQUtils.declareQueue(connection, channel, queueName, eventAdapterConfiguration.getDurable(), eventAdapterConfiguration.getExclusive(), eventAdapterConfiguration.getAutoDelete(), arg);
            //declaring exchange
            RabbitMQUtils.declareExchange(connection, channel, exchangeName, exchangeType, eventAdapterConfiguration.getExchangeDurable());
            channel.queueBind(queueName, exchangeName, routeKey);

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Bind queue '" + queueName + "' to exchange '" + exchangeName + "' with route key '" + routeKey + "'");
            }

            channel.basicPublish(exchangeName, queueName, MessageProperties.PERSISTENT_TEXT_PLAIN, payload.getBytes());

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Start publishing queue '" + queueName + "' for receiver " + adapterName + " payload: " + payload);
            }

        } catch (IOException e) {
            openChannel();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }

    /**
     * Check connection is available or not and calling method to create connection
     *
     * @throws IOException
     */
    private Connection getConnection() throws IOException {
        if (connection == null) {
            connection = makeConnection();
            connectionSucceeded = true;
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Output event adapter publisher getConnection(): " + connectionSucceeded);
        }
        return connection;
    }

    /**
     * Open the Channel
     */
    private Channel openChannel() {
        try {
            if (channel == null || !channel.isOpen()) {
                channel = connection.createChannel();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Channel is not open. Creating a new channel for receiver " + adapterName);
                }
            }
        } catch (IOException e) {
            handleException("Error in creating Channel", e);
        }
        return channel;
    }

    /**
     * Making connection to the rabbitMQ broker
     *
     * @throws IOException
     */
    private Connection makeConnection() throws IOException {
        Connection connection = null;
        try {
            connection = RabbitMQUtils.createConnection(connectionFactory);
            LOGGER.info("Successfully connected to RabbitMQ Broker " + adapterName);
        } catch (IOException e) {
            handleException("Error creating connection to RabbitMQ Broker. Reattempting to connect.", e);
            int retryC = 0;
            while ((connection == null) && (workerState == STATE_STARTED) && ((retryCountMax == -1) || (retryC < retryCountMax))) {
                retryC++;
                LOGGER.info("Attempting to create connection to RabbitMQ Broker" + adapterName + " in " + retryInterval + " ms");
                try {
                    Thread.sleep(retryInterval);
                    connection = RabbitMQUtils.createConnection(connectionFactory);
                    LOGGER.info("Successfully connected to RabbitMQ Broker" + adapterName);
                } catch (InterruptedException e1) {
                    handleException("Thread has been interrupted while trying to reconnect to RabbitMQ Broker " + adapterName, e1);
                } catch (TimeoutException e1) {
                    e1.printStackTrace();
                }
            }
            if (connection == null) {
                handleException("Could not connect to RabbitMQ Broker" + adapterName + "Error while creating " + "connection", e);
            }
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
        return connection;
    }

    public void close() throws OutputEventAdapterException {
        if (connection != null && connection.isOpen()) {
            try {
                connection.close();
                LOGGER.info("RabbitMQ connection closed for publisher " + adapterName);
            } catch (IOException e) {
                handleException("Error while closing RabbitMQ connection with the event adapter" + adapterName, e);
            } finally {
                connection = null;
            }
        }
        workerState = STATE_STOPPED;
        connectionSucceeded = true;
        if (channel != null && channel.isOpen()) {
            try {
                channel.close();
            } catch (IOException e) {
                handleException("Error while closing RabbitMQ channel with the event adapter " + adapterName, e);
            } catch (TimeoutException e) {
                e.printStackTrace();
            } finally {
                channel = null;
            }
        }
    }

    /**
     * Handle the exception by throwing the exception
     *
     * @param msg Error message of the exception.
     * @param e   Exception Object
     */
    private void handleException(String msg, Exception e) {
        LOGGER.error(msg, e);
        throw new RabbitMQException(msg, e);
    }

    /**
     * Handle the exception by throwing the exception
     *
     * @param msg Error message of the exception.
     */
    private void handleException(String msg) {
        LOGGER.error(msg);
        throw new RabbitMQException(msg);
    }
}