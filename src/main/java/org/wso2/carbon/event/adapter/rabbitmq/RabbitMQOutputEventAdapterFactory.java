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
package org.wso2.carbon.event.adapter.rabbitmq;

import org.wso2.carbon.event.adapter.rabbitmq.internal.util.RabbitMQOutputEventAdapterConstants;
import org.wso2.carbon.event.output.adapter.core.*;

import java.util.*;

/**
 * The mqtt event adapter factory class to create a mqtt output adapter
 */
public class RabbitMQOutputEventAdapterFactory extends OutputEventAdapterFactory {
    private ResourceBundle resourceBundle =
            ResourceBundle.getBundle("org.wso2.carbon.event.output.adapter.rabbitmq.i18n.Resources", Locale.getDefault());

    /**
     * Here type needs to be specified,
     * this string will be displayed in the publisher interface in the adapter type drop down list.
     *
     * @return
     */
    @Override
    public String getType() {
        return RabbitMQOutputEventAdapterConstants.ADAPTER_TYPE_RABBITMQ;
    }

    /**
     * Specify supported message formats for the created publisher type.
     *
     * @return
     */
    @Override
    public List<String> getSupportedMessageFormats() {
        List<String> supportedMessageFormats = new ArrayList<String>();
        supportedMessageFormats.add(MessageType.XML);
        supportedMessageFormats.add(MessageType.JSON);
        supportedMessageFormats.add(MessageType.TEXT);
        return supportedMessageFormats;
    }

    /**org.wso2.carbon.event.output.adapter
     * Here static properties have to be specified.
     * These properties will use the values assigned when creating a publisher.
     * For more information on adapter properties see Event Publisher Configuration.
     *
     * @return
     */
    @Override
    public List<Property> getStaticPropertyList() {

        List<Property> staticPropertyList = new ArrayList<Property>();
        // Server Host Name
        Property hostNameProperty = new Property(RabbitMQOutputEventAdapterConstants.RABBITMQ_SERVER_HOST_NAME);
        hostNameProperty.setDisplayName(
                resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_SERVER_HOST_NAME));
        hostNameProperty.setRequired(true);
        hostNameProperty.setHint(resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_SERVER_HOST_NAME_HINT));
        staticPropertyList.add(hostNameProperty);

        // Server Server Port
        Property serverPortProperty = new Property(RabbitMQOutputEventAdapterConstants.RABBITMQ_SERVER_PORT);
        serverPortProperty.setDisplayName(
                resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_SERVER_PORT));
        serverPortProperty.setRequired(true);
        serverPortProperty.setHint(resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_SERVER_PORT_HINT));
        staticPropertyList.add(serverPortProperty);

        // Server User name
        Property userNameProperty = new Property(RabbitMQOutputEventAdapterConstants.RABBITMQ_SERVER_USERNAME);
        userNameProperty.setDisplayName(
                resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_SERVER_USERNAME));
        userNameProperty.setRequired(true);
        //userNameProperty.setDefaultValue("guest");
        userNameProperty.setHint(resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_SERVER_USERNAME_HINT));
        staticPropertyList.add(userNameProperty);

        // Server Password
        Property passwordProperty = new Property(RabbitMQOutputEventAdapterConstants.RABBITMQ_SERVER_PASSWORD);
        passwordProperty.setDisplayName(
                resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_SERVER_PASSWORD));
        passwordProperty.setRequired(true);
        passwordProperty.setSecured(true);
        //  passwordProperty.setDefaultValue("guest");
        passwordProperty.setHint(resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_SERVER_PASSWORD_HINT));
        staticPropertyList.add(passwordProperty);

        // Server Queue Name
        Property queueNameProperty = new Property(RabbitMQOutputEventAdapterConstants.RABBITMQ_QUEUE_NAME);
        queueNameProperty.setDisplayName(
                resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_QUEUE_NAME));
        queueNameProperty.setRequired(true);
        queueNameProperty.setHint(resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_QUEUE_NAME_HINT));
        staticPropertyList.add(queueNameProperty);

        // Server Exchange Name
        Property exchangeNameProperty = new Property(RabbitMQOutputEventAdapterConstants.RABBITMQ_EXCHANGE_NAME);
        exchangeNameProperty.setDisplayName(
                resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_EXCHANGE_NAME));
        exchangeNameProperty.setRequired(true);
        exchangeNameProperty.setHint(resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_EXCHANGE_NAME_HINT));
        staticPropertyList.add(exchangeNameProperty);

        //Queue Durable
        Property queueDurableProperty = new Property(RabbitMQOutputEventAdapterConstants.RABBITMQ_QUEUE_DURABLE);
        queueDurableProperty.setRequired(false);
        queueDurableProperty.setDisplayName(
                resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_QUEUE_DURABLE));
        queueDurableProperty.setHint(resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_QUEUE_DURABLE_HINT));
        queueDurableProperty.setOptions(new String[]{"true", "false"});
        queueDurableProperty.setDefaultValue("true");
        staticPropertyList.add(queueDurableProperty);

        //Queue Exclusive
        Property queueExclusiveProperty = new Property(RabbitMQOutputEventAdapterConstants.RABBITMQ_QUEUE_EXCLUSIVE);
        queueExclusiveProperty.setRequired(false);
        queueExclusiveProperty.setDisplayName(
                resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_QUEUE_EXCLUSIVE));
        queueExclusiveProperty.setHint(resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_QUEUE_EXCLUSIVE_HINT));
        queueExclusiveProperty.setOptions(new String[]{"true", "false"});
        queueExclusiveProperty.setDefaultValue("false");
        staticPropertyList.add(queueExclusiveProperty);

        //Queue Auto Delete
        Property queueAutoDeleteProperty = new Property(RabbitMQOutputEventAdapterConstants.RABBITMQ_QUEUE_AUTO_DELETE);
        queueAutoDeleteProperty.setRequired(false);
        queueAutoDeleteProperty.setDisplayName(
                resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_QUEUE_AUTO_DELETE));
        queueAutoDeleteProperty.setHint(resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_QUEUE_AUTO_DELETE_HINT));
        queueAutoDeleteProperty.setOptions(new String[]{"true", "false"});
        queueAutoDeleteProperty.setDefaultValue("false");
        staticPropertyList.add(queueAutoDeleteProperty);

        //Queue Auto Ack
        Property queueAutoAckProperty = new Property(RabbitMQOutputEventAdapterConstants.RABBITMQ_QUEUE_AUTO_ACK);
        queueAutoAckProperty.setRequired(false);
        queueAutoAckProperty.setDisplayName(
                resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_QUEUE_AUTO_ACK));
        queueAutoAckProperty.setHint(resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_QUEUE_AUTO_ACK_HINT));
        queueAutoAckProperty.setOptions(new String[]{"true", "false"});
        queueAutoAckProperty.setDefaultValue("false");
        staticPropertyList.add(queueAutoAckProperty);

        // Queue Routing Key
        Property routingKeyProperty = new Property(RabbitMQOutputEventAdapterConstants.RABBITMQ_QUEUE_ROUTING_KEY);
        routingKeyProperty.setDisplayName(
                resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_QUEUE_ROUTING_KEY));
        routingKeyProperty.setRequired(false);
        routingKeyProperty.setHint(resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_QUEUE_ROUTING_KEY_HINT));
        staticPropertyList.add(routingKeyProperty);

        // Tenant Name
        Property tenantNameProperty = new Property(RabbitMQOutputEventAdapterConstants.RABBITMQ_TENANT_NAME);
        tenantNameProperty.setDisplayName(
                resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_TENANT_NAME));
        tenantNameProperty.setRequired(false);
        tenantNameProperty.setHint(resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_TENANT_NAME_HINT));
        staticPropertyList.add(tenantNameProperty);

        //Consumer Tag
        Property consumerDurableProperty = new Property(RabbitMQOutputEventAdapterConstants.CONSUMER_TAG);
        consumerDurableProperty.setRequired(false);
        consumerDurableProperty.setDisplayName(
                resourceBundle.getString(RabbitMQOutputEventAdapterConstants.CONSUMER_TAG));
        consumerDurableProperty.setHint(resourceBundle.getString(RabbitMQOutputEventAdapterConstants.CONSUMER_TAG_HINT));
        staticPropertyList.add(consumerDurableProperty);

        // Exchange Type
        Property exchangeTypeProperty = new Property(RabbitMQOutputEventAdapterConstants.RABBITMQ_EXCHANGE_TYPE);
        exchangeTypeProperty.setDisplayName(
                resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_EXCHANGE_TYPE));
        exchangeTypeProperty.setRequired(false);
        exchangeTypeProperty.setHint(resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_EXCHANGE_TYPE_HINT));
        staticPropertyList.add(exchangeTypeProperty);

        //Exchange Durable
        Property exchangeDurableProperty = new Property(RabbitMQOutputEventAdapterConstants.RABBITMQ_EXCHANGE_DURABLE);
        exchangeDurableProperty.setRequired(false);
        exchangeDurableProperty.setDisplayName(
                resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_EXCHANGE_DURABLE));
        exchangeDurableProperty.setHint(resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_EXCHANGE_DURABLE_HINT));
        exchangeDurableProperty.setOptions(new String[]{"true", "false"});
        exchangeDurableProperty.setDefaultValue("false");
        staticPropertyList.add(exchangeDurableProperty);

        //Exchange Auto Delete
        Property exchangeAutoDeleteProperty = new Property(RabbitMQOutputEventAdapterConstants.RABBITMQ_EXCHANGE_AUTO_DELETE);
        exchangeAutoDeleteProperty.setRequired(false);
        exchangeAutoDeleteProperty.setDisplayName(
                resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_EXCHANGE_AUTO_DELETE));
        exchangeAutoDeleteProperty.setHint(resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_EXCHANGE_AUTO_DELETE_HINT));
        exchangeAutoDeleteProperty.setOptions(new String[]{"true", "false"});
        exchangeAutoDeleteProperty.setDefaultValue("false");
        staticPropertyList.add(exchangeAutoDeleteProperty);

        // Retry Count
        Property retryCountProperty = new Property(RabbitMQOutputEventAdapterConstants.RABBITMQ_CONNECTION_RETRY_COUNT);
        retryCountProperty.setDisplayName(
                resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_CONNECTION_RETRY_COUNT));
        retryCountProperty.setRequired(false);
        retryCountProperty.setHint(resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_CONNECTION_RETRY_COUNT_HINT));
        staticPropertyList.add(retryCountProperty);

        // Retry Interval
        Property retryIntervalProperty = new Property(RabbitMQOutputEventAdapterConstants.RABBITMQ_CONNECTION_RETRY_INTERVAL);
        retryIntervalProperty.setDisplayName(
                resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_CONNECTION_RETRY_INTERVAL));
        retryIntervalProperty.setRequired(false);
        retryIntervalProperty.setHint(resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_CONNECTION_RETRY_INTERVAL_HINT));
        staticPropertyList.add(retryIntervalProperty);

        // Virtual Host
        Property virtualHostProperty = new Property(RabbitMQOutputEventAdapterConstants.RABBITMQ_SERVER_VIRTUAL_HOST);
        virtualHostProperty.setDisplayName(
                resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_SERVER_VIRTUAL_HOST));
        virtualHostProperty.setRequired(false);
        virtualHostProperty.setHint(resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_SERVER_VIRTUAL_HOST_HINT));
        staticPropertyList.add(virtualHostProperty);

        // Factory Heart beat
        Property heartBeatProperty = new Property(RabbitMQOutputEventAdapterConstants.RABBITMQ_FACTORY_HEARTBEAT);
        heartBeatProperty.setDisplayName(
                resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_FACTORY_HEARTBEAT));
        heartBeatProperty.setRequired(false);
        heartBeatProperty.setHint(resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_FACTORY_HEARTBEAT_HINT));
        staticPropertyList.add(heartBeatProperty);

        // SSL Enable
        Property sslEnableProperty = new Property(RabbitMQOutputEventAdapterConstants.RABBITMQ_CONNECTION_SSL_ENABLED);
        sslEnableProperty.setRequired(false);
        sslEnableProperty.setDisplayName(
                resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_CONNECTION_SSL_ENABLED));
        sslEnableProperty.setHint(resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_CONNECTION_SSL_ENABLED_HINT));
        sslEnableProperty.setOptions(new String[]{"true", "false"});
        sslEnableProperty.setDefaultValue("false");
        staticPropertyList.add(sslEnableProperty);

        // SSL Keystore Location
        Property keystoreLocationProperty = new Property(RabbitMQOutputEventAdapterConstants.RABBITMQ_CONNECTION_SSL_KEYSTORE_LOCATION);
        keystoreLocationProperty.setDisplayName(
                resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_CONNECTION_SSL_KEYSTORE_LOCATION));
        keystoreLocationProperty.setRequired(false);
        staticPropertyList.add(keystoreLocationProperty);

        // SSL Keystore Type
        Property keystoreTypeProperty = new Property(RabbitMQOutputEventAdapterConstants.RABBITMQ_CONNECTION_SSL_KEYSTORE_TYPE);
        keystoreTypeProperty.setDisplayName(
                resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_CONNECTION_SSL_KEYSTORE_TYPE));
        keystoreTypeProperty.setRequired(false);
        staticPropertyList.add(keystoreTypeProperty);

        // SSL Keystore Password
        Property keystorePasswordProperty = new Property(RabbitMQOutputEventAdapterConstants.RABBITMQ_CONNECTION_SSL_KEYSTORE_PASSWORD);
        keystorePasswordProperty.setDisplayName(
                resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_CONNECTION_SSL_KEYSTORE_PASSWORD));
        keystorePasswordProperty.setRequired(false);
        staticPropertyList.add(keystorePasswordProperty);

        // SSL Truststore Location
        Property truststoreLocationProperty = new Property(RabbitMQOutputEventAdapterConstants.RABBITMQ_CONNECTION_SSL_TRUSTSTORE_LOCATION);
        truststoreLocationProperty.setDisplayName(
                resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_CONNECTION_SSL_TRUSTSTORE_LOCATION));
        truststoreLocationProperty.setRequired(false);
        staticPropertyList.add(truststoreLocationProperty);

        // SSL Truststore Type
        Property truststoreTypeProperty = new Property(RabbitMQOutputEventAdapterConstants.RABBITMQ_CONNECTION_SSL_TRUSTSTORE_TYPE);
        truststoreTypeProperty.setDisplayName(
                resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_CONNECTION_SSL_TRUSTSTORE_TYPE));
        truststoreTypeProperty.setRequired(false);
        staticPropertyList.add(truststoreTypeProperty);

        // SSL Truststore Password
        Property truststorePasswordProperty = new Property(RabbitMQOutputEventAdapterConstants.RABBITMQ_CONNECTION_SSL_TRUSTSTORE_PASSWORD);
        truststorePasswordProperty.setDisplayName(
                resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_CONNECTION_SSL_TRUSTSTORE_PASSWORD));
        truststorePasswordProperty.setRequired(false);
        staticPropertyList.add(truststorePasswordProperty);

        // SSL Version
        Property versionProperty = new Property(RabbitMQOutputEventAdapterConstants.RABBITMQ_CONNECTION_SSL_VERSION);
        versionProperty.setDisplayName(
                resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_CONNECTION_SSL_VERSION));
        versionProperty.setRequired(false);
        staticPropertyList.add(versionProperty);

        return staticPropertyList;

    }

    /**
     * You can define dynamic properties similar to static properties,
     * the only difference is dynamic property values can be derived by events handling by publisher.
     * For more information on adapter properties see Event Publisher Configuration.
     *
     * @return
     */
    @Override
    public List<Property> getDynamicPropertyList() {

        List<Property> dynamicstaticPropertyList = new ArrayList<Property>();

        // Tenant Name
//        Property tenantNameProperty = new Property(RabbitMQOutputEventAdapterConstants.RABBITMQ_TENANT_NAME);
//        tenantNameProperty.setDisplayName(
//                resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_TENANT_NAME));
//        tenantNameProperty.setRequired(false);
//        tenantNameProperty.setHint(resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_TENANT_NAME_HINT));
//        dynamicstaticPropertyList.add(tenantNameProperty);

        return dynamicstaticPropertyList;
    }

    /**
     * Specify any hints to be displayed in the management console.
     *
     * @return
     */
    @Override
    public String getUsageTips() {
        return null;
    }

    /**
     * This method creates the publisher by specifying event adapter configuration
     * and global properties which are common to every adapter type.
     *
     * @param outputEventAdapterConfiguration
     * @param globalProperties
     * @return
     */
    @Override
    public OutputEventAdapter createEventAdapter(OutputEventAdapterConfiguration outputEventAdapterConfiguration,
                                                 Map<String, String> globalProperties) {
        return new RabbitMQOutputEventAdapter(outputEventAdapterConfiguration, globalProperties);
    }
}