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
        hostNameProperty.setDisplayName(resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_SERVER_HOST_NAME));
        hostNameProperty.setRequired(true);
        hostNameProperty.setHint(resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_SERVER_HOST_NAME_HINT));
        staticPropertyList.add(hostNameProperty);

        // Virtual Host
        Property virtualHostProperty = new Property(RabbitMQOutputEventAdapterConstants.RABBITMQ_SERVER_VIRTUAL_HOST);
        virtualHostProperty.setDisplayName(
                resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_SERVER_VIRTUAL_HOST));
        virtualHostProperty.setRequired(false);
        virtualHostProperty.setHint(resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_SERVER_VIRTUAL_HOST_HINT));
        staticPropertyList.add(virtualHostProperty);

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
        Property tenantNameProperty = new Property(RabbitMQOutputEventAdapterConstants.RABBITMQ_TENANT_NAME);
        tenantNameProperty.setDisplayName(
                resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_TENANT_NAME));
        tenantNameProperty.setRequired(true);
        tenantNameProperty.setHint(resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_TENANT_NAME_HINT));
        dynamicstaticPropertyList.add(tenantNameProperty);

        // Server Exchange Name
        Property exchangeNameProperty = new Property(RabbitMQOutputEventAdapterConstants.RABBITMQ_EXCHANGE_NAME);
        exchangeNameProperty.setDisplayName(
                resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_EXCHANGE_NAME));
        exchangeNameProperty.setRequired(false);
        exchangeNameProperty.setHint(resourceBundle.getString(RabbitMQOutputEventAdapterConstants.RABBITMQ_EXCHANGE_NAME_HINT));
        dynamicstaticPropertyList.add(exchangeNameProperty);

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