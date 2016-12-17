/*
*  Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.carbon.event.adapter.rabbitmq.internal.ds;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.wso2.carbon.event.adapter.rabbitmq.RabbitMQInputEventAdapterFactory;
import org.wso2.carbon.event.input.adapter.core.InputEventAdapterFactory;

//@scr.component name="input.rabbitMQEventAdapterService.component" immediate="true"
@Component(name="input.RabbitMQEventAdapterService.component", immediate=true)
public class RabbitMQInputEventAdapterServiceDS {

    private static final Log log = LogFactory.getLog(RabbitMQInputEventAdapterServiceDS.class);

    /**
     * initialize the agent service here service here.
     *
     * @param context
     */
    @Activate
    protected void activate(ComponentContext context) {

        try {
            InputEventAdapterFactory rabbitmqEventEventAdapterFactory = new RabbitMQInputEventAdapterFactory();
            context.getBundleContext().registerService(InputEventAdapterFactory.class.getName(), rabbitmqEventEventAdapterFactory, null);
            if (log.isDebugEnabled()) {
                log.debug("Successfully deployed the input RabbitMQ adapter service");
            }

        } catch (RuntimeException e) {
            log.error("Can not create the input RabbitMQ adapter service ", e);
        }
    }

}
