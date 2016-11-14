/* Copyright (C) 2015~ Hyundai Heavy Industries. All Rights Reserved.
 *
 * This software is the confidential and proprietary information of Hyundai Heavy Industries
 * You shall not disclose such Confidential Information and shall use it only 
 * in accordance with the terms of the license agreement
 * you entered into with Hyundai Heavy Industries.
 *
 * Revision History
 * Author             Date              Description
 * ---------------	----------------	------------
 * Jerry Jeong	       2015. 4. 10.		    First Draft.
 */
/**
 * 
 */
package org.wso2.carbon.event.adapter.rabbitmq.internal.exception;

/**
 * <pre>
 * RabbitAdapterMQException.java
 * </pre>
 * 
 * @author jerryj
 * @date   2015. 4. 10.
 *
 */
public class RabbitAdapterMQException extends RuntimeException {

	private static final long serialVersionUID = 5705881542569296345L;

	public RabbitAdapterMQException(String msg) { super(msg); }

	public RabbitAdapterMQException(Exception e) {
		super(e);
	}

	/**
	 * <pre>
	 * Default Constructor
	 * </pre>
	 * @param e
	 */
	public RabbitAdapterMQException(String msg, Exception e) {
		super(msg, e);
	}
	//end of default constructor
}
//end of RabbitAdapterMQException.java