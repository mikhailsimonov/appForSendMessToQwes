package com.simonov.utils;

import com.ibm.mq.jms.JMSC;
import com.ibm.mq.jms.MQQueueConnectionFactory;

import javax.jms.*;


public class JMSutil {
    public static String sendMessage = null;

    public static void putMessageToMq(String server, Integer port, String qwe, String channel, String type, String msg) {
        try {
            /*MQ Configuration*/
            MQQueueConnectionFactory mqQueueConnectionFactory = new MQQueueConnectionFactory();
            mqQueueConnectionFactory.setHostName(server);
            mqQueueConnectionFactory.setChannel(channel);//communications link
            mqQueueConnectionFactory.setPort(port);
            //mqQueueConnectionFactory.setQueueManager();//service provider
            mqQueueConnectionFactory.setTransportType(JMSC.MQJMS_TP_CLIENT_MQ_TCPIP);

            /*Create Connection */
            QueueConnection queueConnection = mqQueueConnectionFactory.createQueueConnection();
            queueConnection.start();

            /*Create session */
            QueueSession queueSession = queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);

            /*Create response queue */
            Queue queue = queueSession.createQueue(qwe);


            /*Create text message */
            TextMessage textMessage = queueSession.createTextMessage("put some message here");
            textMessage.setJMSReplyTo(queue);
            textMessage.setJMSType(type);//message type
            textMessage.setJMSExpiration(2 * 1000);//message expiration
            textMessage.setJMSDeliveryMode(DeliveryMode.PERSISTENT); //message delivery mode either persistent or non-persistemnt

            /*Create sender queue */
            QueueSender queueSender = queueSession.createSender(queueSession.createQueue(qwe));
            queueSender.setTimeToLive(2 * 1000);
            queueSender.send(textMessage);

            /*After sending a message we get message id */
            System.out.println("after sending a message we get message id " + textMessage.getJMSMessageID());
            sendMessage = "after sending a message we get message id " + textMessage.getJMSMessageID();
            String jmsCorrelationID = " JMSCorrelationID = '" + textMessage.getJMSMessageID() + "'";


            /*Within the session we have to create queue reciver */
            QueueReceiver queueReceiver = queueSession.createReceiver(queue, jmsCorrelationID);


            /*Receive the message from*/
            Message message = queueReceiver.receive(60 * 1000);
            String responseMsg = ((TextMessage) message).getText();

            queueSender.close();
            queueReceiver.close();
            queueSession.close();
            queueConnection.close();


        } catch (JMSException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
