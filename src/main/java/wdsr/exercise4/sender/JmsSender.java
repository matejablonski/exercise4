package wdsr.exercise4.sender;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Set;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wdsr.exercise4.Order;

public class JmsSender {
  private static final Logger log = LoggerFactory.getLogger(JmsSender.class);

  private final String queueName;
  private final String topicName;

  private Connection connection = null;

  public JmsSender(final String queueName, final String topicName) {
    this.queueName = queueName;
    this.topicName = topicName;
    connection = null;
  }

  /**
   * This method creates an Order message with the given parameters and sends it as an ObjectMessage
   * to the queue.
   * 
   * @param orderId ID of the product
   * @param product Name of the product
   * @param price Price of the product
   * @throws JMSException
   */
  public void sendOrderToQueue(final int orderId, final String product, final BigDecimal price)
      throws JMSException {
    Order order = new Order(orderId, product, price);

    try {
      ActiveMQConnectionFactory connectionFactory =
          new ActiveMQConnectionFactory("vm://localhost:62616");
      connection = connectionFactory.createConnection();
      connection.start();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Destination destination = session.createQueue(queueName);
      MessageProducer producer = session.createProducer(destination);
      producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      ObjectMessage objMessage = session.createObjectMessage(order);
      objMessage.setJMSType(Order.class.getSimpleName());
      objMessage.setStringProperty("WDSR-System", "OrderProcessor");
      objMessage.setObject(order);
      producer.send(objMessage);

    } catch (JMSException e) {
      e.printStackTrace();
    } finally {
      if (connection != null) {
        try {
          connection.close();
        } catch (JMSException e) {
          e.printStackTrace();
        }
      }
    }
  }

  /**
   * This method sends the given String to the queue as a TextMessage.
   * 
   * @param text String to be sent
   */
  public void sendTextToQueue(String text) {

    try {
      ActiveMQConnectionFactory connectionFactory =
          new ActiveMQConnectionFactory("vm://localhost:62616");
      connection = connectionFactory.createConnection();
      connection.start();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Destination destination = session.createQueue(queueName);
      MessageProducer producer = session.createProducer(destination);
      producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      Message message = session.createTextMessage(text);
      producer.send(message);

    } catch (JMSException e) {
      e.printStackTrace();
    } finally {
      if (connection != null) {
        try {
          connection.close();
        } catch (JMSException e) {
          e.printStackTrace();
        }
      }
    }
  }

  /**
   * Sends key-value pairs from the given map to the topic as a MapMessage.
   * 
   * @param map Map of key-value pairs to be sent.
   */
  public void sendMapToTopic(Map<String, String> map) {
    try {
      ActiveMQConnectionFactory connectionFactory =
          new ActiveMQConnectionFactory("vm://localhost:62616");
      connection = connectionFactory.createConnection();
      connection.start();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Destination destination = session.createTopic(topicName);
      MessageProducer producer = session.createProducer(destination);
      producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MapMessage message = session.createMapMessage();
      Set<String> keys = map.keySet();

      for (String key : keys) {
        String value = map.get(key);
        message.setObject(key, value);
      }

      producer.send(message);

    } catch (JMSException e) {
      e.printStackTrace();
    } finally {
      if (connection != null) {
        try {
          connection.close();
        } catch (JMSException e) {
          e.printStackTrace();
        }
      }
    }
  }
}
