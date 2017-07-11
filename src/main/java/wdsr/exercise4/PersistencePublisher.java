package wdsr.exercise4;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PersistencePublisher {
  private static final Logger log = LoggerFactory.getLogger(PersistencePublisher.class);
  private static ActiveMQConnectionFactory connectionFactory =
      new ActiveMQConnectionFactory("tcp://localhost:61616");
  private static Connection connection = null;
  static final String TOPIC_NAME = "MATEJABLONSKI.TOPIC";

  public static void main(String[] args) {
    try {
      connection = connectionFactory.createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Destination destination = session.createTopic(TOPIC_NAME);
      MessageProducer producer = session.createProducer(destination);
      producer.setDeliveryMode(DeliveryMode.PERSISTENT);

      connection.start();
      long startTime = System.currentTimeMillis();

      for (int i = 0; i < 10000; i++) {
        Message msg = session.createTextMessage("test_" + i);
        producer.send(msg);
      }

      long endTime = System.currentTimeMillis();
      long persistentTime = endTime - startTime;
      log.info("10000 persistent messages sent in {" + persistentTime + "} milliseconds.");
      producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      startTime = System.currentTimeMillis();

      for (int i = 10000; i < 20000; i++) {
        Message msg = session.createTextMessage("test_" + i);
        producer.send(msg);
      }

      endTime = System.currentTimeMillis();
      long nonPersistenceTime = endTime - startTime;
      log.info("10000 non-persistent messages sent in {" + nonPersistenceTime + "} milliseconds.");
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
