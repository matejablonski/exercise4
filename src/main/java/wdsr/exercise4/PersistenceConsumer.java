package wdsr.exercise4;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PersistenceConsumer {
  private static final Logger log = LoggerFactory.getLogger(PersistenceConsumer.class);
  static final String QUEUE_NAME = "MATEJABLONSKI.QUEUE";

  public static void main(String[] args) {
    ActiveMQConnectionFactory connectionFactory =
        new ActiveMQConnectionFactory("tcp://localhost:61616");
    Connection connection = null;
    try {
      connection = connectionFactory.createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Destination destination = session.createQueue(QUEUE_NAME);
      MessageConsumer consumer = session.createConsumer(destination);

      connection.start();
      int counter = 0;
      Message message = null;
      while (true) {
        message = consumer.receive(3000);
        TextMessage textMessage;
        if (!(message instanceof TextMessage)) {
          break;
        } else {
          counter++;
          textMessage = (TextMessage) message;
        }
        log.info(textMessage.getText());
      }
      log.info(counter + " received messages");
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
