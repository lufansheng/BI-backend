package com.yupi.springbootinit.mq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class TopicConsumer {

  private static final String EXCHANGE_NAME = "topic-exchange";

  public static void main(String[] argv) throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();

    channel.exchangeDeclare(EXCHANGE_NAME, "topic");


      String queueName = "frontend_queue";
      channel.queueDeclare(queueName,true,false,false,null);
      channel.queueBind(queueName,EXCHANGE_NAME,"#.前端.#");

      // 声明一个匿名队列，并获取队列名称
      String queueName2 = "backend_queue";
      channel.queueDeclare(queueName2,true,false,false,null);
      channel.queueBind(queueName2,EXCHANGE_NAME,"#.后端.#");

      // 声明一个匿名队列，并获取队列名称
      String queueName3 = "backend_queue";
      channel.queueDeclare(queueName3,true,false,false,null);
      channel.queueBind(queueName3,EXCHANGE_NAME,"#.产品.#");

      System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

      // 创建一个 DeliverCallback 实例来处理接收到的消息
      DeliverCallback deliverCallback1 = (consumerTag, delivery) -> {
          String message = new String(delivery.getBody(), "UTF-8");
          System.out.println(" [a] Received '" +
                  delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
      };

      // 创建一个 DeliverCallback 实例来处理接收到的消息
      DeliverCallback deliverCallback2 = (consumerTag, delivery) -> {
          String message = new String(delivery.getBody(), "UTF-8");
          System.out.println(" [b] Received '" +
                  delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
      };

      // 创建一个 DeliverCallback 实例来处理接收到的消息
      DeliverCallback deliverCallback3 = (consumerTag, delivery) -> {
          String message = new String(delivery.getBody(), "UTF-8");
          System.out.println(" [c] Received '" +
                  delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
      };
      // 开始消费队列中的消息，设置自动确认消息已被消费
      channel.basicConsume(queueName, true, deliverCallback1, consumerTag -> { });
      channel.basicConsume(queueName2, true, deliverCallback2, consumerTag -> { });
      channel.basicConsume(queueName3, true, deliverCallback2, consumerTag -> { });

  }
}