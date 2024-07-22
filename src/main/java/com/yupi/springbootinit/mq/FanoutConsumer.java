package com.yupi.springbootinit.mq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class FanoutConsumer {
  private static final String EXCHANGE_NAME = "fanout-exchange";

  public static void main(String[] argv) throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    Connection connection = factory.newConnection();
    //创建两个通道
    Channel channel1 = connection.createChannel();
    Channel channel2 = connection.createChannel();

    //声明交换机
    channel1.exchangeDeclare(EXCHANGE_NAME, "fanout");

    //创建按队列1
    String queueName = "xiaowang_queue";
    channel1.queueDeclare(queueName,true,false,false,null);
    channel1.queueBind(queueName,EXCHANGE_NAME,"");

    //创建按队列2
    String queueName2 = "xiaoli_queue";
    channel1.queueDeclare(queueName2,true,false,false,null);
    channel1.queueBind(queueName2,EXCHANGE_NAME,"");


    System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
        String message = new String(delivery.getBody(), "UTF-8");
        System.out.println(" [x] Received '" + message + "'");
    };

    DeliverCallback deliverCallback2 = (consumerTag, delivery) -> {
      String message = new String(delivery.getBody(), "UTF-8");
      System.out.println(" [x] Received '" + message + "'");
    };
    channel1.basicConsume(queueName, true, deliverCallback, consumerTag -> { });
    channel2.basicConsume(queueName2, true, deliverCallback, consumerTag -> { });
  }
}