package com.yupi.springbootinit.mq;

import com.rabbitmq.client.*;

public class DirectConsumer {
  // 定义我们正在监听的交换机名称
  private static final String EXCHANGE_NAME = "direct-exchange";

  public static void main(String[] argv) throws Exception {
    // 创建连接工厂
    ConnectionFactory factory = new ConnectionFactory();
    // 设置连接工厂的主机地址为本地主机
    factory.setHost("localhost");
    // 建立与 RabbitMQ 服务器的连接
    Connection connection = factory.newConnection();
    // 创建一个通道
    Channel channel = connection.createChannel();
	// 声明一个 direct 类型的交换机
    channel.exchangeDeclare(EXCHANGE_NAME, "direct");
    // 声明一个匿名队列，并获取队列名称
    String queueName = "xiaoshuai_queue";
    channel.queueDeclare(queueName,true,false,false,null);
    channel.queueBind(queueName,EXCHANGE_NAME,"shuai");

      // 声明一个匿名队列，并获取队列名称
      String queueName2 = "xiaoge_queue";
      channel.queueDeclare(queueName2,true,false,false,null);
      channel.queueBind(queueName2,EXCHANGE_NAME,"ge");

    System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

    // 创建一个 DeliverCallback 实例来处理接收到的消息
    DeliverCallback deliverCallback1 = (consumerTag, delivery) -> {
        String message = new String(delivery.getBody(), "UTF-8");
        System.out.println(" [shuai] Received '" +
            delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
    };

      // 创建一个 DeliverCallback 实例来处理接收到的消息
      DeliverCallback deliverCallback2 = (consumerTag, delivery) -> {
          String message = new String(delivery.getBody(), "UTF-8");
          System.out.println(" [ge] Received '" +
                  delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
      };
    // 开始消费队列中的消息，设置自动确认消息已被消费
    channel.basicConsume(queueName, true, deliverCallback1, consumerTag -> { });
    channel.basicConsume(queueName2, true, deliverCallback2, consumerTag -> { });
  }
}