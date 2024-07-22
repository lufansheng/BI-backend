package com.yupi.springbootinit.mq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.util.HashMap;
import java.util.Map;

public class DlxDirectConsumer {
  // 死信队列
  private static final String DEAD_EXCHANGE_NAME = "dlx-direct-exchange";

  private static final String WORD_EXCHANGE_NAME = "direct2-exchange";

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
    channel.exchangeDeclare(WORD_EXCHANGE_NAME, "direct");

    // 创建用于指定死信队列的参数的Map对象
    Map<String, Object> args = new HashMap<>();
    // 将要创建的队列绑定到指定的交换机，并设置死信队列的参数
    args.put("x-dead-letter-exchange", DEAD_EXCHANGE_NAME);
    // 指定死信要转发到外包死信队列
    args.put("x-dead-letter-routing-key", "waibao");

    // 声明一个匿名队列，并获取队列名称
    String queueName = "a_queue";
    channel.queueDeclare(queueName,true,false,false,args);
    channel.queueBind(queueName,WORD_EXCHANGE_NAME,"a");

    // 创建用于指定死信队列的参数的Map对象
    Map<String, Object> args2 = new HashMap<>();
    // 将要创建的队列绑定到指定的交换机，并设置死信队列的参数
    args.put("x-dead-letter-exchange", DEAD_EXCHANGE_NAME);
    // 指定死信要转发到外包死信队列
    args.put("x-dead-letter-routing-key", "laoban");

    // 声明一个匿名队列，并获取队列名称
    String queueName2 = "b_queue";
    channel.queueDeclare(queueName2,true,false,false,args2);
    channel.queueBind(queueName2,WORD_EXCHANGE_NAME,"b");

    System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

    // 创建一个 DeliverCallback 实例来处理接收到的消息
    DeliverCallback deliverCallback1 = (consumerTag, delivery) -> {
      String message = new String(delivery.getBody(), "UTF-8");
      channel.basicNack(delivery.getEnvelope().getDeliveryTag(),false,false);

      System.out.println(" [a] Received '" +
            delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
    };

      // 创建一个 DeliverCallback 实例来处理接收到的消息
      DeliverCallback deliverCallback2 = (consumerTag, delivery) -> {
          String message = new String(delivery.getBody(), "UTF-8");
        channel.basicNack(delivery.getEnvelope().getDeliveryTag(),false,false);

        System.out.println(" [b] Received '" +
                  delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
      };
    // 开始消费队列中的消息，设置自动确认消息已被消费
    channel.basicConsume(queueName, false, deliverCallback1, consumerTag -> { });
    channel.basicConsume(queueName2, false, deliverCallback2, consumerTag -> { });
  }
}