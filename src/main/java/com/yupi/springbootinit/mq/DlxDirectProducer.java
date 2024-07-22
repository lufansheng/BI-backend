package com.yupi.springbootinit.mq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.util.Scanner;

public class DlxDirectProducer {
    // 死信队列
    private static final String DEAD_EXCHANGE_NAME = "dlx-direct-exchange";

    private static final String WORD_EXCHANGE_NAME = "direct2-exchange";

    public static void main(String[] argv) throws Exception {
        // 创建连接工厂
        ConnectionFactory factory = new ConnectionFactory();
        // 设置连接工厂的主机地址为本地主机
        factory.setHost("localhost");
        // 建立连接并创建通道
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            // 使用通道声明交换机，类型为direct
            channel.exchangeDeclare(DEAD_EXCHANGE_NAME, "direct");

            // 声明一个匿名队列，并获取队列名称
            String queueName = "laoban_queue";
            channel.queueDeclare(queueName, true, false, false, null);
            channel.queueBind(queueName, DEAD_EXCHANGE_NAME, "laoban");

            // 声明一个匿名队列，并获取队列名称
            String queueName1 = "waibao_queue";
            channel.queueDeclare(queueName1, true, false, false, null);
            channel.queueBind(queueName1, DEAD_EXCHANGE_NAME, "waibao");

            // 创建一个 DeliverCallback 实例来处理接收到的消息
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), "UTF-8");
                channel.basicNack(delivery.getEnvelope().getDeliveryTag(),false,false);

                System.out.println(" [laoban] Received '" +
                        delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
            };

            // 创建一个 DeliverCallback 实例来处理接收到的消息
            DeliverCallback deliverCallback1 = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), "UTF-8");
                channel.basicNack(delivery.getEnvelope().getDeliveryTag(),false,false);

                System.out.println(" [waibao] Received '" +
                        delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
            };
            channel.basicConsume(queueName, false, deliverCallback, consumerTag -> {
            });
            // 注册消费者，用于消费外包的死信队列，绑定回调函数
            channel.basicConsume(queueName1, false, deliverCallback1, consumerTag -> {
            });
            // 获取严重程度（路由键）和消息内容
            Scanner sc = new Scanner(System.in);
            while (sc.hasNext()) {
                String userInput = sc.nextLine();
                String[] s = userInput.split(" ");
                if (s.length < 1) {
                    continue;
                }
                String message = s[0];
                String routingKey = s[1];

                channel.basicPublish(WORD_EXCHANGE_NAME, routingKey, null, message.getBytes("UTF-8"));
                System.out.println(" [x] Sent '" + message + "'with routingKey'" + routingKey + "'");
            }
            // 发布消息到交换机
        }
    }
    //..
}