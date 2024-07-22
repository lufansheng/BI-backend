package com.yupi.springbootinit.mq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.util.Scanner;

public class DirectProducer {
  // 定义交换机名称
  private static final String EXCHANGE_NAME = "direct-exchange";

  public static void main(String[] argv) throws Exception {
    // 创建连接工厂
    ConnectionFactory factory = new ConnectionFactory();
    // 设置连接工厂的主机地址为本地主机
    factory.setHost("localhost");
    // 建立连接并创建通道
    try (Connection connection = factory.newConnection();
         Channel channel = connection.createChannel()) {
        // 使用通道声明交换机，类型为direct
        channel.exchangeDeclare(EXCHANGE_NAME, "direct");
    	// 获取严重程度（路由键）和消息内容
        Scanner sc = new Scanner(System.in);
        while (sc.hasNext()){
            String userInput = sc.nextLine();
            String[] s = userInput.split(" ");
            if (s.length < 1){
                continue;
            }
            String message = s[0];
            String routingKey = s[1];

            channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes("UTF-8"));
            System.out.println(" [x] Sent '" + message + "'with routingKey'" + routingKey + "'");
        }
    	// 发布消息到交换机
    }
  }
  //..
}