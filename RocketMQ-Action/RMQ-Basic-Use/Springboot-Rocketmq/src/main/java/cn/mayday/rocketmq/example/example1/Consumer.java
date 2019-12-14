package cn.mayday.rocketmq.example.example1;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;

/**
 * @Author: Mayday05
 * @Date: 2019/12/14 17:37
 */
@Component
public class Consumer {

    /**
     * 读取属性文件中的消费者的组名
     */
    @Value("${apache.rocketmq.consumer.PushConsumerGroup}")
    private String consumerGroupName;

    /**
     * 读取属性文件中的NameServer地址
     */
    @Value("${apache.rocketmq.nameSrvAddr}")
    private String nameSrvAddr;

    @PostConstruct
    public void defaultMQPushConsumer() {

        // 消费者的组名
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroupName);

        // 指定NameServer地址，多个地址以 ; 隔开
        consumer.setNamesrvAddr(nameSrvAddr);
        try {
            // 订阅PushTopic下Tag为push的消息,指定Topic
            consumer.subscribe("PushTopic", "push");

            // 设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费
            // 如果非第一次启动，那么按照上次消费的位置继续消费
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            consumer.registerMessageListener(new MessageListenerConcurrently() {

                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext context) {
                    try {
                        for (MessageExt messageExt : list) {

                            System.out.println("messageExt: " + messageExt);//输出消息内容

                            String messageBody = new String(messageExt.getBody(), "utf-8");

                            System.out.println("消费响应：Msg: " + messageExt.getMsgId() + ",msgBody: " + messageBody);//输出消息内容

                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER; //稍后再试
                    }
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS; //消费成功
                }


            });
            // 启动消费者
            consumer.start();
            System.out.println("+++++++++++++++ 消费者启动成功 ++++++++++++++");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
