package cn.minalz.example;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.List;

/**
 * 消息生产者
 */
public class RocketMqProducer {


    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        //事务消息的时候会用到
        DefaultMQProducer producer=new DefaultMQProducer("gp_producer_group");
        producer.setNamesrvAddr("localhost:9876"); //它会从命名服务器上拿到broker的地址
        producer.start();

        int num=0;
        while(num<20){
            num++;
            //Topic
            //tags -> 标签 （分类） -> (筛选)
            Message message=new Message("minalz_test_topic","TagA",("Hello , RocketMQ:"+num).getBytes());

            //消息路由策略
            // 同步发送
            /*SendResult send = producer.send(message, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                    return list.get(0);
                }
            }, "key-" + num);*/
            // 异步发送
            /*producer.send(message, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.printf("%s%n", sendResult);
                }

                @Override
                public void onException(Throwable throwable) {
                    throwable.printStackTrace();
                }
            });*/
            // 自定义策略发送消息
            producer.send(message, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                    // key 有可能是负数 所以取绝对值
                    int key = Math.abs(o.hashCode());
                    int size = list.size();
                    int index = key%size;
                    return list.get(index);
                }
            }, "key_" + num);
        }
    }
}
