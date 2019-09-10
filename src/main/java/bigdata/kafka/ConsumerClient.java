package bigdata.kafka;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumerClient {
    public static final String brokers = "10.192.208.176:9092, 10.193.21.244:9092, 10.192.208.130:9092";
    public static final String groupId = "cg1";
    public static final AtomicBoolean isRunning = new AtomicBoolean(true);

    /**
     * 配置consumer相关参数
     * @return
     */
    public static Properties initConfig(){
        //防止参数的键和值由于人为原因写错，可以使用kafka客户端内置的工具类进行优化
        Properties props = new Properties();
        //设置bootstrap.servers参数值，consumer客户端连接的borker地址列表
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);

        //设置key和value使用String类型的反序列化器
        //props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //下一行代码是等价设置
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        //props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //下一行代码是等价设置
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        //consumer组id
        props.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);

        return props;
    }

    /**
     * 从订阅的主题中拉取消息消费，自动提交offset
     * @param topic 订阅的主题
     */
    public void pollMessage(String topic) throws InterruptedException {
        //1）配置consumer相关参数
        Properties props = initConfig();

        //2）创建Consumer实例
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(props);

        //3）订阅主题
        consumer.subscribe(Arrays.asList(topic));

        //4）拉取消息并消费
        //通过重复轮询的方式，consumer客户端通过poll方法从订阅的topic中拉取消息
        while(isRunning.get()){
            Thread.sleep(2000);
            //在poll方法中需要传入一个timeout超时时间（单位毫秒），timeout超时时间用来控制poll方法的阻塞时间
            ConsumerRecords<String,String> records = consumer.poll(500);
            System.out.println("拉取到的消息个数：" + records.count());

            for(ConsumerRecord<String,String> record : records){
                System.out.println("topic:" + record.topic());
                System.out.println("partition:" + record.partition());
                System.out.println("offset:" + record.offset());
                System.out.println("value:" + record.value());
            }
        }

        //5）提交消费的消息偏移量（offset），默认consumer客户端每隔5秒自动提交offset

        //6）关闭消费者实例
        consumer.close();
    }

    /**
     * 按照分区处理消息，自动提交offset
     * @param topic 订阅的主题
     */
    public void pollMessageDealByPartition(String topic) throws InterruptedException {
        //1）配置consumer相关参数
        Properties props = initConfig();

        //2）创建Consumer实例
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(props);

        //3）订阅主题
        consumer.subscribe(Arrays.asList(topic));

        //4）拉取消息并消费
        while(isRunning.get()){
            Thread.sleep(2000);
            ConsumerRecords<String,String> records = consumer.poll(500);
            System.out.println("拉取到的消息个数：" + records.count());
            //按照分区处理消息
            for(TopicPartition partition : records.partitions()){
                System.out.println(">>>>>>>>> 正在处理的partition = " + partition.partition());
                //处理每个分区里的消息
                for(ConsumerRecord<String,String> record : records.records(partition)){
                    System.out.println("topic:" + record.topic());
                    System.out.println("partition:" + record.partition());
                    System.out.println("offset:" + record.offset());
                    System.out.println("value:" + record.value());
                }
            }
        }

        //5）提交消费的消息偏移量（offset），默认consumer客户端每隔5秒自动提交offset

        //6）关闭消费者实例
        consumer.close();
    }

    public static void main(String[] args) throws InterruptedException {
        String topic = "flumeselector";
        ConsumerClient consumer = new ConsumerClient();
        // 测试1：从订阅的主题中拉取消息消费，自动提交offset
//         consumer.pollMessage(topic);

        // 测试2：从订阅的主题中拉取消息，按照分区处理消息，自动提交offset
         consumer.pollMessageDealByPartition(topic);
    }
}
