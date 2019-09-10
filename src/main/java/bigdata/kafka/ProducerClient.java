package bigdata.kafka;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerClient {
    //broker列表
    public static final String brokers = "10.192.208.176:9092, 10.193.21.244:9092, 10.192.208.130:9092";
    //producer要发送的主题名称
    public static final String topic = "topic_producer_client1";

    /**
     * 初始化配置kafka producer客户端相关参数
     * @return
     *  Properties对象
     */
    public static Properties initConfig(){
        Properties props = new Properties();
        //设置bootstrap.servers参数值，producer客户端连接的borker地址列表
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);

        //防止参数的键和值由于人为原因写错，可以使用kafka客户端内置的工具类进行优化

        //设置key和value使用String类型的序列化器

        //props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //下一行代码是等价设置
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //下一行代码是等价设置
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //（可选项）设置客户端等待接收Broker返回成功送达信号的方式
        //props.put("acks","1");
        //下一行代码是等价设置
        props.put(ProducerConfig.ACKS_CONFIG,"1");

        //（可选项）设置客户端发送消息失败后的重试次数
        props.put(ProducerConfig.RETRIES_CONFIG,"3");

        //（可选项）设置producer客户端id
        props.put(ProducerConfig.CLIENT_ID_CONFIG,"producer.client.id.test1");

        return props;
    }


    /**
     * key为空的情况，producer客户端向kafka指定主题发送value
     * 客户端发送消息方式：异步，无回调函数
     * @param topic ：主题名称
     * @param value ：发送的value值
     */
    public void sendMessage(String topic,String value){
        //1）配置Producer相关参数
        Properties props = initConfig();
        //2）创建Producer实例
        KafkaProducer<String, String> producer = new KafkaProducer<String,String>(props);
        //3）构建待发送的消息ProducerRecord，ProducerRecord<String, String>泛型中的两个String类型指的是Key和Value的类型
        ProducerRecord record = new ProducerRecord<String, String>(topic,value);
        //4）发送消息
        producer.send(record);//send方法默认是异步发送消息
        //5）关闭Producer实例
        producer.close();
    }

    /**
     * key为空的情况，producer客户端向kafka指定主题发送value
     * 客户端发送消息方式：异步，有回调函数
     * @param topic ：主题名称
     * @param value ：发送的value值
     */
    public void sendMessageCallback(String topic,String value){
        //1）配置Producer相关参数
        Properties props = initConfig();
        //2）创建Producer实例
        KafkaProducer<String, String> producer = new KafkaProducer<String,String>(props);
        //3）构建待发送的消息ProducerRecord，ProducerRecord<String, String>泛型中的两个String类型指的是Key和Value的类型
        ProducerRecord record = new ProducerRecord<String, String>(topic,value);
        //4）发送消息
        //异步发送消息，通过自定义回调函数处理消息发送后等待服务端响应
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(e != null){
                    //如果发送消息发生异常，打印出异常信息
                    e.printStackTrace();
                }else{
                    //打印发送成功的消息的元数据信息
                    System.out.println("topic:" + recordMetadata.topic());
                    System.out.println("partition:" + recordMetadata.partition());//消息被发送到的分区号
                    System.out.println("offset:" + recordMetadata.offset());//消息在分区中的偏移量
                }
            }
        });

        //5）关闭Producer实例
        producer.close();
    }

    /**
     * key不为空的情况，producer客户端向kafka指定主题发送value
     * 客户端发送消息方式：异步
     * @param topic ：主题名称
     * @param key ：发送的key值
     * @param value ：发送的value值
     */
    public void sendMessage(String topic,String key,String value){
        Properties props = initConfig();
        KafkaProducer<String, String> producer = new KafkaProducer<String,String>(props);
        ProducerRecord record = new ProducerRecord<String, String>(topic,key,value);
        producer.send(record);
        producer.close();
    }

    /**
     * key不为空的情况，producer客户端向kafka指定主题发送value
     * 客户端发送消息方式：同步
     * @param topic ：主题名称
     * @param key ：发送的key值
     * @param value ：发送的value值
     */
    public void sendMessageSync(String topic,String key,String value){
        Properties props = initConfig();
        KafkaProducer<String, String> producer = new KafkaProducer<String,String>(props);
        ProducerRecord record = new ProducerRecord<String, String>(topic,key,value);
        try {
            //在调用完send方法后链式地调用get方法，会同步阻塞，等待消息发送成功或者发生异常
            producer.send(record).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        producer.close();
    }

    public static void main(String[] args) {
        ProducerClient producer = new ProducerClient();

        //调用不需要传key的发送消息的方法
        producer.sendMessage(topic,"hello kafka 1"); //异步发送消息，没有设置回调函数
        producer.sendMessageCallback(topic,"hello kafka callback1"); //异步发送消息，并设置回调函数

        //调用传入自定义key值的发送消息的方法
        producer.sendMessage(topic,"key2","hello kafka 2");//异步发送消息
        producer.sendMessageSync(topic,"key3","hello kafka sync 2");//同步发送消息
    }
}
