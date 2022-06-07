package tuto1.ma.com;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class consumer1 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		//definir les proprietes
				Properties prop=new Properties();
				prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
				prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ,StringDeserializer.class.getName());
				prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ,StringDeserializer.class.getName());
				prop.put(ConsumerConfig.GROUP_ID_CONFIG,"grp1");
				prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
				
				
				KafkaConsumer<String, String> consumer=new KafkaConsumer<String, String>(prop);
				
				consumer.subscribe(Arrays.asList("mytopicatos2"));
				
				while(true)
				{
					ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
					
					for (ConsumerRecord<String, String> r : records) {
						System.out.println(r.key()+"   :   "+r.value()+"    offsets:"+r.offset()+ "----"+r.partition());
					}
					
				}
				
				
				
				
				
				
				
				
				
				
				
				

	}

}
