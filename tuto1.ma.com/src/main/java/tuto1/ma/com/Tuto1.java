package tuto1.ma.com;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class Tuto1 {

	 private static final Logger log = LoggerFactory.getLogger(Tuto1.class);

	public static void main(String[] args) {
		
		//definir les proprietes
		Properties prop=new Properties();
		prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG ,StringSerializer.class.getName());
		prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG ,StringSerializer.class.getName());
		
		//creer un producteur
		KafkaProducer<String, String> producer=new KafkaProducer<String, String>(prop);
		//creer un record
		
			
		//envoi
//		for(int i=0;i<10;i++)
//		{
//			
			final ProducerRecord<String, String> record=new ProducerRecord<String, String>("mytopicatos2","key3", "atos -"+2);
			
		    producer.send(record, new Callback() {
			
			public void onCompletion(RecordMetadata metadata, Exception e) {
				if(e==null)
				{
					//System.out.println(metadata.topic()+"---"+metadata.offset()+"----"+metadata.partition()+"----");
					log.info("key:"+ record.key()+"---offset:"+metadata.offset()+"-partition: "+metadata.partition());
				}
				
			}
		});
		    
//				try {
//					Thread.sleep(1000);
//				} catch (InterruptedException e1) {
//					// TODO Auto-generated catch block
//					e1.printStackTrace();
//				}
			
		
		//}
		//fermer
		producer.flush();
		producer.close();
	}

}
