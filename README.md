# sparkStreamingKafkaDemo
This project is based on kafka 0.10.0.1 (or higher), and spark 2.2.0 (or higher.) 
It uses the new Kafka producer API to generate kafka Producer Records; 
It uses the spark-streaming-kafka-0-10_2.11 to create Direct Kafka Stream.

To produce records into KAFKA topic: 
  java -cp ./sparkStreamingDemo-1.0-SNAPSHOT-jar-with-dependencies.jar com.hpeu.sparkStreaming.test.NewKafkaProducerTest topicName

To consume records from KAFKA topic: 
  spark-submit --master spark://node1:7077 --class com.hpeu.sparkStreaming.test.DirectKafkaConsumerTest ./sparkStreamingDemo-1.0-SNAPSHOT-jar-with-dependencies.jar node1:9092 topicName consumerGroupName earliest/latest/none SIGNAL3
