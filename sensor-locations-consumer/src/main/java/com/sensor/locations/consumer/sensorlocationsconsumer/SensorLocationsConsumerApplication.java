package com.sensor.locations.consumer.sensorlocationsconsumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@SpringBootApplication
public class SensorLocationsConsumerApplication {

	private static final Logger logger = LoggerFactory.getLogger(SensorLocationsConsumerApplication.class);

	@Value("${kafka.bootstrap.servers}")
	private String bootstrapServers;

	@Value("${kafka.topic}")
	private String topic;

	@Value("${kafka.group.id}")
	private String groupId;

	@Bean
	public Consumer<String, String> kafkaConsumer() {
		Properties props = new Properties();
		props.put("bootstrap.servers", bootstrapServers);
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", StringDeserializer.class.getName());
		props.put("group.id", groupId);

		return new KafkaConsumer<>(props);
	}

	public void runConsumer() {
		Consumer<String, String> consumer = kafkaConsumer();
		consumer.subscribe(Collections.singletonList(topic));

		try {
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));

				records.forEach(record -> {
					String payload = record.value();
					double angle1 = calculateAngle(payload, "sensor1currentLocation");
					double angle2 = calculateAngle(payload, "sensor2currentLocation");
					logger.info("Sensor1 için hedefin kerterizi Y pozitif ekseninde saat yönündeki açısı: " + angle1 + " derece");
					logger.info("Sensor2 için hedefin kerterizi Y pozitif ekseninde saat yönündeki açısı: " + angle2 + " derece");
				});
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			consumer.close();
		}
	}

	private double calculateAngle(String payload, String sensorLocation) {
		String location = extractLocation(payload, sensorLocation);
		double x = Double.parseDouble(location.split(",")[0].trim());
		double y = Double.parseDouble(location.split(",")[1].trim());
		double aimX = Double.parseDouble(extractLocation(payload, "aim").split(",")[0].trim());
		double aimY = Double.parseDouble(extractLocation(payload, "aim").split(",")[1].trim());
		return Math.toDegrees(Math.atan2(aimX - x, aimY - y));
	}

	private String extractLocation(String payload, String sensorLocation) {
		String locationStart = "\"" + sensorLocation + "\": \"";
		int start = payload.indexOf(locationStart) + locationStart.length();
		int end = payload.indexOf("\"", start);
		return payload.substring(start, end);
	}

	public static void main(String[] args) {
		SpringApplication.run(SensorLocationsConsumerApplication.class, args).getBean(SensorLocationsConsumerApplication.class).runConsumer();
	}
}