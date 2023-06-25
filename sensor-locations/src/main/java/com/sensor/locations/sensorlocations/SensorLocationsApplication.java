package com.sensor.locations.sensorlocations;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SpringBootApplication
public class SensorLocationsApplication {
	private static final Logger logger = LoggerFactory.getLogger(SensorLocationsApplication.class);
	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@Value("${kafka.topic}")
	private String topic;

	public static void main(String[] args) {
		SpringApplication.run(SensorLocationsApplication.class, args).getBean(SensorLocationsApplication.class).sendMessage();
	}

	@Bean
	public ProducerFactory<String, String> producerFactory(@Value("${kafka.bootstrap.servers}") String bootstrapServers) {
		Map<String, Object> configProps = new HashMap<>();
		configProps.put("bootstrap.servers", bootstrapServers);
		configProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		configProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		return new DefaultKafkaProducerFactory<>(configProps);
	}

	@Bean
	public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
		return new KafkaTemplate<>(producerFactory);
	}

	public void sendMessage() {
		Scanner scanner = new Scanner(System.in);

		int xMin =-1000;
		int xMax = 1000;
		int yMin = -1000;
		int yMax = 1000;

		String sensor1Location;
		String sensor2Location;
		String aim;

		do {
			System.out.print("Enter sensor1currentLocation value (x,y): ");
			sensor1Location = scanner.nextLine();
		} while (!isValidLocation(sensor1Location, xMin, xMax, yMin, yMax));

		do {
			System.out.print("Enter sensor2currentLocation value (x,y): ");
			sensor2Location = scanner.nextLine();
		} while (!isValidLocation(sensor2Location, xMin, xMax, yMin, yMax));

		do {
			System.out.print("Enter aim value (x,y): ");
			aim = scanner.nextLine();
		} while (!isValidLocation(aim, xMin, xMax, yMin, yMax));

		String payload = "{\"sensor1currentLocation\": \"" + sensor1Location + "\", " +
				"\"sensor2currentLocation\": \"" + sensor2Location + "\", " +
				"\"aim\": \"" + aim + "\"}";
		ProducerRecord<String, String> record = new ProducerRecord<>(topic, payload);
		kafkaTemplate.send(record);
		logger.info("Payload sent successfully!");
	}

	private boolean isValidLocation(String location, int xMin, int xMax, int yMin, int yMax) {
		if (!location.matches("\\((-?\\d+),(-?\\d+)\\)")) {
			logger.info("Invalid location format. Please use (x,y) format.");
			return false;
		}

		String[] coordinates = location.substring(1, location.length() - 1).split(",");
		int x = Integer.parseInt(coordinates[0]);
		int y = Integer.parseInt(coordinates[1]);

		if (x < xMin || x > xMax || y < yMin || y > yMax) {
			logger.info("Location coordinates are out of range. Please enter coordinates within the 1000x1000 area.");
			return false;
		}

		return true;
	}
}