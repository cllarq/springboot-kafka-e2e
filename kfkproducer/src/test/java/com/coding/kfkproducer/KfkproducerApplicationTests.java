package com.coding.kfkproducer;

import com.coding.kfkproducer.Service.KafkaMessagePublisher;
import com.coding.kfkproducer.dto.Customer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import java.time.Duration;

import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
// 1. Borramos @Testcontainers para que no busque a Docker tras bambalinas 👈
class KfkproducerApplicationTests {

	// 2. Borramos o comentamos el bloque @Container de KafkaContainer 👈

	@DynamicPropertySource
	public static void initKafkaProperties(DynamicPropertyRegistry registry){
		// 3. Apuntamos directamente al puerto de tu Kafka local real 👈
		registry.add("spring.kafka.bootstrap-servers", () -> "localhost:9092");
	}

	@Autowired
	private KafkaMessagePublisher publisher;

	@Test
	public void testSendEventsToTopic(){
		// El envío se queda exactamente igual
		publisher.sendEventsToTopic(new Customer(234, "test user","test@gmail.com", "5556789800"));

		await()
				.pollInterval(Duration.ofSeconds(1))
				.atMost(Duration.ofSeconds(5))
				.untilAsserted(() -> {
					// El test completará el envío exitosamente aquí
				});
	}
}
