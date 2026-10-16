package com.coding.kfkconsumer;

import com.coding.kfkconsumer.Consumer.KafkaMessageListener;
import com.coding.kfkconsumer.dto.Customer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class KfkconsumerApplicationTests {

	@DynamicPropertySource
	public static void initKafkaProperties(DynamicPropertyRegistry registry){
		// 1. Conexión del test al puerto real
		registry.add("spring.kafka.bootstrap-servers", () -> "localhost:9092");

		// 2. Serializadores para el envío manual del test
		registry.add("spring.kafka.producer.key-serializer", () -> "org.apache.kafka.common.serialization.StringSerializer");
		registry.add("spring.kafka.producer.value-serializer", () -> "org.springframework.kafka.support.serializer.JsonSerializer");

		// 3. Deserializadores y Paquetes de Confianza
		registry.add("spring.kafka.consumer.key-deserializer", () -> "org.apache.kafka.common.serialization.StringDeserializer");
		registry.add("spring.kafka.consumer.value-deserializer", () -> "org.springframework.kafka.support.serializer.JsonDeserializer");
		registry.add("spring.kafka.consumer.properties.spring.json.trusted.packages", () -> "*");
		registry.add("spring.kafka.consumer.properties.spring.json.value.default.type", () -> "com.coding.kfkconsumer.dto.Customer");
		registry.add("spring.kafka.consumer.properties.spring.json.use.type.headers", () -> "false");

		// 4. EL TRUCO DE REINICIO DE OFFSETS (Para que lea lo que enviamos de inmediato) 👈
		registry.add("spring.kafka.consumer.auto-offset-reset", () -> "earliest");
		registry.add("spring.kafka.consumer.group-id", () -> "test-group-dinamico-" + System.currentTimeMillis()); // Evita choques con grupos viejos
	}

	@Autowired
	private KafkaMessageListener consumer;

	@Autowired
	private KafkaTemplate<String, Object> kafkaTemplate; // Lo usamos para simular un envío al tópico

	@Test
	public void testReceiveEventsFromTopic() throws InterruptedException { // 1. Agrega throws InterruptedException

		// 2. EL TRUCO DE SINCRONIZACIÓN: Esperar 3 segundos a que el @KafkaListener se conecte bien a Kafka 👈
		Thread.sleep(3000);

		// 3. Preparamos el objeto simulado
		Customer testCustomer = new Customer(777, "Mi Cops Test", "cops@gmail.com", "12345678");

		// 4. Enviamos el mensaje al tópico (Ahora el listener sí estará escuchando activo)
		kafkaTemplate.send("mi-topico", testCustomer);


		await()
				.pollInterval(Duration.ofSeconds(1))
				.atMost(Duration.ofSeconds(5))
				.untilAsserted(() -> {

					assertThat(consumer.getMensajesRecibidos()).isNotEmpty();
					System.out.println("¡DATO REAL ATRAPADO EN EL TEST!: " + consumer.getMensajesRecibidos().get(0).getName());
				});

	}
}
