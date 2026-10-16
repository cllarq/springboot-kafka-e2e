package com.coding.kfkconsumer.Consumer;

import com.coding.kfkconsumer.dto.Customer;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class KafkaMessageListener {

   // private static final Logger logger = LoggerFactory.getLogger(KafkaMessageListener.class);
   private final List<Customer> mensajesRecibidos = new ArrayList<>();

    @KafkaListener(topics = "mi-topico", groupId = "jt-group-1")
    public void escuchar(Customer customer) {
        System.out.println("Consumer recibió con éxito: " + customer);
        mensajesRecibidos.add(customer); // Lo guardamos para el test
    }

    public List<Customer> getMensajesRecibidos() {
        return mensajesRecibidos;
    }
}
