package com.coding.kfkproducer.Controller;

import com.coding.kfkproducer.Service.KafkaMessagePublisher;
import com.coding.kfkproducer.dto.Customer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/producer-app")
public class EventController {

    @Autowired
    KafkaMessagePublisher publisher;

    /*
    @PostMapping("/publish") // Ahora sí es POST y no lleva /{message}
    public ResponseEntity<?> publishMessage(@RequestBody String message){ // Con RequestBody
        try {
            publisher.sendMessageToTopic(message);
            return ResponseEntity.ok("Mensaje enviado con éxito vía POST.");
        } catch (Exception ex) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ex.getMessage());
        }
    }
    */

    @PostMapping("/publish")
    public void sendEvents(@RequestBody Customer customer){
        publisher.sendEventsToTopic(customer);
    }
}

























