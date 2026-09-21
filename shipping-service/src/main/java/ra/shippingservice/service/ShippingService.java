package ra.shippingservice.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ra.shippingservice.entity.Shipment;
import ra.shippingservice.event.ShippingFailedEvent;
import ra.shippingservice.event.ShippingRequestEvent;
import ra.shippingservice.event.ShippingSuccessEvent;
import ra.shippingservice.repository.ShipmentRepository;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Service
@RequiredArgsConstructor
@Slf4j
public class ShippingService {

    private final ShipmentRepository shipmentRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    private final ConcurrentHashMap<String, Boolean> processedEvents = new ConcurrentHashMap<>();
    
    // Supported addresses
    private final List<String> supportedAddresses = Arrays.asList("Hanoi", "HCM", "DaNang");

    @KafkaListener(topics = "shipping.request", groupId = "shipping-group")
    @Transactional
    public void handleShippingRequest(ShippingRequestEvent event) {
        if (processedEvents.putIfAbsent(event.getEventId(), true) != null) return;

        if (supportedAddresses.contains(event.getShippingAddress())) {
            Shipment shipment = Shipment.builder()
                    .orderId(event.getOrderId())
                    .shippingAddress(event.getShippingAddress())
                    .status("SHIPPED")
                    .timestamp(LocalDateTime.now())
                    .build();
            shipment = shipmentRepository.save(shipment);

            log.info("Shipping created for order {}", event.getOrderId());

            ShippingSuccessEvent successEvent = ShippingSuccessEvent.builder()
                    .eventId(UUID.randomUUID().toString())
                    .orderId(event.getOrderId())
                    .shipmentId(shipment.getShipmentId())
                    .timestamp(LocalDateTime.now())
                    .build();
            kafkaTemplate.send("shipping.success", String.valueOf(event.getOrderId()), successEvent);
        } else {
            log.warn("Address {} not supported for order {}", event.getShippingAddress(), event.getOrderId());

            ShippingFailedEvent failedEvent = ShippingFailedEvent.builder()
                    .eventId(UUID.randomUUID().toString())
                    .orderId(event.getOrderId())
                    .reason("ADDRESS_NOT_SUPPORTED")
                    .timestamp(LocalDateTime.now())
                    .build();
            kafkaTemplate.send("shipping.failed", String.valueOf(event.getOrderId()), failedEvent);
        }
    }
}
