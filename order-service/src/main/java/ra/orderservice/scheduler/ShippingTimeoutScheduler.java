package ra.orderservice.scheduler;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ra.orderservice.entity.Order;
import ra.orderservice.entity.OrderStatus;
import ra.orderservice.event.CompensatePaymentEvent;
import ra.orderservice.repository.OrderRepository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Component
@RequiredArgsConstructor
@Slf4j
public class ShippingTimeoutScheduler {

    private final OrderRepository orderRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${saga.shipping.timeout-seconds:30}")
    private long timeoutSeconds;

    @Scheduled(fixedDelayString = "${saga.shipping.scheduler-delay:5000}")
    @Transactional
    public void checkShippingTimeouts() {
        LocalDateTime thresholdTime = LocalDateTime.now().minusSeconds(timeoutSeconds);

        List<Order> paidOrders = orderRepository.findByStatus(OrderStatus.PAID);
        List<Order> shippingOrders = orderRepository.findByStatus(OrderStatus.SHIPPING);

        paidOrders.addAll(shippingOrders);

        for (Order order : paidOrders) {
            if (order.getUpdatedAt().isBefore(thresholdTime)) {
                log.warn("Order {} shipping timeout exceeded ({} seconds). Triggering compensation.", order.getOrderId(), timeoutSeconds);

                order.setStatus(OrderStatus.COMPENSATING);
                order.setUpdatedAt(LocalDateTime.now());
                orderRepository.save(order);

                CompensatePaymentEvent compensate = CompensatePaymentEvent.builder()
                        .eventId(UUID.randomUUID().toString())
                        .orderId(order.getOrderId())
                        .amount(order.getAmount())
                        .reason("SHIPPING_TIMEOUT")
                        .timestamp(LocalDateTime.now())
                        .build();
                kafkaTemplate.send("payment.compensate", String.valueOf(order.getOrderId()), compensate);
            }
        }
    }
}
