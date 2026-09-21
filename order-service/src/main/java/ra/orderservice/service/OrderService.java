package ra.orderservice.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ra.orderservice.dto.OrderRequest;
import ra.orderservice.entity.Order;
import ra.orderservice.entity.OrderStatus;
import ra.orderservice.event.*;
import ra.orderservice.repository.OrderRepository;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrderService {

    private final OrderRepository orderRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    
    // In-memory set to track processed events for idempotency
    private final ConcurrentHashMap<String, Boolean> processedEvents = new ConcurrentHashMap<>();

    @Transactional
    public Order createOrder(OrderRequest request) {
        Order order = Order.builder()
                .customerId(request.getCustomerId())
                .customerName(request.getCustomerName())
                .product(request.getProduct())
                .quantity(request.getQuantity())
                .amount(request.getAmount())
                .shippingAddress(request.getShippingAddress())
                .status(OrderStatus.PENDING)
                .createdAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .build();
                
        order = orderRepository.save(order);
        
        OrderCreatedEvent event = OrderCreatedEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .orderId(order.getOrderId())
                .customerId(order.getCustomerId())
                .amount(order.getAmount())
                .shippingAddress(order.getShippingAddress())
                .timestamp(LocalDateTime.now())
                .build();
                
        kafkaTemplate.send("order.created", String.valueOf(order.getOrderId()), event);
        log.info("Published OrderCreatedEvent for order ID: {}", order.getOrderId());
        
        return order;
    }

    @KafkaListener(topics = "payment.success", groupId = "order-group")
    @Transactional
    public void handlePaymentSuccess(PaymentSuccessEvent event) {
        if (processedEvents.putIfAbsent(event.getEventId(), true) != null) {
            log.info("Event {} already processed. Skipping.", event.getEventId());
            return;
        }

        Order order = orderRepository.findById(event.getOrderId()).orElse(null);
        if (order != null && order.getStatus() == OrderStatus.PENDING) {
            order.setStatus(OrderStatus.PAID);
            order.setUpdatedAt(LocalDateTime.now());
            orderRepository.save(order);
            log.info("Order {} status changed to PAID", order.getOrderId());

            ShippingRequestEvent shippingRequest = ShippingRequestEvent.builder()
                    .eventId(UUID.randomUUID().toString())
                    .orderId(order.getOrderId())
                    .shippingAddress(order.getShippingAddress())
                    .timestamp(LocalDateTime.now())
                    .build();
            kafkaTemplate.send("shipping.request", String.valueOf(order.getOrderId()), shippingRequest);
            log.info("Published ShippingRequestEvent for order ID: {}", order.getOrderId());
        }
    }

    @KafkaListener(topics = "payment.failed", groupId = "order-group")
    @Transactional
    public void handlePaymentFailed(PaymentFailedEvent event) {
        if (processedEvents.putIfAbsent(event.getEventId(), true) != null) return;

        Order order = orderRepository.findById(event.getOrderId()).orElse(null);
        if (order != null && order.getStatus() == OrderStatus.PENDING) {
            order.setStatus(OrderStatus.CANCELED);
            order.setUpdatedAt(LocalDateTime.now());
            orderRepository.save(order);
            log.info("Order {} status changed to CANCELED due to Payment Failed", order.getOrderId());
        }
    }

    @KafkaListener(topics = "shipping.success", groupId = "order-group")
    @Transactional
    public void handleShippingSuccess(ShippingSuccessEvent event) {
        if (processedEvents.putIfAbsent(event.getEventId(), true) != null) return;

        Order order = orderRepository.findById(event.getOrderId()).orElse(null);
        // Có thể ở trạng thái PAID (vừa gửi shipping request) hoặc SHIPPING (nếu có update status trung gian)
        if (order != null && (order.getStatus() == OrderStatus.PAID || order.getStatus() == OrderStatus.SHIPPING)) {
            order.setStatus(OrderStatus.COMPLETED);
            order.setUpdatedAt(LocalDateTime.now());
            orderRepository.save(order);
            log.info("Order {} status changed to COMPLETED", order.getOrderId());
        }
    }

    @KafkaListener(topics = "shipping.failed", groupId = "order-group")
    @Transactional
    public void handleShippingFailed(ShippingFailedEvent event) {
        if (processedEvents.putIfAbsent(event.getEventId(), true) != null) return;

        Order order = orderRepository.findById(event.getOrderId()).orElse(null);
        if (order != null && (order.getStatus() == OrderStatus.PAID || order.getStatus() == OrderStatus.SHIPPING)) {
            order.setStatus(OrderStatus.COMPENSATING);
            order.setUpdatedAt(LocalDateTime.now());
            orderRepository.save(order);
            log.info("Order {} status changed to COMPENSATING. Sending CompensatePaymentEvent.", order.getOrderId());

            CompensatePaymentEvent compensate = CompensatePaymentEvent.builder()
                    .eventId(UUID.randomUUID().toString())
                    .orderId(order.getOrderId())
                    .amount(order.getAmount())
                    .reason(event.getReason())
                    .timestamp(LocalDateTime.now())
                    .build();
            kafkaTemplate.send("payment.compensate", String.valueOf(order.getOrderId()), compensate);
        }
    }

    @KafkaListener(topics = "payment.refund.success", groupId = "order-group")
    @Transactional
    public void handleRefundSuccess(RefundSuccessEvent event) {
        if (processedEvents.putIfAbsent(event.getEventId(), true) != null) return;

        Order order = orderRepository.findById(event.getOrderId()).orElse(null);
        if (order != null && order.getStatus() == OrderStatus.COMPENSATING) {
            order.setStatus(OrderStatus.CANCELED);
            order.setUpdatedAt(LocalDateTime.now());
            orderRepository.save(order);
            log.info("Order {} status changed to CANCELED due to successful refund", order.getOrderId());
        }
    }

    @KafkaListener(topics = "payment.refund.failed", groupId = "order-group")
    @Transactional
    public void handleRefundFailed(RefundFailedEvent event) {
        if (processedEvents.putIfAbsent(event.getEventId(), true) != null) return;

        Order order = orderRepository.findById(event.getOrderId()).orElse(null);
        if (order != null && order.getStatus() == OrderStatus.COMPENSATING) {
            order.setStatus(OrderStatus.COMPENSATION_FAILED);
            order.setUpdatedAt(LocalDateTime.now());
            orderRepository.save(order);
            log.error("Order {} status changed to COMPENSATION_FAILED. Require manual intervention.", order.getOrderId());
        }
    }
}
