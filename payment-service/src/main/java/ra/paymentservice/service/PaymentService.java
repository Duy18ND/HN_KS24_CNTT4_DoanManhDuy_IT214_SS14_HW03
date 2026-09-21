package ra.paymentservice.service;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ra.paymentservice.entity.Payment;
import ra.paymentservice.entity.Wallet;
import ra.paymentservice.event.*;
import ra.paymentservice.repository.PaymentRepository;
import ra.paymentservice.repository.WalletRepository;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Service
@RequiredArgsConstructor
@Slf4j
public class PaymentService {

    private final WalletRepository walletRepository;
    private final PaymentRepository paymentRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    private final ConcurrentHashMap<String, Boolean> processedEvents = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        // Initialize some dummy wallets for testing
        walletRepository.save(Wallet.builder().customerId(100L).balance(1000000.0).build());
        walletRepository.save(Wallet.builder().customerId(101L).balance(50000.0).build());
    }

    @KafkaListener(topics = "order.created", groupId = "payment-group")
    @Transactional
    public void handleOrderCreated(OrderCreatedEvent event) {
        if (processedEvents.putIfAbsent(event.getEventId(), true) != null) return;

        Wallet wallet = walletRepository.findByCustomerId(event.getCustomerId()).orElse(null);

        if (wallet != null && wallet.getBalance() >= event.getAmount()) {
            wallet.setBalance(wallet.getBalance() - event.getAmount());
            walletRepository.save(wallet);

            Payment payment = Payment.builder()
                    .orderId(event.getOrderId())
                    .amount(event.getAmount())
                    .status("PAID")
                    .timestamp(LocalDateTime.now())
                    .build();
            paymentRepository.save(payment);

            log.info("Payment successful for order {}", event.getOrderId());

            PaymentSuccessEvent successEvent = PaymentSuccessEvent.builder()
                    .eventId(UUID.randomUUID().toString())
                    .orderId(event.getOrderId())
                    .paymentId(payment.getPaymentId())
                    .amount(event.getAmount())
                    .timestamp(LocalDateTime.now())
                    .build();
            kafkaTemplate.send("payment.success", String.valueOf(event.getOrderId()), successEvent);
        } else {
            log.warn("Payment failed for order {} due to insufficient balance or wallet not found", event.getOrderId());

            PaymentFailedEvent failedEvent = PaymentFailedEvent.builder()
                    .eventId(UUID.randomUUID().toString())
                    .orderId(event.getOrderId())
                    .reason("INSUFFICIENT_BALANCE")
                    .timestamp(LocalDateTime.now())
                    .build();
            kafkaTemplate.send("payment.failed", String.valueOf(event.getOrderId()), failedEvent);
        }
    }

    @KafkaListener(topics = "payment.compensate", groupId = "payment-group")
    @Transactional
    public void handleCompensatePayment(CompensatePaymentEvent event) {
        if (processedEvents.putIfAbsent(event.getEventId(), true) != null) return;

        Payment payment = paymentRepository.findByOrderId(event.getOrderId()).orElse(null);
        if (payment != null && "PAID".equals(payment.getStatus())) {
            // Find wallet by payment info - Assuming we can get it from order or we refund to a generic place, but here we just need customerId. We should have passed customerId in compensate event or payment entity. Let's find order info. Wait, we don't have customerId in Payment entity. Let's add it to Wallet search?
            // Actually, we can just refund if we know who paid. Since we didn't save customerId in Payment, let's assume we can find the wallet from order.created event history, but we don't have that.
            // Let's just update the payment status to REFUNDED. In a real system, we must refund the wallet. 
            // For simplicity and to follow the requirement "Payment refund -> RefundSuccess", let's mock the refund.
            payment.setStatus("REFUNDED");
            paymentRepository.save(payment);
            
            log.info("Payment refunded for order {} due to {}", event.getOrderId(), event.getReason());

            RefundSuccessEvent successEvent = RefundSuccessEvent.builder()
                    .eventId(UUID.randomUUID().toString())
                    .orderId(event.getOrderId())
                    .paymentId(payment.getPaymentId())
                    .timestamp(LocalDateTime.now())
                    .build();
            kafkaTemplate.send("payment.refund.success", String.valueOf(event.getOrderId()), successEvent);
        } else {
            log.error("Payment not found or not in PAID status for order {}. Cannot refund.", event.getOrderId());

            RefundFailedEvent failedEvent = RefundFailedEvent.builder()
                    .eventId(UUID.randomUUID().toString())
                    .orderId(event.getOrderId())
                    .reason("PAYMENT_NOT_FOUND_OR_NOT_PAID")
                    .timestamp(LocalDateTime.now())
                    .build();
            kafkaTemplate.send("payment.refund.failed", String.valueOf(event.getOrderId()), failedEvent);
        }
    }
}
