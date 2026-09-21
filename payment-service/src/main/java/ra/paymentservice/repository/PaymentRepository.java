package ra.paymentservice.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ra.paymentservice.entity.Payment;

import java.util.Optional;

public interface PaymentRepository extends JpaRepository<Payment, Long> {
    Optional<Payment> findByOrderId(Long orderId);
}
