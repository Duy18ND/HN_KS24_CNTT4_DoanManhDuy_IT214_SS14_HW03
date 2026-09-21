package ra.paymentservice.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ra.paymentservice.entity.Wallet;

import java.util.Optional;

public interface WalletRepository extends JpaRepository<Wallet, Long> {
    Optional<Wallet> findByCustomerId(Long customerId);
}
