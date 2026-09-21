package ra.orderservice.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ra.orderservice.entity.Order;
import ra.orderservice.entity.OrderStatus;

import java.util.List;

public interface OrderRepository extends JpaRepository<Order, Long> {
    List<Order> findByStatus(OrderStatus status);
}
