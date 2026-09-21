package ra.shippingservice.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ra.shippingservice.entity.Shipment;

public interface ShipmentRepository extends JpaRepository<Shipment, Long> {
}
