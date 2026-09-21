package ra.paymentservice.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OrderCreatedEvent {
    private String eventId;
    private Long orderId;
    private Long customerId;
    private Double amount;
    private String shippingAddress;
    private LocalDateTime timestamp;
}
