package ra.shippingservice.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ShippingSuccessEvent {
    private String eventId;
    private Long orderId;
    private Long shipmentId;
    private LocalDateTime timestamp;
}
