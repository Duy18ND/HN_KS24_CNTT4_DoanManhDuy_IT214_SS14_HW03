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
public class CompensatePaymentEvent {
    private String eventId;
    private Long orderId;
    private Double amount;
    private String reason;
    private LocalDateTime timestamp;
}
