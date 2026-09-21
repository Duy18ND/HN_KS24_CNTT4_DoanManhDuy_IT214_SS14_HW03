package ra.orderservice.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class RefundFailedEvent {
    private String eventId;
    private Long orderId;
    private String reason;
    private LocalDateTime timestamp;
}
