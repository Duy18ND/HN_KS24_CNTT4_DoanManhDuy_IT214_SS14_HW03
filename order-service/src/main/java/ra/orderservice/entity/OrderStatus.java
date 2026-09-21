package ra.orderservice.entity;

/**
 * Mục đích: Quản lý trạng thái của Order theo State Machine
 */
public enum OrderStatus {
    PENDING,
    PAID,
    SHIPPING,
    COMPLETED,
    CANCELED,
    COMPENSATING,
    COMPENSATION_FAILED
}
