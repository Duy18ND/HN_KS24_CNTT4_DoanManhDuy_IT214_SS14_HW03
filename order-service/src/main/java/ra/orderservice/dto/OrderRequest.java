package ra.orderservice.dto;

import lombok.Data;

@Data
public class OrderRequest {
    private Long customerId;
    private String customerName;
    private String product;
    private Integer quantity;
    private Double amount;
    private String shippingAddress;
}
