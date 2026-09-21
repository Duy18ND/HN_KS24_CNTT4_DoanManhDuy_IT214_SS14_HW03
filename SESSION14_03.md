# BÀI TẬP 3: THIẾT KẾ VŨ ĐIỆU CHOREOGRAPHY SAGA

## 1. PHÂN TÍCH INPUT
Quy trình nghiệp vụ này nhận các thông tin đầu vào:

**Customer:**
- `customerId`: ID của khách hàng đặt hàng (VD: 100)
- `customerName`: Tên của khách hàng

**Order:**
- `orderId`: ID của đơn hàng (VD: 1)
- `product`: Sản phẩm đặt mua
- `quantity`: Số lượng
- `amount`: Tổng số tiền đơn hàng (VD: 500000)

**Payment:**
- `wallet/account`: Tài khoản người dùng
- `availableBalance`: Số dư hiện tại có thể dùng để thanh toán

**Shipping:**
- `shippingAddress`: Địa chỉ giao hàng (VD: Hanoi)
- `supportedAddress`: Các địa chỉ được hệ thống hỗ trợ giao hàng

## 2. PHÂN TÍCH OUTPUT
Sau khi chạy hết quy trình, hệ thống có thể kết thúc ở một trong hai nhánh trạng thái cuối cùng:

**LUỒNG THÀNH CÔNG:**
- **Order:** `COMPLETED`
- **Payment:** `PAID`
- **Shipping:** `SHIPPED`

**LUỒNG THẤT BẠI (Bù trừ / Compensation):**
- **Order:** `CANCELED`
- **Payment:** `REFUNDED`
- **Shipping:** `FAILED`

## 3. EVENT TOPICS

Bảng danh sách các Event sử dụng qua Kafka:

| Event | Kafka Topic | Producer | Consumer |
|---|---|---|---|
| OrderCreated | order.created | Order | Payment |
| PaymentSuccess | payment.success | Payment | Order |
| PaymentFailed | payment.failed | Payment | Order |
| ShippingRequest | shipping.request | Order | Shipping |
| ShippingSuccess | shipping.success | Shipping | Order |
| ShippingFailed | shipping.failed | Shipping | Order |
| CompensatePayment | payment.compensate | Order | Payment |
| RefundSuccess | payment.refund.success | Payment | Order |
| RefundFailed | payment.refund.failed | Payment | Order |

## 4. STATE MACHINE
Sơ đồ trạng thái của Order:

```mermaid
stateDiagram-v2

    [*] --> PENDING

    PENDING --> PAID:
        PaymentSuccess

    PENDING --> CANCELED:
        PaymentFailed

    PAID --> SHIPPING:
        ShippingRequest

    SHIPPING --> COMPLETED:
        ShippingSuccess

    SHIPPING --> COMPENSATING:
        ShippingFailed

    SHIPPING --> COMPENSATING:
        Timeout > 30s

    COMPENSATING --> CANCELED:
        RefundSuccess

    COMPENSATING --> COMPENSATION_FAILED:
        RefundFailed

    COMPLETED --> [*]

    CANCELED --> [*]
```
