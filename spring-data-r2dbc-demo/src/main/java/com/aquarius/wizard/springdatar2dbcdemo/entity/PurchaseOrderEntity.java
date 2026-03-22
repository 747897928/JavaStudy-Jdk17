package com.aquarius.wizard.springdatar2dbcdemo.entity;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * 订单表实体。
 */
@Table("purchase_order")
public class PurchaseOrderEntity {

    @Id
    private Long id;

    private String orderNo;

    private String customerName;

    private String customerTier;

    private String skuCode;

    private Integer quantity;

    private BigDecimal unitPrice;

    private BigDecimal shippingFee;

    private String riskLevel;

    private String status;

    private String remark;

    private Instant createdAt;

    private Instant updatedAt;

    public PurchaseOrderEntity() {
    }

    public PurchaseOrderEntity(
            Long id,
            String orderNo,
            String customerName,
            String customerTier,
            String skuCode,
            Integer quantity,
            BigDecimal unitPrice,
            BigDecimal shippingFee,
            String riskLevel,
            String status,
            String remark,
            Instant createdAt,
            Instant updatedAt
    ) {
        this.id = id;
        this.orderNo = orderNo;
        this.customerName = customerName;
        this.customerTier = customerTier;
        this.skuCode = skuCode;
        this.quantity = quantity;
        this.unitPrice = unitPrice;
        this.shippingFee = shippingFee;
        this.riskLevel = riskLevel;
        this.status = status;
        this.remark = remark;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getOrderNo() {
        return orderNo;
    }

    public void setOrderNo(String orderNo) {
        this.orderNo = orderNo;
    }

    public String getCustomerName() {
        return customerName;
    }

    public void setCustomerName(String customerName) {
        this.customerName = customerName;
    }

    public String getCustomerTier() {
        return customerTier;
    }

    public void setCustomerTier(String customerTier) {
        this.customerTier = customerTier;
    }

    public String getSkuCode() {
        return skuCode;
    }

    public void setSkuCode(String skuCode) {
        this.skuCode = skuCode;
    }

    public Integer getQuantity() {
        return quantity;
    }

    public void setQuantity(Integer quantity) {
        this.quantity = quantity;
    }

    public BigDecimal getUnitPrice() {
        return unitPrice;
    }

    public void setUnitPrice(BigDecimal unitPrice) {
        this.unitPrice = unitPrice;
    }

    public BigDecimal getShippingFee() {
        return shippingFee;
    }

    public void setShippingFee(BigDecimal shippingFee) {
        this.shippingFee = shippingFee;
    }

    public String getRiskLevel() {
        return riskLevel;
    }

    public void setRiskLevel(String riskLevel) {
        this.riskLevel = riskLevel;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }
}
