-- fact_payments
{{ config(materialized='table') }}

SELECT 
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value,
    -- Categorizar tipos de pagamento
    CASE 
        WHEN payment_type = 'credit_card' THEN 'Credit Card'
        WHEN payment_type = 'boleto' THEN 'Bank Slip'
        WHEN payment_type = 'voucher' THEN 'Voucher'
        WHEN payment_type = 'debit_card' THEN 'Debit Card'
        ELSE 'Other'
    END as payment_type_clean,
    
    -- Categorizar parcelamento
    CASE 
        WHEN payment_installments = 1 THEN 'Single Payment'
        WHEN payment_installments BETWEEN 2 AND 6 THEN 'Short Term (2-6x)'
        WHEN payment_installments BETWEEN 7 AND 12 THEN 'Medium Term (7-12x)'
        WHEN payment_installments > 12 THEN 'Long Term (12x+)'
        ELSE 'Unknown'
    END as installment_category,
    
    CURRENT_TIMESTAMP as created_at
FROM {{ source('raw_data', 'olist_order_payments_dataset') }}