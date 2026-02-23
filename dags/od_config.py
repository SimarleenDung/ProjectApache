GCS_BUCKET_NAME = "olist_project_rs"
 
GCS_FILE_TABLE_CONFIG = [
    {
        "gcs_path": "landing/olist_products_dataset.csv",
        "table": "PRODUCTS_OD",
        "merge_key": "product_id",
        "columns": [
            "product_id",
            "product_category_name",
            "product_name_lenght",
            "product_description_lenght",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm"
        ]
    },
    {
        "gcs_path": "landing/olist_sellers_dataset.csv",
        "table": "SELLERS_OD",
        "merge_key": "seller_id",
        "columns": [
            "seller_id",
            "seller_zip_code_prefix",
            "seller_city",
            "seller_state"
        ]
    },
    {
    "gcs_path": "landing/olist_customers_dataset.csv",
    "table": "CUSTOMER_OD",
    "merge_key": "customer_id",
    "columns": [
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state"
        ]
    },
    {
        "gcs_path": "landing/olist_orders_dataset.csv",
        "table": "ORDERS_OD",
        "merge_key": "order_id",
        "columns": [
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date"
        ]
    },
    {
        "gcs_path": "landing/olist_order_items_dataset.csv",
        "table": "ORDERITEM_OD",
        "merge_key": "order_id",  # composite in reality (order_id + order_item_id)
        "columns": [
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "shipping_limit_date",
            "price",
            "freight_value"
        ]
    },
    {
        "gcs_path": "landing/olist_order_payments_dataset.csv",
        "table": "ORDERPAYMENT_OD",
        "merge_key": "order_id",  # composite in reality (order_id + payment_sequential)
        "columns": [
            "order_id",
            "payment_sequential",
            "payment_type",
            "payment_installments",
            "payment_value"
        ]
    },
    {
        "gcs_path": "landing/olist_order_reviews_dataset.csv",
        "table": "ORDERREVIEWS_OD",
        "merge_key": "review_id",
        "columns": [
            "review_id",
            "order_id",
            "review_score",
            "review_comment_title",
            "review_comment_message",
            "review_creation_date",
            "review_answer_timestamp"
        ]
    },
    {
        "gcs_path": "landing/olist_geolocation_dataset.csv",
        "table": "GEOLOCATION_OD",
        "merge_key": "geolocation_zip_code_prefix",  # not truly unique in source
        "columns": [
            "geolocation_zip_code_prefix",
            "geolocation_lat",
            "geolocation_lng",
            "geolocation_city",
            "geolocation_state"
        ]
    },
    {
        "gcs_path": "landing/product_category_name_translation.csv",
        "table": "PRODUCT_CATEGORY_OD",
        "merge_key": "product_category_name",
        "columns": [
            "product_category_name",
            "product_category_name_english"
        ]
    }
    ]
