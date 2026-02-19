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
    }
]
