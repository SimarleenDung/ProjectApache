{%- macro build_email_filter(selected_customer) -%}
    {# Handle list of customers #}
    {%- if selected_customer is iterable and selected_customer is not string -%}
        and email in (
            {%- for customer in selected_customer %}
            '{{ customer }}'{% if not loop.last %},{% endif %}
            {%- endfor %}
        )
    
    {# Handle single customer #}
    {%- elif selected_customer not in ['all', 'None', none] -%}
        and email = '{{ selected_customer }}'
    
    {# 'all' means no filter #}
    {%- endif -%}
{%- endmacro %}