with purchase_with_params as (
  select * except (ecommerce),
    ecommerce.total_item_quantity,
    ecommerce.purchase_revenue_in_usd,
    ecommerce.purchase_revenue,
    ecommerce.shipping_value_in_usd,
    ecommerce.shipping_value,
    ecommerce.tax_value_in_usd,
    ecommerce.tax_value,
    ecommerce.unique_items,
    {{ ga4.unnest_key('event_params', 'coupon') }},
    {{ ga4.unnest_key('event_params', 'transaction_id') }},
    {{ ga4.unnest_key('event_params', 'currency') }},
    {{ ga4.unnest_key('event_params', 'value', 'double_value') }},
    {{ ga4.unnest_key('event_params', 'tax', 'double_value') }},
    {{ ga4.unnest_key('event_params', 'shipping', 'double_value') }},
    {{ ga4.unnest_key('event_params', 'affiliation') }}
    {% if var("default_custom_parameters", "none") != "none" %}
      {{ ga4.stage_custom_parameters( var("default_custom_parameters") )}}
    {% endif %}
    {% if var("purchase_custom_parameters", "none") != "none" %}
      {{ ga4.stage_custom_parameters( var("purchase_custom_parameters") )}}
    {% endif %}
 from {{ref('stg_ga4__events')}}
 where event_name = 'purchase'
),

add_trans_clean_number as (
  select
    *,
    regexp_replace(transaction_id, r'(#|-IT|-CH|-ES|-DE)', '') as trans_clean_number
  from purchase_with_params
)

select
  *,
  case when page_hostname = "dalfilo.com" then concat("#", trans_clean_number, "-IT")
        when page_hostname = "dalfilo.de" then concat("#", trans_clean_number, "-DE")
        when page_hostname = "dalfilo.ch" then concat("#", trans_clean_number, "-CH")
        when page_hostname = "dalfilo.es" then concat("#", trans_clean_number, "-ES")
        else "no_order_number" end as trans_shop_number
from add_trans_clean_number
qualify row_number() over (partition by session_partition_key, trans_clean_number)=1