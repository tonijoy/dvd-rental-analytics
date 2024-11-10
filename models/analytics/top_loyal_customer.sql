with loyal_customers as (
SELECT first_name ||' '|| last_name AS full_name,
    email, 
    address, 
    phone, 
    city, 
    country, 
    sum(amount) AS total_purchase
FROM {{source('dvd_rental', 'customer')}} cs 
JOIN {{source('dvd_rental', 'address')}} ad
ON cs.address_id = ad.address_id
JOIN {{source('dvd_rental', 'city')}} as ct
ON ad.city_id = ct.city_id
JOIN {{source('dvd_rental', 'country')}} cy
ON ct.country_id = cy.country_id
JOIN {{source('dvd_rental', 'payment')}} pm
ON cs.customer_id = pm.customer_id
GROUP BY 1, 2, 3, 4, 5, 6
),
final as (
select * from loyal_customers
order by total_purchase desc
limit 10
)
select * from final