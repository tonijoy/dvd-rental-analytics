with genre_movie_demand as (
    select 
        category.name as genre,
        count(customer.customer_id) as total_rent_demand,
        sum(payment.amount) as total_sales
    from {{source('dvd_rental', 'category')}}  
    join {{source('dvd_rental', 'film_category')}} 
    using(category_id)
    join {{source('dvd_rental', 'film')}} 
    using(film_id)
    join {{source('dvd_rental', 'inventory')}} 
    using(film_id)
    join {{source('dvd_rental', 'rental')}}  
    on inventory.inventory_id = rental.inventory_id
    join {{source('dvd_rental', 'customer')}}  
    on rental.customer_id = customer.customer_id
    join {{source('dvd_rental', 'payment')}} 
    on rental.rental_id = payment.rental_id
    group by category.category_id, category.name
    order by total_rent_demand desc, total_sales desc
    limit 5
)
select *
from genre_movie_demand
