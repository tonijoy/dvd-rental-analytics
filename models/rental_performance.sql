with rental_performance AS (
    SELECT inventory_id,
    DATEDIFF('day', return_date, rental_date) AS date_difference
    FROM {{source('dvd_rental', 'rental')}} 
    ),

final AS (SELECT f.film_id,
      CASE 
          WHEN rental_duration > date_difference THEN 'Returned early'
          WHEN rental_duration = date_difference THEN 'Returned on time'
          ELSE 'Returned late'
      END AS return_status
      FROM {{source('dvd_rental', 'film')}} f
      JOIN {{source('dvd_rental', 'inventory')}} i
      USING(film_id)
      JOIN rental_performance
      USING(inventory_id))

select return_status, count(film_id) total_films
from final
group by return_status