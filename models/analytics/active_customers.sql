select first_name, last_name, email, active
from {{source('dvd_rental', 'customer')}}
where active = 1
limit 5

