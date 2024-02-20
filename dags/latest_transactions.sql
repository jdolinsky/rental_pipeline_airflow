select 
        p.payment_id,
        p.customer_id,
        p.staff_id,
        p.rental_id,
        p.payment_date,
        r.rental_date,
        r.return_date,
        m.title,
        m.description,
        m.release_year,
        m.rating,
        c3.name as category,
        p.amount::float as price,
        c.first_name,
        c.last_name,
        c.email,
        c.address_id,
        a.address,
        c1.city,
        c2.country
    from payment as p
    join rental as r on
        p.rental_id = r.rental_id
    join customer as c on
        c.customer_id = p.customer_id
    join address as a on 
        a.address_id = c.address_id
    join city as c1 on
        c1.city_id = a.city_id
    join country as c2 on
        c2.country_id = c1.country_id
    join inventory as i on
        i.inventory_id = r.inventory_id
    join film as m on 
        m.film_id = i.film_id
    join film_category as f1 on
        f1.film_id = m.film_id
    join category as c3 on
        c3.category_id = f1.category_id
    where
        payment_date > '{{ task_instance.xcom_pull(task_ids='get_max_date_bq', key='date') }}'
    order by 
        payment_id desc