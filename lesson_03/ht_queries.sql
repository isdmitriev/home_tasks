/*
 Завдання на SQL до лекції 03.
 */


/*
1.
Вивести кількість фільмів в кожній категорії.
Результат відсортувати за спаданням.
*/
-- SQL code goes here...
select category.name, count(*) as category_films_amount
from category
         inner join public.film_category fc on category.category_id = fc.category_id
group by category.category_id
order by category_films_amount desc;


/*
2.
Вивести 10 акторів, чиї фільми брали на прокат найбільше.
Результат відсортувати за спаданням.
*/
-- SQL code goes here...
select actor.actor_id, actor.first_name, actor.last_name, count(*) as actor_rental_amount
from inventory
         inner join public.rental on inventory.inventory_id = rental.inventory_id
         inner join public.film f on inventory.film_id = f.film_id
         inner join film_actor on public.film_actor.film_id = f.film_id
         inner join public.actor actor on film_actor.actor_id = actor.actor_id
group by actor.actor_id, actor.first_name, actor.last_name
order by actor_rental_amount desc limit 10;

/*
3.
Вивести категорія фільмів, на яку було витрачено найбільше грошей
в прокаті.
tab,tab1 -таблиці з даними згрупованими по категоріі фільмів.колонки-назва категоріі,
money_for_category-гроші витрачені на кожну категорію в прокаті
*/
-- SQL code goes here...
select category_name
from (select category.name as category_name, sum(amount) as money_for_category
      from inventory
               inner join rental on inventory.inventory_id = rental.inventory_id
               inner join payment on rental.rental_id = payment.rental_id
               inner join film on inventory.film_id = film.film_id
               inner join film_category on film.film_id = film_category.film_id
               inner join category on film_category.category_id = category.category_id
      group by category.name
      order by money_for_category) as tab1
where money_for_category = (select max(money_for_category) max_amount
                            from (select category.name, sum(amount) as money_for_category
                                  from inventory
                                           inner join rental on inventory.inventory_id = rental.inventory_id
                                           inner join payment on rental.rental_id = payment.rental_id
                                           inner join film on inventory.film_id = film.film_id
                                           inner join film_category on film.film_id = film_category.film_id
                                           inner join category on film_category.category_id = category.category_id
                                  group by category.name
                                  order by money_for_category desc) as tab)

/*
4.
Вивести назви фільмів, яких не має в inventory.
Запит має бути без оператора IN
*/
-- SQL code goes here...
select film.title
from film
         left join inventory on film.film_id = inventory.film_id
where inventory_id is null

/*
5.
Вивести топ 3 актори, які найбільше зʼявлялись в категорії фільмів “Children”.
*/
-- SQL code goes here...
select actor.first_name, actor.last_name, count(*) as actor_films_in_category_amount
from film
         inner join film_actor on film.film_id = film_actor.film_id
         inner join actor on film_actor.actor_id = actor.actor_id
         inner join film_category on film.film_id = film_category.film_id
         inner join category on film_category.category_id = category.category_id
where category.name = 'Children'
group by actor.actor_id, actor.first_name, actor.last_name
order by actor_films_in_category_amount desc limit 3
