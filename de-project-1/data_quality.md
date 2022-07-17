# 1.3. Качество данных

## Оцените, насколько качественные данные хранятся в источнике.
Опишите, как вы проверяли исходные данные и какие выводы сделали.

## Укажите, какие инструменты обеспечивают качество данных в источнике.
Ответ запишите в формате таблицы со следующими столбцами:
- `Наименование таблицы` - наименование таблицы, объект которой рассматриваете.
- `Объект` - Здесь укажите название объекта в таблице, на который применён инструмент. Например, здесь стоит перечислить поля таблицы, индексы и т.д.
- `Инструмент` - тип инструмента: первичный ключ, ограничение или что-то ещё.
- `Для чего используется` - здесь в свободной форме опишите, что инструмент делает.

## Проверка качества данных
- дубли в данных
Результат проверки. дубли не встречается
'''
/** качество данных, дубликаты **/
SELECT product_id, order_id, price, discount, quantity, COUNT(*)
FROM orderitems oi 
GROUP BY product_id, order_id, price, discount, quantity
HAVING COUNT(*) > 1;
/** качество данных, дубликаты **/
SELECT order_id, COUNT(order_id)
FROM orders
GROUP BY order_id
HAVING COUNT(order_id) > 1;
/** качество данных, дубликаты **/
SELECT order_id, status_id, COUNT(*)
FROM orderstatuslog osl 
GROUP BY order_id, status_id
HAVING COUNT(*) > 1;
/** качество данных, дубликаты **/
SELECT name, COUNT(name)
FROM products p 
GROUP BY name
HAVING COUNT(name) > 1;
'''
- пропуски в важных полях
Результат проверки. Дубли не встречаются.
SELECT * FROM orderitems oi  WHERE NOT (oi IS NOT NULL);
SELECT * FROM orders o  WHERE NOT (o IS NOT NULL);
SELECT * FROM orderstatuses os  WHERE NOT (os IS NOT NULL);
SELECT * FROM orderstatuslog osl WHERE NOT (osl IS NOT NULL);
SELECT * FROM products p WHERE NOT (p IS NOT NULL);
- некорректные типы данных
Результат. Формат данных корректный
скрипт проверки формата данных:
'''
	select table_name , column_name , is_nullable , data_type 
	from information_schema."columns" c 
	where table_schema = 'production'
	order by table_name ;
'''
- неверный формат записи
Результат. В таблице `users` в столбцах `login` и `name` не соответствует содержимое. Лучше всего поменять названия полей если эту таблицу будем использовать в ходе решения.


Пример ответа:

| Таблицы             | Объект                      | Инструмент      | Для чего используется |
| ------------------- | --------------------------- | --------------- | --------------------- |
| production.Products | id int NOT NULL PRIMARY KEY | Первичный ключ  | Обеспечивает уникальность записей о пользователях |
| production.Products | price                       | Check           | Обеспечивает правильность ввода цены (больше или равна нулю), тип данных число |
| production.Products | price numeric(19, 5) NOT NULL DEFAULT 0 | NOT NULL | Обеспечивает правильность ввода цены, задает начальное значение |
| production.Products | "name" varchar(2048) NOT NULL | NOT NULL | Обеспечивает правильность ввода наименования, не пустое значение |
---------------------------------------------------------------------------------------------------------------------------------------
| production.users    | id int NOT NULL PRIMARY KEY | Первичный ключ  | Обеспечивает уникальность записей о пользователях |
| production.users    | login varchar(2048) NOT NULL | NOT NULL       | Обеспечивает отсутствие пропусков в столбце|
---------------------------------------------------------------------------------------------------------------------------------------
| production.orderstatuslog | id int NOT NULL PRIMARY KEY | Первичный ключ  | Обеспечивает уникальность записей о пользователях |
| production.orderstatuslog | UNIQUE (order_id, status_id) | CONSTAINT  | Обеспечивает уникальность записей заказа и статуса |
| production.orderstatuslog | FOREIGN KEY(order_id) | CONSTAINT  | Обеспечивает однородность данных в таблицах |
| production.orderstatuslog | FOREIGN KEY(status_id) | CONSTAINT  | Обеспечивает однородность данных в таблицах |
| production.orderstatuslog | order_id int4 NOT NULL | NOT NULL  | Обеспечивает отсутствие пропусков |
| production.orderstatuslog | status_id int4 NOT NULL | NOT NULL  | Обеспечивает отсутствие пропусков |
| production.orderstatuslog | dttm timestamp NOT NULL | NOT NULL  | Обеспечивает отсутствие пропусков |
---------------------------------------------------------------------------------------------------------------------------------------
| production.orderstatuses | id int NOT NULL PRIMARY KEY | Первичный ключ  | Обеспечивает уникальность записей о статусе заказа |
| production.orderstatuses | "key" varchar(255) NOT NULL | NOT NULL  | Обеспечивает отсутствие пропусков |
---------------------------------------------------------------------------------------------------------------------------------------
| production.orders | order_id int NOT NULL PRIMARY KEY | Первичный ключ  | Обеспечивает уникальность записей о заказе |
| production.orders | cost | CHECK  | Обеспечивает корректность расчета стоимости |
| production.orders| order_ts timestamp NOT NULL | NOT NULL  | Обеспечивает отсутствие пропусков |
| production.orders| user_id int4 NOT NULL | NOT NULL  | Обеспечивает отсутствие пропусков |
| production.orders| bonus_payment numeric(19, 5) NOT NULL DEFAULT 0 | NOT NULL  | Обеспечивает отсутствие пропусков и задает начальное значение |
| production.orders| payment numeric(19, 5) NOT NULL DEFAULT 0 | NOT NULL  | Обеспечивает отсутствие пропусков и задает начальное значение |
| production.orders| "cost" numeric(19, 5) NOT NULL DEFAULT 0 | NOT NULL  | Обеспечивает отсутствие пропусков и задает начальное значение |
| production.orders| bonus_grant numeric(19, 5) NOT NULL DEFAULT 0 | NOT NULL  | Обеспечивает отсутствие пропусков и задает начальное значение|
---------------------------------------------------------------------------------------------------------------------------------------
| production.orderitems| id int4 NOT NULL GENERATED ALWAYS AS IDENTITY | NOT NULL  | Обеспечивает отсутствие пропусков генерирует id |
| production.orderitems| product_id int4 NOT NULL | NOT NULL  | Обеспечивает отсутствие пропусков |
| production.orderitems| order_id int4 NOT NULL | NOT NULL  | Обеспечивает отсутствие пропусков |
| production.orderitems| "name" varchar(2048) NOT NULL | NOT NULL  | Обеспечивает отсутствие пропусков |
| production.orderitems| price numeric(19, 5) NOT NULL DEFAULT 0 | NOT NULL  | Обеспечивает отсутствие пропусков и задает значение 0 по-умолчанию |
| production.orderitems| discount numeric(19, 5) NOT NULL DEFAULT 0 | NOT NULL  | Обеспечивает отсутствие пропусков и задает значение 0 по-умолчанию |
| production.orderitems| quantity int4 NOT NULL | NOT NULL  | Обеспечивает отсутствие пропусков и задает значение 0 по-умолчанию |
---------------------------------------------------------------------------------------------------------------------------------------

Вывод: качество данных удовлетворяет требованиям.










