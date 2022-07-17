# Проект 2
Задача: оптимизировать нагрузку на хранилище, сделав миграцию данных в отдельные логические таблицы, а затем собрать на них витрину данных. 

## Ход выполнения проекта

1. Создать справочник стоимости доставки в страны `shipping_country_rates` из данных, указанных в `shipping_country` и `shipping_country_base_rate`, сделать первичный ключ таблицы — серийный `id`, то есть серийный идентификатор каждой строчки. Дать серийному ключу имя `id`. Справочник должен состоять из уникальных пар полей из таблицы shipping.

2. Создать справочник тарифов доставки вендора по договору `shipping_agreement` из данных строки `vendor_agreement_description` через разделитель ":".
Названия полей:
- `agreementid` PK,
- `agreement_number`,
- `agreement_rate`,
- `agreement_commission`.

3. Создать справочник о типах доставки `shipping_transfer` из строки `shipping_transfer_description` через разделитель `:`.
Названия полей:
- `transfer_type` serial PK,
- `transfer_model`,
- `shipping_transfer_rate` .

4. Создать таблицу `shipping_info` с уникальными доставками `shippingid` и связать её с созданными справочниками `shipping_country_rates`, `shipping_agreement`, `shipping_transfer` и константной информацией о доставке `shipping_plan_datetime` , `payment_amount` , `vendorid` .

5. Создать таблицу статусов о доставке `shipping_status` и включите туда информацию из лога `shipping (status , state)`. Добаить туда вычислимую информацию по фактическому времени доставки `shipping_start_fact_datetime`, `shipping_end_fact_datetime` . Отразить для каждого уникального `shippingid` его итоговое состояние доставки.

6. Создать представление `shipping_datamart` на основании готовых таблиц для аналитики и включить в него:
- `shippingid`
- `vendorid`
- `transfer_type` — тип доставки из таблицы shipping_transfer
- `full_day_at_shipping` — количество полных дней, в течение которых длилась доставка. Высчитывается как: `shipping_end_fact_datetime-shipping_start_fact_datetime`.
- `is_delay` — статус, показывающий просрочена ли доставка. Высчитывается как: `shipping_end_fact_datetime > shipping_plan_datetime → 1 ; 0`
- `is_shipping_finish` — статус, показывающий, что доставка завершена. Если финальный `status = finished → 1; 0`
- `delay_day_at_shipping` — количество дней, на которые была просрочена доставка. Высчитыается как: `shipping_end_fact_datetime > shipping_end_plan_datetime → shipping_end_fact_datetime - shipping_plan_datetime ; 0`.
- `payment_amount` — сумма платежа пользователя
- `vat` — итоговый налог на доставку. Высчитывается как: `payment_amount * ( shipping_country_base_rate + agreement_rate + shipping_transfer_rate)` .
- `profit` — итоговый доход компании с доставки. Высчитывается как: `payment_amount * agreement_commission`.

## Отработка скриптов
Скрипты выполняются последовательно:

- 1_create_tables.sql

- 2_data_import.sql

- 3_create_temp_tables.sql

- 4_temp_tables_query.sql

- 5_shipping_datamart_view.sql
