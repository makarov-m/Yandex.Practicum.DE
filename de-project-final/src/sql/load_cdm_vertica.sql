MERGE INTO SMARTFLIPYANDEXRU.global_metrics AS tgt USING
  (WITH t1 AS
     (-- main table with all data
 SELECT o.*,
        c.currency_with_div,
        (o.amount * c.currency_with_div) AS amount_usd
      FROM SMARTFLIPYANDEXRU__DWH.operations o
      LEFT JOIN SMARTFLIPYANDEXRU__DWH.currencies c ON o.currency_code = c.currency_code
      WHERE c.currency_code_with = 420
        AND o.transaction_dt::date = c.date_update::date
        AND o.status = 'done'
        AND o.transaction_dt::date = '2022-10-01' ) SELECT cast(transaction_dt AS date) AS date_update,
                                                           currency_code AS currency_from,
                                                           sum(amount_usd) AS amount_total,
                                                           count(amount_usd) AS cnt_transactions,
                                                           round((count(amount_usd)/count(DISTINCT (account_number_from))),3) AS avg_transactions_per_account,
                                                           count(DISTINCT (account_number_from)) AS cnt_accounts_make_transactions
   FROM t1
   GROUP BY date_update,
            currency_code
   ORDER BY date_update ASC) src ON tgt.date_update = src.date_update
AND tgt.currency_from = src.currency_from WHEN MATCHED THEN
UPDATE
SET amount_total = src.amount_total,
    cnt_transactions = src.cnt_transactions,
    avg_transactions_per_account = src.avg_transactions_per_account,
    cnt_accounts_make_transactions = src.cnt_accounts_make_transactions WHEN NOT MATCHED THEN
INSERT (date_update,
        currency_from,
        amount_total,
        cnt_transactions,
        avg_transactions_per_account,
        cnt_accounts_make_transactions)
VALUES (src.date_update, src.currency_from, src.amount_total, src.cnt_transactions, src.avg_transactions_per_account, src.cnt_accounts_make_transactions);

-- useful links:
-- https://stackoverflow.com/questions/74703172/vertica-merge-and-cte
-- https://practicum.yandex.ru/learn/data-engineer/courses/a3ce3493-c85c-405e-9dbd-4897de149564/sprints/92211/topics/db6680d5-6671-473d-ac57-c879818adf78/lessons/bdee64a7-f475-4c07-8774-0b0b777bd815/
