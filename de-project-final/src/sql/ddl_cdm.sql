
-- Global metrics & Projections
CREATE SCHEMA IF NOT EXISTS dwh.SMARTFLIPYANDEXRU;
DROP TABLE if exists SMARTFLIPYANDEXRU.global_metrics;
CREATE TABLE if not exists SMARTFLIPYANDEXRU.global_metrics (
	date_update date NOT NULL,
	currency_from smallint NOT NULL,
    amount_total numeric (17,3) NOT NULL,
    cnt_transactions int NOT NULL,
	avg_transactions_per_account numeric (17,3) NOT NULL,
	cnt_accounts_make_transactions int NOT NULL,
	CONSTRAINT pk PRIMARY KEY (date_update, currency_from) ENABLED
)
ORDER BY date_update
SEGMENTED BY HASH(date_update, currency_from) ALL NODES
PARTITION BY date_update::date
GROUP BY calendar_hierarchy_day(date_update::date, 3, 2);

CREATE PROJECTION SMARTFLIPYANDEXRU.global_metrics 
(
 date_update,
 currency_from,
 amount_total,
 cnt_transactions,
 avg_transactions_per_account,
 cnt_accounts_make_transactions
)
AS
 SELECT global_metrics.date_update,
        global_metrics.currency_from,
        global_metrics.amount_total,
        global_metrics.cnt_transactions,
        global_metrics.avg_transactions_per_account,
        global_metrics.cnt_accounts_make_transactions
 FROM SMARTFLIPYANDEXRU.global_metrics
 ORDER BY global_metrics.date_update
SEGMENTED BY hash(global_metrics.date_update, global_metrics.currency_from) ALL NODES KSAFE 1;

SELECT MARK_DESIGN_KSAFE(1);