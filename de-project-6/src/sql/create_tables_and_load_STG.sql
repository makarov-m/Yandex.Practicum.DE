DROP TABLE IF EXISTS SMARTFLIPYANDEXRU__STAGING.group_log;
DROP TABLE IF EXISTS SMARTFLIPYANDEXRU__STAGING.group_log_rej;

CREATE TABLE SMARTFLIPYANDEXRU__STAGING.group_log
(
    group_id INT NOT NULL,
    user_id INT,
    user_id_from INT,
    event varchar(50),
    datetime timestamp
);

COPY SMARTFLIPYANDEXRU__STAGING.group_log (group_id, user_id, user_id_from, event, datetime)
FROM LOCAL '/Users/max/Documents/GitHub/de-project-sprint-6/data/group_log.csv'
DELIMITER ','
REJECTED DATA AS TABLE SMARTFLIPYANDEXRU__STAGING.group_log_rej;