drop table if exists SMARTFLIPYANDEXRU__DWH.h_users CASCADE;
drop table if exists SMARTFLIPYANDEXRU__DWH.h_groups CASCADE;
drop table if exists SMARTFLIPYANDEXRU__DWH.l_user_group_activity CASCADE;
drop table if exists SMARTFLIPYANDEXRU__DWH.s_auth_history CASCADE;

------------------------------
---USERS HUB
------------------------------

create table SMARTFLIPYANDEXRU__DWH.h_users
(
    hk_user_id bigint primary key,
    user_id      int,
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


INSERT INTO SMARTFLIPYANDEXRU__DWH.h_users(hk_user_id, user_id, load_dt, load_src)
select distinct 
       hash(user_id) as  hk_user_id,
       user_id,
       now() as load_dt,
       's3' as load_src
       from SMARTFLIPYANDEXRU__STAGING.group_log
where hash(user_id) not in (select hk_user_id from SMARTFLIPYANDEXRU__DWH.h_users);

------------------------------
---GROUPS HUB
------------------------------

create table SMARTFLIPYANDEXRU__DWH.h_groups
(
    hk_group_id       bigint primary key,
    group_id          int,
    registration_dt datetime,
    load_dt           datetime,
    load_src          varchar(20)
)
order by load_dt
SEGMENTED BY hk_group_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


INSERT INTO SMARTFLIPYANDEXRU__DWH.h_groups(hk_group_id, group_id, registration_dt, load_dt, load_src)
select distinct 
       hash(group_id) as hk_group_id,
       group_id,
       datetime, 
       now() as load_dt,
       's3' as load_src
       from SMARTFLIPYANDEXRU__STAGING.group_log
where hash(group_id) not in (select hk_group_id from SMARTFLIPYANDEXRU__DWH.h_groups)
and event = 'create';

------------------------------
---USER ACTIVITY LINK
------------------------------

create table SMARTFLIPYANDEXRU__DWH.l_user_group_activity
(
	hk_l_user_group_activity bigint primary key,
	hk_user_id bigint not null CONSTRAINT fk_h_users REFERENCES SMARTFLIPYANDEXRU__DWH.h_users (hk_user_id),
	hk_group_id bigint not null CONSTRAINT fk_h_groups REFERENCES SMARTFLIPYANDEXRU__DWH.h_groups (hk_group_id),
	load_dt           datetime,
	load_src          varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO SMARTFLIPYANDEXRU__DWH.l_user_group_activity(hk_l_user_group_activity, hk_user_id, hk_group_id, load_dt, load_src)
select distinct 
    hash(hu.hk_user_id, hg.hk_group_id),
    hu.hk_user_id,
    hg.hk_group_id,
    now() as load_dt,
    's3' as load_src
from SMARTFLIPYANDEXRU__STAGING.group_log as gl
    left join SMARTFLIPYANDEXRU__DWH.h_users as hu on gl.user_id = hu.user_id
    left join SMARTFLIPYANDEXRU__DWH.h_groups as hg on gl.group_id = hg.group_id
where hash(hu.hk_user_id, hg.hk_group_id) not in (select hk_l_user_group_activity from SMARTFLIPYANDEXRU__DWH.l_user_group_activity);


------------------------------
---AUTH HISTORY SATELLITE
------------------------------

create table SMARTFLIPYANDEXRU__DWH.s_auth_history
(
    hk_l_user_group_activity bigint not null CONSTRAINT fk_l_user_group_activity REFERENCES SMARTFLIPYANDEXRU__DWH.l_user_group_activity (hk_l_user_group_activity),
    user_id_from integer,
    event varchar(100),
    event_dt datetime,
    load_dt datetime,
    load_scr varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


INSERT INTO SMARTFLIPYANDEXRU__DWH.s_auth_history(hk_l_user_group_activity, user_id_from,event,event_dt,load_dt,load_scr)
select distinct   
	luga.hk_l_user_group_activity
	, gl.user_id_from
	,gl.event
	, gl.datetime 
	,now() as load_dt
	,'s3' as load_src
from SMARTFLIPYANDEXRU__STAGING.group_log as gl
left join SMARTFLIPYANDEXRU__DWH.h_groups as hg on gl.group_id = hg.group_id
left join SMARTFLIPYANDEXRU__DWH.h_users as hu on gl.user_id = hu.user_id
left join SMARTFLIPYANDEXRU__DWH.l_user_group_activity as luga on (hg.hk_group_id = luga.hk_group_id) and (hu.hk_user_id = luga.hk_user_id)
where luga.hk_l_user_group_activity not in (select hk_l_user_group_activity from SMARTFLIPYANDEXRU__DWH.s_auth_history)
and gl.event = 'add' or gl.event = 'leave';