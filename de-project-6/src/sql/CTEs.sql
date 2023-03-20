WITH user_group_messages AS
  (-- users who are active
 SELECT hk_group_id,
        count(DISTINCT (hk_user_id)) AS cnt_users_in_group_with_messages
   FROM SMARTFLIPYANDEXRU__DWH.l_user_group_activity luga
   GROUP BY hk_group_id),
     user_group_log AS
  (-- users who were added
 SELECT hk_group_id,
        count(DISTINCT (hk_user_id)) AS cnt_added_users
   FROM SMARTFLIPYANDEXRU__DWH.l_user_group_activity luga
   LEFT JOIN SMARTFLIPYANDEXRU__DWH.s_auth_history sah ON luga.hk_l_user_group_activity = sah.hk_l_user_group_activity
   WHERE sah.event = 'add'
   GROUP BY luga.hk_group_id)
SELECT ugm.hk_group_id,
       ugl.cnt_added_users,
       ugm.cnt_users_in_group_with_messages,
       (ugm.cnt_users_in_group_with_messages / ugm.hk_group_id) AS group_conversion
FROM user_group_messages AS ugm
LEFT JOIN user_group_log AS ugl ON ugm.hk_group_id = ugl.hk_group_id
LEFT JOIN SMARTFLIPYANDEXRU__DWH.h_groups AS hg ON ugl.hk_group_id = hg.hk_group_id
WHERE ugm.hk_group_id in
    (SELECT hk_group_id
     FROM SMARTFLIPYANDEXRU__DWH.h_groups hg2
     ORDER BY registration_dt ASC
     LIMIT 10)
ORDER BY group_conversion;