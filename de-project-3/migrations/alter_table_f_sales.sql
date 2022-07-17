-- alter column
ALTER TABLE mart.f_sales 
ADD COLUMN status varchar(20);

-- alter column
ALTER TABLE staging.user_order_log  
ADD COLUMN status varchar(20);

-- set default valuealter table staging.user_order_log alter column status set default 'shipped';
ALTER TABLE staging.user_order_log ALTER COLUMN status SET DEFAULT 'shipped';