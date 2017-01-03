mysql> SET PASSWORD FOR 'root'@'localhost' = PASSWORD('Pass1234');
mysql> create database yelp_all;
Query OK, 1 row affected (0.21 sec)

mysql> use yelp_all;
Database changed

Execute json_to_csv_dataset.convert_split_user()
Execute json_to_csv_dataset.convert_busnss()
Execute json_to_csv_dataset.filter_rests()

mysql> create table elite_users(name text, user_id varchar(100), average_stars int, review_count int, yelping_since int);
mysql> load data local infile 'elite_users.csv' into table elite_users FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' IGNORE 1 LINES;
Query OK, 31461 rows affected, 62922 warnings (6.12 sec)
Records: 31461  Deleted: 0  Skipped: 0  Warnings: 62922

mysql>  create table users(name text, user_id varchar(100), average_stars int, review_count int, yelping_since int);
mysql> load data local infile 'users.csv' into table users FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' IGNORE 1 LINES;
Query OK, 520878 rows affected, 65535 warnings (38.93 sec)
Records: 520878  Deleted: 0  Skipped: 0  Warnings: 1041757

mysql> create table busnss(name text,business_id varchar(100), city text, state text, categories text, stars int, review_count int);
mysql> load data local infile 'busnss.csv' into table busnss FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' IGNORE 1 LINES; 
Query OK, 77445 rows affected, 5800 warnings (3.27 sec)
Records: 77445  Deleted: 0  Skipped: 0  Warnings: 5800

mysql> create table rests(name text,business_id varchar(100), city text, state text, categories text, stars int, review_count int);
mysql> load data local infile 'rests.csv' into table rests FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' IGNORE 1 LINES;
Query OK, 34654 rows affected, 1257 warnings (1.57 sec)
Records: 34654  Deleted: 0  Skipped: 0  Warnings: 1257

Execute  generate_ratings_dataset.populate_datasets_db()	 
Execute  generate_ratings_dataset.create_users_ratings_dataset()

create table ratings_users_rests(user_name text,user_id varchar(100), item_name text,item_id varchar(100), rating int);
create table ratings_elite_users_rests(user_name text,user_id varchar(100), item_name text,item_id varchar(100), rating int);
create table ratings_users_busnss(user_name text,user_id varchar(100), item_name text,item_id varchar(100), rating int);
create table ratings_elite_users_busnss(user_name text,user_id varchar(100), item_name text,item_id varchar(100), rating int);

load data local infile 'ratings_users_rests.csv' into table ratings_users_rests FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' IGNORE 1 LINES;
Query OK, 1180091 rows affected (19.05 sec)
Records: 1180091  Deleted: 0  Skipped: 0  Warnings: 0


load data local infile 'ratings_elite_users_rests.csv' into table ratings_elite_users_rests FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' IGNORE 1 LINES;
Query OK, 411550 rows affected (11.38 sec)
Records: 411550  Deleted: 0  Skipped: 0  Warnings: 0


load data local infile 'ratings_users_busnss.csv' into table ratings_users_busnss FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' IGNORE 1 LINES;
Query OK, 1667922 rows affected (37.33 sec)
Records: 1667922  Deleted: 0  Skipped: 0  Warnings: 0


load data local infile 'ratings_elite_users_busnss.csv' into table ratings_elite_users_busnss FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' IGNORE 1 LINES;
Query OK, 542330 rows affected (21.43 sec)
Records: 542330  Deleted: 0  Skipped: 0  Warnings: 0


Generated the datasets for users  for train and test 

create table users_ratings_test(user_name text,user_id varchar(100), item_name text,item_id varchar(100), rating int);
create table users_ratings_train(user_name text,user_id varchar(100), item_name text,item_id varchar(100), rating int);
create table elite_users_ratings_test(user_name text,user_id varchar(100), item_name text,item_id varchar(100), rating int);
create table elite_users_ratings_train(user_name text,user_id varchar(100), item_name text,item_id varchar(100), rating int);


create table train_ratings(user_name text,user_id varchar(100), item_name text,item_id varchar(100), rating int);
create table test_ratings(user_name text,user_id varchar(100), item_name text,item_id varchar(100), rating int);

load data local infile 'train_ratings.csv' into table train_ratings FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;
load data local infile 'test_ratings.csv' into table test_ratings FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;


ALTER TABLE `test_ratings` ADD INDEX `user_id` (`user_id`);
ALTER TABLE `train_ratings` ADD INDEX `user_id` (`user_id`);

ALTER TABLE `train_ratings` ADD INDEX `item_id` (`item_id`);
ALTER TABLE `test_ratings` ADD INDEX `item_id` (`item_id`);

ALTER TABLE `ratings_users_rests` ADD INDEX `item_id` (`item_id`);
ALTER TABLE `ratings_users_rests` ADD INDEX `user_id` (`user_id`);

ALTER TABLE `ratings_elite_users_rests` ADD INDEX `item_id` (`item_id`);
ALTER TABLE `ratings_elite_users_rests` ADD INDEX `user_id` (`user_id`);

ALTER TABLE `users` ADD INDEX `user_id` (`user_id`);
ALTER TABLE `rests` ADD INDEX `business_id` (`business_id`);


create table train_ratings_train_1(user_name text,user_id varchar(100), item_name text,item_id varchar(100), rating int);
create table train_ratings_val_1(user_name text,user_id varchar(100), item_name text,item_id varchar(100), rating int);
create table train_ratings_train_2(user_name text,user_id varchar(100), item_name text,item_id varchar(100), rating int);
create table train_ratings_val_2(user_name text,user_id varchar(100), item_name text,item_id varchar(100), rating int);
create table train_ratings_train_3(user_name text,user_id varchar(100), item_name text,item_id varchar(100), rating int);
create table train_ratings_val_3(user_name text,user_id varchar(100), item_name text,item_id varchar(100), rating int);
create table train_ratings_train_4(user_name text,user_id varchar(100), item_name text,item_id varchar(100), rating int);
create table train_ratings_val_4(user_name text,user_id varchar(100), item_name text,item_id varchar(100), rating int);
create table train_ratings_train_5(user_name text,user_id varchar(100), item_name text,item_id varchar(100), rating int);
create table train_ratings_val_5(user_name text,user_id varchar(100), item_name text,item_id varchar(100), rating int);

ALTER TABLE `train_ratings_train_1` ADD INDEX `user_id` (`user_id`);
ALTER TABLE `train_ratings_val_1` ADD INDEX `user_id` (`user_id`);

ALTER TABLE `train_ratings_train_1` ADD INDEX `item_id` (`item_id`);
ALTER TABLE `train_ratings_val_1` ADD INDEX `item_id` (`item_id`);

ALTER TABLE `train_ratings_train_2` ADD INDEX `user_id` (`user_id`);
ALTER TABLE `train_ratings_val_2` ADD INDEX `user_id` (`user_id`);

ALTER TABLE `train_ratings_train_2` ADD INDEX `item_id` (`item_id`);
ALTER TABLE `train_ratings_val_2` ADD INDEX `item_id` (`item_id`);

ALTER TABLE `train_ratings_train_3` ADD INDEX `user_id` (`user_id`);
ALTER TABLE `train_ratings_val_3` ADD INDEX `user_id` (`user_id`);

ALTER TABLE `train_ratings_train_3` ADD INDEX `item_id` (`item_id`);
ALTER TABLE `train_ratings_val_3` ADD INDEX `item_id` (`item_id`);

ALTER TABLE `train_ratings_train_4` ADD INDEX `user_id` (`user_id`);
ALTER TABLE `train_ratings_val_4` ADD INDEX `user_id` (`user_id`);

ALTER TABLE `train_ratings_train_4` ADD INDEX `item_id` (`item_id`);
ALTER TABLE `train_ratings_val_4` ADD INDEX `item_id` (`item_id`);

ALTER TABLE `train_ratings_train_5` ADD INDEX `user_id` (`user_id`);
ALTER TABLE `train_ratings_val_5` ADD INDEX `user_id` (`user_id`);

ALTER TABLE `train_ratings_train_5` ADD INDEX `item_id` (`item_id`);
ALTER TABLE `train_ratings_val_5` ADD INDEX `item_id` (`item_id`);

load data local infile 'train_ratings_f2.csv' into table train_ratings_train_1 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;
load data local infile 'train_ratings_f3.csv' into table train_ratings_train_1 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;
load data local infile 'train_ratings_f4.csv' into table train_ratings_train_1 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;
load data local infile 'train_ratings_f5.csv' into table train_ratings_train_1 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;

load data local infile 'train_ratings_f1.csv' into table train_ratings_train_2 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;
load data local infile 'train_ratings_f3.csv' into table train_ratings_train_2 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;
load data local infile 'train_ratings_f4.csv' into table train_ratings_train_2 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;
load data local infile 'train_ratings_f5.csv' into table train_ratings_train_2 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;

load data local infile 'train_ratings_f2.csv' into table train_ratings_train_3 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;
load data local infile 'train_ratings_f1.csv' into table train_ratings_train_3 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;
load data local infile 'train_ratings_f4.csv' into table train_ratings_train_3 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;
load data local infile 'train_ratings_f5.csv' into table train_ratings_train_3 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;

load data local infile 'train_ratings_f2.csv' into table train_ratings_train_4 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;
load data local infile 'train_ratings_f3.csv' into table train_ratings_train_4 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;
load data local infile 'train_ratings_f1.csv' into table train_ratings_train_4 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;
load data local infile 'train_ratings_f5.csv' into table train_ratings_train_4 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;

load data local infile 'train_ratings_f2.csv' into table train_ratings_train_5 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;
load data local infile 'train_ratings_f3.csv' into table train_ratings_train_5 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;
load data local infile 'train_ratings_f4.csv' into table train_ratings_train_5 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;
load data local infile 'train_ratings_f1.csv' into table train_ratings_train_5 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;

load data local infile 'train_ratings_f1.csv' into table train_ratings_val_1 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;
load data local infile 'train_ratings_f2.csv' into table train_ratings_val_2 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;
load data local infile 'train_ratings_f3.csv' into table train_ratings_val_3 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;
load data local infile 'train_ratings_f4.csv' into table train_ratings_val_4 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;
load data local infile 'train_ratings_f5.csv' into table train_ratings_val_5 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;

create table elite_ratings_train_1(user_name text,user_id varchar(100), item_name text,item_id varchar(100), rating int);
create table elite_ratings_train_2(user_name text,user_id varchar(100), item_name text,item_id varchar(100), rating int);
create table elite_ratings_train_3(user_name text,user_id varchar(100), item_name text,item_id varchar(100), rating int);
create table elite_ratings_train_4(user_name text,user_id varchar(100), item_name text,item_id varchar(100), rating int);
create table elite_ratings_train_5(user_name text,user_id varchar(100), item_name text,item_id varchar(100), rating int);

load data local infile 'elite_ratings_f1.csv' into table elite_ratings_train_1 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;
load data local infile 'elite_ratings_f2.csv' into table elite_ratings_train_2 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;
load data local infile 'elite_ratings_f3.csv' into table elite_ratings_train_3 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;
load data local infile 'elite_ratings_f4.csv' into table elite_ratings_train_4 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;
load data local infile 'elite_ratings_f5.csv' into table elite_ratings_train_5 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;
