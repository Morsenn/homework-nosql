describe cluster;

create keyspace my_ks with replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
use my_ks;
-- В CQL нельзя наложить огранчичение на длину строки, text и varchar - синонимы
create table users (
    user_id int,
    username varchar,
    email varchar,
    PRIMARY KEY (user_id)
);

create table user_posts (
    post_id int,
    user_id int,
    username varchar,
    content text,
    created_at timestamp,
    PRIMARY KEY (user_id, created_at)
) with clustering order by (created_at desc);

-- Добавил в ключ post_id, так как инчае ключ может быть не уникальным
create table topic_of_posts (
    post_id int,
    user_id int,
    username varchar,
    topic_id int,
    created_at timestamp,
    PRIMARY KEY (topic_id, created_at, post_id)
) with clustering order by (created_at desc);

SELECT
    post_id,
    content,
    created_at
FROM user_posts
WHERE
    user_id = 1;

SELECT
    post_id,
    content,
    created_at,
    username
FROM user_posts
WHERE
    user_id = 1
LIMIT 1;

-- Не понял что означает "после заданной даты", если дата - это тип данных date. Если это timestamp, то
-- всё ок
SELECT
    post_id,
    content,
    created_at
FROM user_posts
WHERE
    user_id = 1
    AND created_at > '2023-12-01 01:00:00';

SELECT
    user_id,
    username,
    post_id,
    created_at
FROM topic_of_posts
WHERE
    topic_id = 1
    AND created_at >= '2023-12-01'
    AND created_at < '2023-12-01' + 1d;
