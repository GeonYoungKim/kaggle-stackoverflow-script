#!/usr/bin/env bash

HIVE=/usr/bin/hive

if [ $# -lt 5 ]
then
  echo "Usage : $0  need path : [user, link, comment, answer, question]"
  exit 1
fi

USER_HDFS_PATH=$1
LINK_HDFS_PATH=$2
COMMENT_HDFS_PATH=$3
ANSWER_HDFS_PATH=$4
QUESTION_HDFS_PATH=$5

DELEMETER='`'

$HIVE -e "

USE etl_dev
;

DROP TABLE IF EXISTS es_study_user
;

CREATE EXTERNAL TABLE es_study_user (
    id string,
    display_name string,
    about_me string,
    age string,
    creation_date string,
    up_votes string,
    down_votes string,
    profile_image_url string,
    website_url string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '${DELEMETER}'
STORED AS TEXTFILE
LOCATION '${USER_HDFS_PATH}'
;

DROP TABLE IF EXISTS es_study_link
;

CREATE EXTERNAL TABLE es_study_link (
    id string,
    post_id string,
    related_post_id string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '${DELEMETER}'
STORED AS TEXTFILE
LOCATION '${LINK_HDFS_PATH}'
;

DROP TABLE IF EXISTS es_study_comment
;

CREATE EXTERNAL TABLE es_study_comment (
    id string,
    text string,
    creation_date string,
    post_id string,
    user_id string,
    user_display_name string,
    score string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '${DELEMETER}'
STORED AS TEXTFILE
LOCATION '${COMMENT_HDFS_PATH}'
;

DROP TABLE IF EXISTS es_study_answer
;

CREATE EXTERNAL TABLE es_study_answer (
    id string,
    body string,
    comment_count string,
    creation_date string,
    owner_display_name string,
    owner_user_id string,
    parent_id string,
    score string,
    tags string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '${DELEMETER}'
STORED AS TEXTFILE
LOCATION '${ANSWER_HDFS_PATH}'
;

DROP TABLE IF EXISTS es_study_question
;

CREATE EXTERNAL TABLE es_study_question (
    id string,
    title string,
    body string,
    answer_count string,
    comment_count string,
    creation_date string,
    favorite_count string,
    owner_display_name string,
    owner_user_id string,
    score string,
    tags string,
    view_count string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '${DELEMETER}'
STORED AS TEXTFILE
LOCATION '${QUESTION_HDFS_PATH}'
;

DROP TABLE IF EXISTS es_answer_user
;

CREATE TABLE es_answer_user
AS
SELECT
    es_answer.id as answer_id,
    es_answer.body as body,
    es_answer.comment_count as comment_count,
    es_answer.creation_date as creation_date,
    es_user.display_name as owner_display_name,
    es_answer.owner_user_id as owner_user_id,
    es_answer.parent_id as parent_id,
    es_answer.score as score,
    es_answer.tags as tags,
    es_user.about_me as user_about_me,
    es_user.age as user_age,
    es_user.creation_date as user_creation_date,
    es_user.up_votes as user_up_votes,
    es_user.down_votes as user_down_votes,
    es_user.profile_image_url as user_profile_image_url,
    es_user.website_url as user_website_url
FROM es_study_answer es_answer
INNER JOIN es_study_user es_user ON (es_answer.owner_user_id=es_user.id)
;

DROP TABLE IF EXISTS es_question_user
;

CREATE TABLE es_question_user
AS
SELECT
    es_question.id as question_id,
    es_question.title as title,
    es_question.body as body,
    es_question.answer_count as answer_count,
    es_question.comment_count as comment_count,
    es_question.creation_date as creation_date,
    es_question.favorite_count as favorite_count,
    es_user.display_name as owner_display_name,
    es_question.owner_user_id as owner_user_id,
    es_question.score as score,
    es_question.tags as tags,
    es_question.view_count as view_count,
    es_user.about_me as user_about_me,
    es_user.age as user_age,
    es_user.creation_date as user_creation_date,
    es_user.up_votes as user_up_votes,
    es_user.down_votes as user_down_votes,
    es_user.profile_image_url as user_profile_image_url,
    es_user.website_url as user_website_url
FROM es_study_question es_question
INNER JOIN es_study_user es_user ON (es_question.owner_user_id=es_user.id)
;

DROP TABLE IF EXISTS es_comment_user
;

CREATE TABLE es_comment_user
AS
SELECT
    es_comment.id as comment_id,
    es_comment.text as text,
    es_comment.creation_date as creation_date,
    es_comment.post_id as post_id,
    es_comment.user_id as user_id,
    es_user.display_name as user_display_name,
    es_comment.score as score,
    es_user.about_me as user_about_me,
    es_user.age as user_age,
    es_user.creation_date as user_creation_date,
    es_user.up_votes as user_up_votes,
    es_user.down_votes as user_down_votes,
    es_user.profile_image_url as user_profile_image_url,
    es_user.website_url as user_website_url
FROM es_study_comment es_comment
INNER JOIN es_study_user es_user ON (es_comment.user_id=es_user.id)
;

DROP TABLE IF EXISTS answer_comment_link_set
;

CREATE TABLE answer_comment_link_set
AS
SELECT
    eau.answer_id as answer_id,
    eau.body as body,
    eau.comment_count as comment_count,
    eau.creation_date as creation_date,
    eau.owner_display_name as owner_display_name,
    eau.owner_user_id as owner_user_id,
    eau.parent_id as parent_id,
    eau.score as score,
    eau.tags as tags,
    eau.user_about_me as user_about_me,
    eau.user_age as user_age,
    eau.user_creation_date as user_creation_date,
    eau.user_up_votes as user_up_votes,
    eau.user_down_votes as user_down_votes,
    eau.user_profile_image_url as user_profile_image_url,
    eau.user_website_url as user_website_url,
    answer_set.answer_comment_set as answer_comment_set,
    answer_set.answer_link_set as answer_link_set
FROM es_answer_user eau
LEFT OUTER JOIN (
    SELECT
        eau.answer_id as answer_id,
        collect_set(
            CONCAT_WS(
                '${DELEMETER}',
                ecu.comment_id,
                ecu.text,
                ecu.creation_date,
                ecu.post_id,
                ecu.user_id,
                ecu.user_display_name,
                ecu.score,
                ecu.user_about_me,
                ecu.user_age,
                ecu.user_creation_date,
                ecu.user_up_votes,
                ecu.user_down_votes,
                ecu.user_profile_image_url,
                ecu.user_website_url
            )
        ) as answer_comment_set,
        collect_set(
            CONCAT_WS(
                '${DELEMETER}',
                esl.id,
                esl.post_id,
                esl.related_post_id
            )
        ) as answer_link_set
    FROM es_answer_user eau
    LEFT OUTER JOIN es_comment_user ecu ON (eau.answer_id=ecu.post_id)
    LEFT OUTER JOIN es_study_link esl ON (eau.answer_id=esl.post_id)
    GROUP BY eau.answer_id
) answer_set ON (eau.answer_id=answer_set.answer_id)
;

DROP TABLE IF EXISTS question_comment_link_set
;

CREATE TABLE question_comment_link_set
AS
SELECT
    equ.question_id as question_id,
    equ.title as title,
    equ.body as body,
    equ.answer_count as answer_count,
    equ.comment_count as comment_count,
    equ.creation_date as creation_date,
    equ.favorite_count as favorite_count,
    equ.owner_display_name as owner_display_name,
    equ.owner_user_id as owner_user_id,
    equ.score as score,
    equ.tags as tags,
    equ.view_count as view_count,
    equ.user_about_me as user_about_me,
    equ.user_age as user_age,
    equ.user_creation_date as user_creation_date,
    equ.user_up_votes as user_up_votes,
    equ.user_down_votes as user_down_votes,
    equ.user_profile_image_url as user_profile_image_url,
    equ.user_website_url as user_website_url,
    question_set.question_comment_set as question_comment_set,
    question_set.question_link_set as question_link_set
FROM es_question_user equ
LEFT OUTER JOIN (
    SELECT
        equ.question_id as question_id,
        collect_set(
            CONCAT_WS(
                '\`',
                ecu.comment_id,
                ecu.text,
                ecu.creation_date,
                ecu.post_id,
                ecu.user_id,
                ecu.user_display_name,
                ecu.score,
                ecu.user_about_me,
                ecu.user_age,
                ecu.user_creation_date,
                ecu.user_up_votes,
                ecu.user_down_votes,
                ecu.user_profile_image_url,
                ecu.user_website_url
            )
        ) as question_comment_set,
        collect_set(
            CONCAT_WS(
                '\`',
                esl.id,
                esl.post_id,
                esl.related_post_id
            )
        ) as question_link_set
    FROM es_question_user equ
    LEFT OUTER JOIN es_comment_user ecu ON (equ.question_id=ecu.post_id)
    LEFT OUTER JOIN es_study_link esl ON (equ.question_id=esl.post_id)
    GROUP BY equ.question_id
) question_set ON (equ.question_id=question_set.question_id)
;

CREATE TABLE question_comment_link_set
AS
SELECT
    equ.question_id as question_id,
    equ.title as title,
    equ.body as body,
    equ.answer_count as answer_count,
    equ.comment_count as comment_count,
    equ.creation_date as creation_date,
    equ.favorite_count as favorite_count,
    equ.owner_display_name as owner_display_name,
    equ.owner_user_id as owner_user_id,
    equ.score as score,
    equ.tags as tags,
    equ.view_count as view_count,
    equ.user_about_me as user_about_me,
    equ.user_age as user_age,
    equ.user_creation_date as user_creation_date,
    equ.user_up_votes as user_up_votes,
    equ.user_down_votes as user_down_votes,
    equ.user_profile_image_url as user_profile_image_url,
    equ.user_website_url as user_website_url,
    question_set.question_comment_set as question_comment_set,
    question_set.question_link_set as question_link_set
FROM es_question_user equ
LEFT OUTER JOIN (
    SELECT
        equ.question_id as question_id,
        collect_set(
            CONCAT_WS(
                '${DELEMETER}',
                ecu.comment_id,
                ecu.text,
                ecu.creation_date,
                ecu.post_id,
                ecu.user_id,
                ecu.user_display_name,
                ecu.score,
                ecu.user_about_me,
                ecu.user_age,
                ecu.user_creation_date,
                ecu.user_up_votes,
                ecu.user_down_votes,
                ecu.user_profile_image_url,
                ecu.user_website_url
            )
        ) as question_comment_set,
        collect_set(
            CONCAT_WS(
                '${DELEMETER}',
                esl.id,
                esl.post_id,
                esl.related_post_id
            )
        ) as question_link_set
    FROM es_question_user equ
    LEFT OUTER JOIN es_comment_user ecu ON (equ.question_id=ecu.post_id)
    LEFT OUTER JOIN es_study_link esl ON (equ.question_id=esl.post_id)
    GROUP BY equ.question_id
) question_set ON (equ.question_id=question_set.question_id)
;
" || exit $?