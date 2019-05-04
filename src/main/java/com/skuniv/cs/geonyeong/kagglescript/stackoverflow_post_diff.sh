#!/usr/bin/env bash

HIVE=/usr/bin/hive

$HIVE -e "

USE etl_dev
;

DROP TABLE IF EXISTS answer_recent
;

CREATE TABLE answer_recent
AS
SELECT
    new.answer_id as answer_id,
    new.body as body,
    new.comment_count as comment_count,
    new.creation_date as creation_date,
    new.owner_display_name as owner_display_name,
    new.owner_user_id as owner_user_id,
    new.parent_id as parent_id,
    new.score as score,
    new.tags as tags,
    new.user_about_me as user_about_me,
    new.user_age as user_age,
    new.user_creation_date as user_creation_date,
    new.user_up_votes as user_up_votes,
    new.user_down_votes as user_down_votes,
    new.user_profile_image_url as user_profile_image_url,
    new.user_website_url as user_website_url,
    new.answer_comment_set as answer_comment_set,
    new.answer_link_set as answer_link_set
FROM answer_comment_link_set new
LEFT OUTER JOIN answer_comment_link_set_old old ON(new.answer_id=old.answer_id)
WHERE old.answer_id IS NULL
    OR
    (
      new.body != old.body OR new.tags != old.tags OR new.owner_display_name != old.owner_display_name
      OR new.user_about_me != old.user_about_me OR new.user_age != old.user_age OR new.user_up_votes != old.user_up_votes
      OR new.user_down_votes != old.user_down_votes OR new.user_profile_image_url != old.user_profile_image_url
      OR new.user_website_url != old.user_website_url
    )
    OR
    (
        concat_ws(',',new.answer_comment_set) != concat_ws(',',old.answer_comment_set)
    )
;

DROP TABLE IF EXISTS answer_delete
;

CREATE TABLE answer_delete
AS
SELECT
    old.answer_id as answer_id
FROM answer_comment_link_set_old old
LEFT OUTER JOIN answer_comment_link_set new ON(new.answer_id=old.answer_id)
WHERE new.answer_id IS NULL
;

DROP TABLE IF EXISTS question_recent
;

CREATE TABLE question_recent
AS
SELECT
    new.question_id as question_id,
    new.title as title,
    new.body as body,
    new.answer_count as answer_count,
    new.comment_count as comment_count,
    new.creation_date as creation_date,
    new.favorite_count as favorite_count,
    new.owner_display_name as owner_display_name,
    new.owner_user_id as owner_user_id,
    new.score as score,
    new.tags as tags,
    new.view_count as view_count,
    new.user_about_me as user_about_me,
    new.user_age as user_age,
    new.user_creation_date as user_creation_date,
    new.user_up_votes as user_up_votes,
    new.user_down_votes as user_down_votes,
    new.user_profile_image_url as user_profile_image_url,
    new.user_website_url as user_website_url,
    new.question_comment_set as question_comment_set,
    new.question_link_set as question_link_set
FROM question_comment_link_set new
LEFT OUTER JOIN question_comment_link_set_old old ON(new.question_id=old.question_id)
WHERE old.question_id IS NULL
    OR
    (
      new.title != old.title OR new.body != old.body OR new.tags != old.tags 
      OR new.owner_display_name != old.owner_display_name OR new.user_about_me != old.user_about_me 
      OR new.user_age != old.user_age OR new.user_up_votes != old.user_up_votes
      OR new.user_down_votes != old.user_down_votes OR new.user_profile_image_url != old.user_profile_image_url
      OR new.user_website_url != old.user_website_url
    )
    OR
    (
        concat_ws(',',new.question_comment_set) != concat_ws(',',old.question_comment_set)
    )
;

DROP TABLE IF EXISTS question_delete
;

CREATE TABLE question_delete
AS
SELECT
    old.question_id as question_id
FROM question_comment_link_set_old old
LEFT OUTER JOIN question_comment_link_set new ON(new.question_id=old.question_id)
WHERE new.question_id IS NULL
;

DROP TABLE IF EXISTS question_comment_link_set_old
;

CREATE TABLE question_comment_link_set_old
AS
SELECT
    *
FROM question_comment_link_set
;

DROP TABLE IF EXISTS answer_comment_link_set_old
;

CREATE TABLE answer_comment_link_set_old
AS
SELECT
    *
FROM answer_comment_link_set
;
" || exit $?