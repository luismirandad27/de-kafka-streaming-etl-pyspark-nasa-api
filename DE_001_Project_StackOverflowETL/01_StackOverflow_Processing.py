# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading BigQuery Tables

# COMMAND ----------

#loading the datasets
table_post_questions = 'bigquery-public-data.stackoverflow.posts_questions'
table_post_answers = 'bigquery-public-data.stackoverflow.posts_answers'
table_users = 'bigquery-public-data.stackoverflow.users'
table_badges = 'bigquery-public-data.stackoverflow.badges'

# COMMAND ----------

column_post_questions = [col('id').alias('post_id'),'accepted_answer_id','answer_count','comment_count',
                          'creation_date','owner_user_id','score','tags','view_count']

column_post_answers = [col('id').alias('answer_id'),col('creation_date').alias('answer_creation_date'),col('owner_user_id').alias('user_id_answer'),
                 col('score').alias('answer_score')]

column_users = [col('id').alias('user_id'),'reputation',col('creation_date').alias('user_creation_date')]

column_badges = [col('id').alias('badge_id'),col('name').alias('badge_name'),col('date').alias('badge_creation_date'),'user_id','class','tag_based']

# COMMAND ----------

#storing data into dataframes
df_post_questions = spark.read.format('bigquery').option('table',table_post_questions).load().select(column_post_questions)
df_post_answers = spark.read.format('bigquery').option('table',table_post_answers).load().select(column_post_answers)
df_users = spark.read.format('bigquery').option('table',table_users).load().select(column_users)
df_badges = spark.read.format('bigquery').option('table',table_badges).load().select(column_badges)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Cleaning and Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Considerations:
# MAGIC - Answer dataset: not consider user_id null and answer_date null
# MAGIC - Badges dataset: not consider (date_badge - date_user_created) < 0

# COMMAND ----------

#Filtering only from 2020 to now
df_post_questions = df_post_questions.filter(year('creation_date')>=2020)
df_post_answers = df_post_answers.where((~col('user_id_answer').isNull()) & (year('answer_creation_date')>=2020) & (~col('answer_creation_date').isNull()))
df_badges = df_badges.filter(col('tag_based') == False)

# COMMAND ----------

#Joining Questions and Answers
df_users_answers = df_users.withColumnRenamed('user_id','user_id_answer')

df_post_questions_answers = df_post_questions\
                                .join(df_post_answers,df_post_questions.accepted_answer_id == df_post_answers.answer_id,how='left')\
                                .join(df_users_answers,df_post_answers.user_id_answer == df_users_answers.user_id_answer, how='left' )

# COMMAND ----------

#Joining Users and Badges
class_name = when(col('class') == 1,lit('gold'))\
             .when(col('class') == 2, lit('silver')).otherwise(lit('bronze'))

df_users_badges = df_users\
                    .join(df_badges,on=['user_id'],how='inner')\
                    .withColumn('class',class_name)\
                    .withColumn('days_after_creation',datediff(col('badge_creation_date'),col('user_creation_date')))

# COMMAND ----------

df_post_questions_answers_f = df_post_questions_answers.select(
                                                concat(year(col('creation_date')),lit('-'),month(col('creation_date'))).alias('year_month'),
                                                year(col('creation_date')).alias('year'),
                                                month(col('creation_date')).alias('month'),
                                                '*'
                                                )

display(df_post_questions_answers_f)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Final Tables to Export

# COMMAND ----------

#Let's create some custome aggregations

#For tag counting
sum_total_1_tag = sum(when(size(split(col('tags'),"\|")) == 1,lit(1)).otherwise(lit(0)))
sum_total_2_tag = sum(when(size(split(col('tags'),"\|")) == 2,lit(1)).otherwise(lit(0)))
sum_total_3_tag = sum(when(size(split(col('tags'),"\|")) == 3,lit(1)).otherwise(lit(0)))
sum_total_4_tag = sum(when(size(split(col('tags'),"\|")) == 4,lit(1)).otherwise(lit(0)))
sum_total_5m_tag = sum(when(size(split(col('tags'),"\|")) >= 5,lit(1)).otherwise(lit(0)))

#For questions with accepted answers
sum_quest_accepted_answer = sum(when(~col('accepted_answer_id').isNull(),lit(1)).otherwise(lit(0)))

#For delay of accepted answer
avg_days_accepted_answer = avg(datediff(col('answer_creation_date'),col('creation_date')))
max_days_accepted_answer = max(datediff(col('answer_creation_date'),col('creation_date')))
min_days_accepted_answer = min(datediff(col('answer_creation_date'),col('creation_date')))

#For answers average score and reputation
avg_answer_score = avg(col('answer_score'))
avg_answer_user_reputation = avg(col('reputation'))


#For percentiles in score
percentile_25_score = expr('percentile(answer_score, array(0.25))')[0]
percentile_50_score = expr('percentile(answer_score, array(0.50))')[0]
percentile_75_score = expr('percentile(answer_score, array(0.75))')[0]
percentile_100_score = expr('percentile(answer_score, array(1))')[0]


# COMMAND ----------

# MAGIC %md
# MAGIC #### 1- Table Post Question Level

# COMMAND ----------

df_post_questions_grouped = df_post_questions_answers_f\
            .groupBy('year','month')\
            .agg(count('*').alias('qty_questions'),
                 sum_quest_accepted_answer.alias('sum_questions_w_acceptedanswer'),
                 avg_days_accepted_answer.alias('avg_days_accepted_answer'),
                 max_days_accepted_answer.alias('max_days_accepted_answer'),
                 min_days_accepted_answer.alias('min_days_accepted_answer'),
                 sum('answer_count').alias('sum_answers'),
                 avg('answer_count').alias('avg_answers'),
                 sum('comment_count').alias('sum_comments'),
                 avg('comment_count').alias('avg_comments'),
                 sum('view_count').alias('sum_views'),
                 avg('view_count').alias('avg_views'),
                 sum_total_1_tag.alias('sum_total_1_tag'),
                 sum_total_2_tag.alias('sum_total_2_tag'),
                 sum_total_3_tag.alias('sum_total_3_tag'),
                 sum_total_4_tag.alias('sum_total_4_tag'),
                 sum_total_5m_tag.alias('sum_total_5m_tag'),
                 avg_answer_score.alias('avg_answer_score'),
                 avg_answer_user_reputation.alias('avg_answer_user_reputation')
                )\
            .orderBy(col('year').desc(),col('month').desc())

display(df_post_questions_grouped)


# COMMAND ----------

# MAGIC %md
# MAGIC #### 2- Table Post Question Tag Level

# COMMAND ----------

tag_name = explode(split(col('tags'),"\|"))

df_post_questions_grouped_tag = df_post_questions_answers_f\
    .select('*',tag_name.alias('tag_name'))\
    .groupBy('year','month','tag_name')\
    .agg(
        count('*').alias('qty_questions'),
         sum_quest_accepted_answer.alias('sum_questions_w_acceptedanswer'),
         avg_days_accepted_answer.alias('avg_days_accepted_answer'),
         max_days_accepted_answer.alias('max_days_accepted_answer'),
         min_days_accepted_answer.alias('min_days_accepted_answer'),
         sum('answer_count').alias('sum_answers'),
         avg('answer_count').alias('avg_answers'),
         sum('comment_count').alias('sum_comments'),
         avg('comment_count').alias('avg_comments'),
         sum('view_count').alias('sum_views'),
         avg('view_count').alias('avg_views'),
         avg_answer_score.alias('avg_answer_score'),
         avg_answer_user_reputation.alias('avg_answer_user_reputation')
    )\
    .orderBy(col('year').desc(),col('month').desc(),col('qty_questions').desc())

display(df_post_questions_grouped_tag)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3- Table Level Day of the Week

# COMMAND ----------

display(df_post_answers\
            .select('*',
                    year(col('answer_creation_date')).alias('year'),
                    month(col('answer_creation_date')).alias('month'),
                    dayofweek(col('answer_creation_date')).alias('day_number'),
                    date_format(col("answer_creation_date"), "EEEE").alias('answer_day')
                   )\
            .groupBy('year','month','day_number','answer_day')\
            .agg(count('*').alias('qty_answers'))\
            .orderBy(col('day_number').asc(),col('year').desc(),col('month').desc())
       )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 4- Badges Velocity to Earned

# COMMAND ----------

#class 3 -> bronze
#class 2 -> silver
#class 1 -> gold
percentile_25_days = expr('percentile(days_after_creation, array(0.25))')[0]
percentile_50_days = expr('percentile(days_after_creation, array(0.50))')[0]
percentile_75_days = expr('percentile(days_after_creation, array(0.75))')[0]
percentile_100_days = expr('percentile(days_after_creation, array(1))')[0]

display(
    df_users_badges\
        .select('*')\
        .where(col('days_after_creation') >= 0)\
        .groupBy('badge_name','class')\
        .agg(count('*').alias('qty_users'),
             avg('days_after_creation').alias('avg_days_after_creation'),
             min('days_after_creation').alias('min_days_after_creation'),
             max('days_after_creation').alias('max_days_after_creation'),
             percentile_25_days,
             percentile_50_days,
             percentile_75_days,
             percentile_100_days
            )\
        .orderBy(col('avg_days_after_creation').asc())
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5- Just in case, Ranking of tags per year and month

# COMMAND ----------

display(df_post_questions_grouped_tag\
       .select('*',dense_rank().over(Window.partitionBy('year','month').orderBy(desc('qty_questions'))).alias('rank'))\
       .filter("""rank <= 15""")\
        .orderBy(col('year').desc(),col('month').desc(),'rank')
       )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing Tables on BigQuery Bucket

# COMMAND ----------

bucket = 'gcs-dataengineer-projects'
table_post_questions_agg = 'lm-dataengineeringproject.de_dataset_stackoverflow.dm_post_questions_agg'

df_post_questions_grouped.write\
  .format("bigquery")\
  .option("temporaryGcsBucket", bucket)\
  .option("table", table_post_questions_agg)\
  .mode("overwrite").save()
