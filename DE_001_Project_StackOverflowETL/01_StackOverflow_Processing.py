# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

#loading the datasets
table_post_questions = 'bigquery-public-data.stackoverflow.posts_questions'
table_post_answers = 'bigquery-public-data.stackoverflow.posts_answers'
table_users = 'bigquery-public-data.stackoverflow.users'
table_badges = 'bigquery-public-data.stackoverflow.badges'

# COMMAND ----------

#storing data into dataframes
df_post_questions = spark.read.format('bigquery').option('table',table_post_questions).load()
df_post_answers = spark.read.format('bigquery').option('table',table_post_answers).load()
df_users = spark.read.format('bigquery').option('table',table_users).load()
df_badges = spark.read.format('bigquery').option('table',table_badges).load()

# COMMAND ----------

#Post Questions Metadata
df_post_questions.printSchema()

column_post_questions = ['id','title','accepted_answer_id','answer_count','comment_count',
                          'creation_date','owner_user_id','score','tags','view_count']

df_post_questions = df_post_questions.select(column_post_questions).filter(year('creation_date')>=2020)

# COMMAND ----------

#Post Answers Metadata
df_post_answers.printSchema()

columns_post_answers = ['id','creation_date','owner_user_id','score','post_type_id']

display(df_post_answers.select(columns_post_answers).filter("""owner_user_id is not null"""))

# COMMAND ----------

#Users metadata
df_users.printSchema()

column_users = ['id','display_name','reputation','location','up_votes','down_votes','views']

display(df_users.select(column_users))

# COMMAND ----------

#Badge metadata
df_badges.printSchema()

column_badges = ['id','name','date','user_id','class','tag_based']

display(df_badges.select(column_badges).filter("""tag_based = false""").groupBy('class').agg(count('*')))

# COMMAND ----------

# MAGIC %md
# MAGIC #### After reviewing the tables, let´s see what types of columns would be interesting
# MAGIC For Posts Questions:
# MAGIC | Column Name | Description |
# MAGIC |-------------|-------------|
# MAGIC |id|             |
# MAGIC |title|             |
# MAGIC |accepted_answer_id|to get the accepted answer detail|
# MAGIC |comment_count|maybe to see the difficulty|
# MAGIC |creation_date|             |
# MAGIC |favorite_count|to see how many people found insightful or a common issue for many developers|
# MAGIC |owner_user_id|             |
# MAGIC |score|             |
# MAGIC |tags|             |
# MAGIC |view_count|             |
# MAGIC 
# MAGIC For Post Answers:
# MAGIC | Column Name | Description |
# MAGIC |-------------|-------------|
# MAGIC |id|             |
# MAGIC |creation_date|             |
# MAGIC |owner_user_id||
# MAGIC |score||
# MAGIC 
# MAGIC For Users:
# MAGIC | Column Name | Description |
# MAGIC |-------------|-------------|
# MAGIC |id|             |
# MAGIC |display_name|             |
# MAGIC |reputation||
# MAGIC |up_votes||
# MAGIC |down_votes||
# MAGIC |views||
# MAGIC 
# MAGIC For Badges:
# MAGIC | Column Name | Description |
# MAGIC |-------------|-------------|
# MAGIC |id|             |
# MAGIC |name|             |
# MAGIC |date||
# MAGIC |user_id||
# MAGIC |class||
# MAGIC |tag_based||

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1- Table Post Question Level

# COMMAND ----------

#Joining Questions and Answers
column_post_a = [col('id').alias('id_answer'),col('creation_date').alias('answer_date'),col('owner_user_id').alias('user_id_answer'),
                 col('score').alias('answer_score')]

df_post_answers = df_post_answers.select(column_post_a)

df_post_questions_answers = df_post_questions\
                                .join(df_post_answers,df_post_questions.accepted_answer_id == df_post_answers.id_answer,how='left')

# COMMAND ----------

display(df_post_questions_answers.select(col('id'),col('title'),col('tags'),col('creation_date'),col('answer_date'),
                                datediff(col('answer_date'),col('creation_date'))))

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
avg_days_accepted_answer = avg(datediff(col('answer_date'),col('creation_date')))
max_days_accepted_answer = max(datediff(col('answer_date'),col('creation_date')))
min_days_accepted_answer = min(datediff(col('answer_date'),col('creation_date')))


# COMMAND ----------

df_post_questions_grouped = df_post_questions_answers\
            .select('*',
                    explode(split(col('tags'),"\|")).alias('tag_name'),
                    year(col('creation_date')).alias('year'),
                    month(col('creation_date')).alias('month')
                   )\
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
                 sum_total_5m_tag.alias('sum_total_5m_tag')
                )\
            .orderBy(col('year').desc(),col('month').desc())

display(df_post_questions_grouped)


# COMMAND ----------

df_post_questions_grouped_tag = df_post_questions\
    .select(explode(split(col('tags'),"\|")).alias('tag_name'),
            year(col('creation_date')).alias('year'),
            month(col('creation_date')).alias('month')
           )\
    .groupBy('year','month','tag_name')\
    .agg(count('*').alias('qty_questions'))\
    .orderBy(col('year').desc(),col('month').desc(),col('qty_questions').desc())

display(df_post_questions_grouped_tag\
       .select('*',dense_rank().over(Window.partitionBy('year','month').orderBy(desc('qty_questions'))).alias('rank'))\
       .filter("""rank <= 15""")\
        .orderBy(col('year').desc(),col('month').desc(),'rank')
       )


# COMMAND ----------

display(df_post_questions\
    .select(explode(split(col('tags'),"\|")).alias('tag_name'),
            year(col('creation_date')).alias('year')
           )\
    .groupBy('year','tag_name')\
    .agg(count('*').alias('qty_questions'))\
    .orderBy(col('year').desc(),col('qty_questions').desc())
       )
