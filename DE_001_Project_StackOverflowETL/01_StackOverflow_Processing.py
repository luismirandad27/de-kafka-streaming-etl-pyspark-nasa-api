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

df_post_questions = df_post_questions.select(columns_post_questions).filter(year('creation_date')>=2020)

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
# MAGIC #### 1- Most frequent questions per technology (tags) - Top 15

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
