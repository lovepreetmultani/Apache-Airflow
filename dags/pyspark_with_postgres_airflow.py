#import required libraries
import pyspark

#create spark session
spark=pyspark.sql.SparkSession \
 .builder \
 .appName("Pyspark SQL basic example") \
 .config('spark.driver.extraClassPath',"/Users/lovepreetmultani/Downloads/postgresql-42.6.0.jar") \
 .getOrCreate()

#read table from spark using jdbc
def extract_movies_to_df():
    movies_df= spark.read \
        .format("jdbc") \
        .option("url","jdbc:postgresql://localhost:5432/airflow") \
        .option("dbtable","movies") \
        .option("user","postgres") \
        .option("password","postgres") \
        .option("driver", "org.postgresql.Driver") \
        .load()
    return movies_df

def extract_users_to_df():
    users_df= spark.read \
        .format("jdbc") \
        .option("url","jdbc:postgresql://localhost:5432/airflow") \
        .option("dbtable","users") \
        .option("user","postgres") \
        .option("password","postgres") \
        .option("driver", "org.postgresql.Driver") \
        .load()
    return users_df

#transforming tables
def transform_avg_ratings(movies_df,users_df):
    avg_rating=users_df.groupBy("movie_id").mean("rating")
    df=movies_df.join(avg_rating,movies_df.id==avg_rating.movie_id)
    df=df.drop("movie_id")
    return df

#Save the transformed dataframe to the database 
def load_to_db(df):
    mode="overwrite"
    url="jdbc:postgresql://localhost:5432/airflow"
    properties= {"user":"postgres",
        "password":"postgres",
        "driver": "org.postgresql.Driver"
         }
    df.write.jdbc(url=url,
                  table="avg_ratings",
                  mode=mode,
                  properties=properties)

if __name__=="__main__":
    movies_df=extract_movies_to_df()
    users_df=extract_users_to_df()
    ratings_df=transform_avg_ratings(movies_df,users_df)
    load_to_db(ratings_df)