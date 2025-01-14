import pandas as pd
import requests
import psycopg2

#fetching the data
# raw_date = requests.get("https://storage.googleapis.com/generall-shared-data/startups_demo.json")

# #storing data into a file
# with open("raw_files/raw_data.json", 'w')as f:
#     f.write(raw_date.text)

# #reading the same data file into a data frame
raw_df = pd.read_json("raw_files/raw_data.json", lines =True)
print(raw_df.sample(10))


#transformation time, will take only those starups that are based on NYC and has HTTPs images meaning they are secure


secure_startups_df = raw_df.loc[(raw_df['link'].str.contains("https")) & (raw_df['city']=='New York')].sort_values("name").reset_index(drop=True)

print(secure_startups_df.sample(10))
#checking whether the above df is working or not
# print(raw_df.count())
# print(secure_startups_df.count())

# print(secure_startups_df['city'].unique())
# print(secure_startups_df['city'].nunique())


#Creating a connection with postgreSQL database
#remove the real credentials

pg_conn = psycopg2.connect(
            user = 'user',
            password = 'password',
            host='localhost',
            port='port',
            database='database'
)

curr = pg_conn.cursor()

#curr.execute("select * from table_name")

#creating a table

curr.execute('CREATE TABLE IF NOT EXISTS SECURE_STARUPS(id bigserial PRIMARY KEY, name VARCHAR(400), image varchar(400), alt varchar(400), description varchar(4000), link varchar(400), city varchar(100))")')


for row in secure_startups_df.itertuples():
    curr.execute("INSERT INTO SECURE_STARTUPS (NAME, IMAGES, ALT, DESCRIPTION, LINK, CITY) VALUES ('{}','{}','{}','{}','{}','{}','{}')".format(row.name.replace("'","'"), row.images, row.alt.replace("'","''"), row.description.replace("'","''"), row.link, row.city))

pg_conn.commit()
curr.close()
pg_conn.close()