import psycopg2
#connect to postgres database - this returns a postgres connection object
connection = psycopg2.connect(user = 'postgres',
                             password = 'postgres',
                             host = '127.0.0.1',
                             port = '5432',
                             database = 'postgres')
#create a cursor object that lets us execute postgres commands through python console
cursor = connection.cursor()

#define query for reading in owner name and address data - limit to only records that are larger than 1 acre in size and have
#a total assessed value less than 2* the land assessed value
readData = "select * from (select \
                string_agg(distinct cast(id as varchar), ', ') as ids, \
                string_agg(distinct cast(std_owner as varchar), ', ') as owns, \
                string_agg(distinct cast(std_addr as varchar), ', ') as adds, \
                string_agg(distinct cast(geom as varchar), ', ') as geoms, \
                round(sum(calc_acres)::numeric,2) as sum_acres, \
                count(id) as num \
                from parcels.rpscentroids \
                where calc_acres>1 and total_av<2*land_av \
                group by geom \
                order by num desc) as foo \
                where num>1"

cursor.execute(readData)

#retrieve results of the query
readTable = cursor.fetchall()

dupList = list()
for row in readTable:
    currIds = row[0].split(', ')
    for currId in currIds:
        dupList.append(currId)
print(len(dupList))
