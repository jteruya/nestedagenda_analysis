#!/usr/bin/python

import psycopg2
import traceback
import csv

import psycopg2.extras

# Robin (US) Conection Info
database = 'analytics'
user = 'etl'
host = '10.223.192.6'
password = 's0.Much.Data'

try:
	conn = psycopg2.connect(dbname=database, user=user, host=host, password=password)
except psycopg2.Error as e:
	print "I am unable to connect to the database."
	print e
	#print e.pgcode
    #print e.pgerror
    #print traceback.format_exc()

cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

try:
	cur.execute("""SELECT Application_Id, Global_User_Id, Created, Identifier, CAST(Metadata->>'ItemId' AS INT) AS ItemId, Metric_Type, NULL AS ViewItemId
                   FROM JT.Retro_Nested_Bookmarks_Android_ItemView_Corr
			       WHERE Device_Type = 'android'
				   ORDER BY 1,2,3
                ;""")
except:
	"Can't run SQL"

rows = cur.fetchall()

result_file = open('csv/result_file.csv', 'wb')
wr = csv.writer(result_file)

view_item_id = 0
application_id = ''
global_user_id = ''

for row in rows:
	if row['identifier'] == 'item':
		view_item_id = row['itemid']
		application_id = row['application_id']
		global_user_id = row['global_user_id']
		row['viewitemid'] = 0
	if row['identifier'] == 'bookmarkButton' and row['application_id'] == application_id and row['global_user_id'] == global_user_id:
		row['viewitemid'] = view_item_id
	wr.writerow(row)

#cur.executemany("""INSERT INTO JT.Retro_Nested_Bookmarks_Android_ItemView_Corr_Final (application_id, global_user_id, created, identifier, itemid, metric_type, viewitemid) VALUES (%(application_id)s, %(global_user_id)s, %(created)s, %(identifier)s, %(itemid)s, %(metric_type)s, %(viewitemid)s)""", rows)

result_file.close()

read_result_file = open('/Users/jonathanteruya/repo/nestedagenda_analysis/csv/result_file.csv', 'r')

cur.copy_from(read_result_file, "JT.Retro_Nested_Bookmarks_Android_ItemView_Corr_Final", sep=',')
#cur.copy_expert("\copy from /Users/jonathanteruya/repo/nestedagenda_analysis/csv/result_file.csv to JT.Retro_Nested_Bookmarks_Android_ItemView_Corr_Final with csv header", read_result_file)

#cur.execute("""psql -h 10.223.192.6 -p 5432 -A -t analytics etl \copy JT.Retro_Nested_Bookmarks_Android_ItemView_Corr_Final from '/Users/jonathanteruya/repo/nestedagenda_analysis/csv/result_file.csv' USING DELIMITERS ',' CSV;""")
conn.commit()

read_result_file.close()

# Close DB Connection
cur.close()
conn.close()


#for row in rows:
#	if row[3] == 'item':
#		view_item_id = row[4]
#	else:
#		row[6] = view_item_id
