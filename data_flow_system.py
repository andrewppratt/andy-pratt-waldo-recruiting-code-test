#!/usr/bin/python
import urllib
import xml.etree.ElementTree as ET
import piexif
from pymongo import MongoClient

opener = urllib.URLopener()
myurl = "https://s3.amazonaws.com/waldo-recruiting"

# write in exception hadling here - might not connect

myfile = opener.open(myurl)
f = myfile.read()
opener.close
# Authenticate xml or other security precaution

# create element tree structure list of photos
root = ET.fromstring(f)
ns = '{http://s3.amazonaws.com/doc/2006-03-01/}'
photos = []
for contents in root.findall(ns + 'Contents'):
	photos.append(contents.find(ns + 'Key').text)

#create mongo connection
client = MongoClient()
db = client.test


# Get photos and parse EXIF info
# Performance issues here. Downloading entire photo file for small amount of text is not fast.
# Ideas: 
# 1. Thread the download and create a separate thread to parse exif info and another to update the db
# 2. Download only the first chunk of photo * assumes exif info is at the beginning of the file and has a finite size 


# TRY EXCEPt
myurl = myurl +'/' + photos[1]
print myurl
photolink = opener.open(myurl) 
photo = photolink.read()
exif_dict = piexif.load(photo)
json = {}
json["photo_name"] = photos[1]

# Needs handling for integers and datetime
db.photos.ensure_index("photo_name", unique=True)
for ifd in ("0th", "Exif", "GPS", "1st"):
	for tag in exif_dict[ifd]:
        #	print(piexif.TAGS[ifd][tag]["name"], exif_dict[ifd][tag])
	# Create a photo object to insert into mongodb
		json['"' + piexif.TAGS[ifd][tag]["name"] + '"'] = exif_dict[ifd][tag]
print json
# Insert photo info into mongodb
result = db.photos.insert_one(json)
print result
cursor = db.photos.find({"photo_name": "0005D22E-0B97-4FF5-B220-B80A1C389D56.839c4a2b-2fcb-4123-8b41-cad084c35896.jpg"})
for document in cursor:
	print document






# connect to mysql and create a new db
# create an external config.py file for credentials
#try:
#	dbcon = mysql.connector.connect(user='root', password='waldo',
#				host='localhost')
#except mysql.connector.Error as err:
#	if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
#    		print("Something is wrong with your user name or password")
#  	elif err.errno == errorcode.ER_BAD_DB_ERROR:
#    		print("Database does not exist")
#  	else:
#    		print(err)
#
#dbcon.close

# create table based on element tree structure
# be sure to handle injection attacks


# populate table

