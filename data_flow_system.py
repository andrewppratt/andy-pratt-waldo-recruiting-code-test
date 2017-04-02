#!/usr/bin/python
import urllib
import urllib2
import xml.etree.ElementTree as ET
import piexif
import exifread
from pymongo import MongoClient
import pymongo.errors

req = urllib2.Request('https://s3.amazonaws.com/waldo-recruiting')
try: response = urllib2.urlopen(req)
except urllib2.HTTPError as e:
	print e.reason
except urllib2.URLError as e:
	print e.reason
else:
	f = response.read()
# TODO Authenticate xml or other security precaution

# create element tree structure list of photos
root = ET.fromstring(f)
ns = '{http://s3.amazonaws.com/doc/2006-03-01/}'
photos = []
for contents in root.findall(ns + 'Contents'):
	photos.append(contents.find(ns + 'Key').text)

#create mongo connection
client = MongoClient()
db = client.test
db.photos.ensure_index("photo_name", unique=True)

# Get photos and parse EXIF info
# Performance issues here. Downloading entire photo file for small amount of text is not fast.
# Ideas: 
# 1. Thread the download and create a separate thread to parse exif info and another to update the db
# 2. Download only the first chunk of photo * assumes exif info is at the beginning of the file and has a finite size 

for photo in photos:
	req = urllib2.Request('https://s3.amazonaws.com/waldo-recruiting' + '/' + photo)
	try: response = urllib2.urlopen(req)
	except urllib2.HTTPError as e:
        	print e.reason
	except urllib2.URLError as e:
        	print e.reason
	else:
		# Add check for file type photo (jpeg, tiff and whatever else has exif info
		fh = open('photos/'+photo, 'w')
		fh.write(response.read())
		fh.close()
		f = open('photos/'+photo)
		tags = exifread.process_file(f)
		picDict = {}
		picDict["photo_name"] = photo
		print photo
		for tag in tags.keys():
    			if tag not in ('JPEGThumbnail', 'TIFFThumbnail', 'Filename', 'EXIF MakerNote'):
				try:
					picDict[unicode(tag).encode('utf-8')] = tags[tag].printable.encode('utf-8')
				except Exception as e: # Better error handling is in order here
					pass
	# Insert photo info into mongodb
		try: result = db.photos.insert_one(picDict)
		except pymongo.errors.DuplicateKeyError as e:
			print e.message + 'No Insert'
			pass
		else:
			print result
