#!/usr/bin/python
import urllib
import urllib2
import xml.etree.ElementTree as ET
import piexif
import exifread
from pymongo import MongoClient
import pymongo.errors
import Queue
import threading

# Get XML list of photos
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
q = Queue.Queue()
for contents in root.findall(ns + 'Contents'):
	file_name = contents.find(ns + 'Key').text
	if file_name[-3:] in ('jpg', 'JPG', 'tif', 'TIF', 'wav', 'WAV'):
		q.put(file_name)

#create mongo connection
client = MongoClient()
db = client.test
db.photos.ensure_index('photo_name', unique=True)

# Get photos and parse EXIF info
def worker(queue):
	queue_full = True
	while queue_full:
		try:
			file_name = queue.get(False)
			req = urllib2.Request('https://s3.amazonaws.com/waldo-recruiting' + '/' + file_name)
			try: response = urllib2.urlopen(req)
			except urllib2.HTTPError as e:
				print 'File download error: ' + e.reason + '\n'
			except urllib2.URLError as e:
				print 'File download error: ' + e.reason + '\n'
			else:
				# I couldn't figure out how to open a url with binary transfer - there's got to be a way
				# So I saved locally and reopened - which seems wastefull.
				fh = open('photos/'+file_name, 'w')
				fh.write(response.read())
				fh.close()
				f = open('photos/'+file_name)
				tags = exifread.process_file(f)
				picDict = {}
				picDict['photo_name'] = file_name 
				print 'Processing Image: ' + file_name 
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
					print 'Inserting into DB:'
					print result
					print '\n'
		except Queue.Empty:
			queue_full = False


thread_count = 5
for i in range(thread_count):
    t = threading.Thread(target=worker, args = (q,))
    t.start()
