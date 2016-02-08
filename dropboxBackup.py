#!/usr/bin/python3

import cmd
import locale
import os
import pprint
import shlex
import sys
import json
import pickle
import hashlib
import copy
import functools
import getopt
import errno
import time
import urllib3

# Include the Dropbox SDK
import dropbox

from io import StringIO

from time import gmtime, strftime

# Get your app key and secret from the Dropbox developer website
# You can find these at http://www.dropbox.com/developers/apps
APP = {
	'key': '7epiizpxauu1mur',  # Setup your own key and secure
	'secret': 'gwdq39pa730p3ba'
}
TOKEN_FILE = 'dropboxToken'
BACKUP = {
	'root': 'backup',
	'main': 'main',
	'snapshots': 'snapshots',
	'folder': '/home/jam/Dokumenty'
}
IGNORED = {
	'paths': [
		'/bin',
		'/boot',
		'/dev',
		'/lib',
		'/lib64',
		'/lost+found',
		'/proc',
		'/run',
		'/sbin',
		'/srv',
		'/sys'
	],
	'folders': [
		'keys',
		'vendor',
		'.idea',
		'.git',
		'log',
		'tmp',
		'temp',
		'node_modules'
	],
	'files': [
		'Thumbs.db',
		'MATLAB-InternalConnector.log.1'
	]
}

class DropboxManager:
	def __init__(self, appKey, appSecret, tokenFile):
		self.appKey = appKey
		self.appSecret = appSecret
		self.tokenFile = os.path.expanduser('~') + '/' + tokenFile

		self.connection = self.connect()

	def connect(self):
		try:
			tokenFileReadStream = open(self.tokenFile, 'r')
			serializedTokenContent = tokenFileReadStream.read()
			tokenFileReadStream.close()

			accessTokenObject = json.loads(serializedTokenContent)
			if accessTokenObject['type'] == 'oauth2':
				accessToken = accessTokenObject['accessToken']
				apiClient = dropbox.client.DropboxClient(accessToken)
				print ("[loaded OAuth 2 access token]")
				apiClient.account_info()
				#print ('linked account: ', apiClient.account_info())

				return apiClient
			else:
				print ('Malformed access token in ' + self.tokenFile + '.')

		except (OSError, IOError, ValueError, dropbox.rest.ErrorResponse) as e:
			accessToken=self.getToken()

			self.saveToken(accessToken)

			return self.connect()

	def getToken(self):
		defaultApp = input("Change dropbox app? [Y/n]").strip()
		if defaultApp == 'n' or defaultApp == 'N':
			print('Setup own dropbox app key')
			self.appKey = input('Set up app key').strip()
			self.appSecret = input('Set up app secret').strip()

		flow = dropbox.client.DropboxOAuth2FlowNoRedirect(self.appKey, self.appSecret)

		# Have the user sign in and authorize this token
		authorizeUrl = flow.start()
		print ('1. Go to: ' + authorizeUrl)
		print ('2. Click "Allow" (you might have to log in first)')
		print ('3. Copy the authorization code.')
		authCode = input("Enter the authorization code here: ").strip()

		try:
			accessToken, userId = flow.finish(authCode)

			return accessToken
		except dropbox.rest.ErrorResponse as e:
			print ('Passed token is malformed\n')

			return self.getToken()

			pass

	def saveToken(self, accessToken):
		tokenFileStream = open(self.tokenFile, 'w+')
		tokenFileStream.write(json.dumps({'type': 'oauth2', 'accessToken': accessToken, 'app': { 'key': self.appKey, 'secret': self.appSecret }}, indent = 4))
		tokenFileStream.close()

	def getApiClient(self):
		return self.connection

class DropboxBackup:
	checksumMetaFile = '.dropboxMetaChecksum'
	repeatSleep = 3000

	def __init__(self, dropboxManager, backup):
		self.startTime = strftime("%Y-%m-%d %H:%M:%S", gmtime())

		self.dropboxManager = dropboxManager
		self.backupFolders = backup
		self.client = self.dropboxManager.getApiClient()

	def backup(self, backupFolder, ignored):
		try:
			if backupFolder in ignored['paths']:
				print ('Skipped ' + backupFolder) # skipping ignored path
				return

			ignored['files'].append(self.checksumMetaFile)

			checksum = self.getBackupChecksum(backupFolder)

			newChecksum = {}

			files = []
			directories = []
			for (dirpath, dirnames, filenames) in os.walk(backupFolder):
				files.extend(filenames)
				directories.extend(dirnames)
				break

			for (oneFile) in files:
				filePath = os.path.join(backupFolder, oneFile)
				if os.path.isfile(filePath):
					if oneFile in ignored['files']:
						if oneFile in newChecksum:
							del newChecksum[oneFile]

						continue # skipping ignored files
					newChecksum[oneFile] = self.generateChecksum(filePath)
					if newChecksum[oneFile] == None:
						print ('Unable sum hash: ' + filePath)
						del newChecksum[oneFile]

			if newChecksum != checksum:
				allChecksum = copy.copy(newChecksum)
				allChecksum.update(checksum)

				actualChecksum = copy.copy(checksum)

				try:
					for check in allChecksum:
						if (check in checksum):
							if (check not in newChecksum):
								self.snapshot(backupFolder, check)
								del actualChecksum[check]

							else:
								if checksum[check] == newChecksum[check]:
									continue
								else:
									self.snapshot(backupFolder, check)
									if self.upload(backupFolder, check):
										actualChecksum[check] = newChecksum[check]

									continue

						else:
							if self.upload(backupFolder, check):
								actualChecksum[check] = newChecksum[check]

					self.updateChecksum(backupFolder, newChecksum)
				except KeyboardInterrupt as e:
					kill = input('Do you realy want to kill it? [Y/n]')

					if kill == 'n' or kill == 'N':
						self.updateChecksum(backupFolder, actualChecksum)
						self.backup(backupFolder, ignored)
					else:
						self.updateChecksum(backupFolder, actualChecksum)

						raise e

			for (dirname) in self.getFolders(backupFolder):
				if dirname not in directories:
					self.snapshot(backupFolder + '/' + dirname, '')
					print(backupFolder + '/' + dirname)

			for (dirname) in directories:
				if dirname in ignored['folders']:
					print ('Skipped ' + backupFolder + '/' + dirname)

					continue

				if os.path.islink(backupFolder + '/' + dirname):
					print ('Skipped symlink ' + backupFolder + '/' + dirname)

					continue

				self.backup(backupFolder + '/' + dirname, ignored)

		except KeyboardInterrupt as e:
			kill = input('Do you realy want to kill it? [Y/n]')

			if kill == 'n' or kill == 'N':
				self.backup(backupFolder, ignored)
			else:

				raise e

	def getFolders(self, backupFolder):
		try:
			resp = self.client.metadata(self.backupFolders['root'] + '/' + self.backupFolders['main'] + '/' + backupFolder)
		except dropbox.rest.ErrorResponse as e:
			print (e)
			if e.status == 404:
				return []
			return self.getFolders(backupFolder)
		except urllib3.exceptions.MaxRetryError as e:
			print (e)
			time.sleep(self.repeatSleep)
			return self.getFolders(backupFolder)

		folders = []

		if 'contents' in resp:
			for f in resp['contents']:
				name = os.path.basename(f['path'])

				if name != self.checksumMetaFile and f['is_dir']:
					folders.append(name)

		return folders

	def getBackupChecksum(self, backupFolder):
		fullChecksumFile = self.backupFolders['root'] + '/' + self.backupFolders['main'] + '/' + backupFolder + '/' + self.checksumMetaFile

		try:
			checksumFile = self.client.get_file(fullChecksumFile)
			checksumFileContent = checksumFile.read()
			checksumFile.close()

			try :
				return json.loads(checksumFileContent.decode("utf-8"))
			except (TypeError, ValueError) as e:
				print(e)
				return {}
		except (dropbox.rest.ErrorResponse, dropbox.rest.ErrorResponse) as e:
			print (e)
		except urllib3.exceptions.MaxRetryError as e:
			print (e)
			time.sleep(self.repeatSleep)
			return self.getBackupChecksum(backupFolder)

		#f = open('/tmp/' + self.checksumMetaFile, 'w+')
		#f.write(json.dumps({}))
		#f.close();

		#f = open('/tmp/' + self.checksumMetaFile, 'rb')
		#try:
		#	response = self.client.put_file(fullChecksumFile, f)
		#except dropbox.rest.ErrorResponse as e:
		#	if e.status == 503:
		#		time.sleep(self.repeatSleep)
		#		self.getBackupChecksum(backupFolder)
		#finally:
		#	f.close()

		try:
			self.client.file_create_folder(self.backupFolders['root'] + '/' + self.backupFolders['main'] + '/' + backupFolder)
			print ('Create folder ' + self.backupFolders['root'] + '/' + self.backupFolders['main'] + '/' + backupFolder)
		except dropbox.rest.ErrorResponse as e:
			if e.status == 503:
				time.sleep(self.repeatSleep)
				return self.getBackupChecksum(backupFolder)
		except urllib3.exceptions.MaxRetryError as e:
			print (e)
			time.sleep(self.repeatSleep)
			return self.getBackupChecksum(backupFolder)

		return {}

	def generateChecksum(self, filePath, block_size=2**20):
		with open(filePath, mode='rb') as f:
			d = hashlib.md5()
			try:
				for buf in iter(functools.partial(f.read, 128), b''):
					d.update(buf)
				return d.hexdigest()
			except OSError as e:
				if e.errno == errno.EOVERFLOW:
					return None
				raise e

	def updateChecksum(self, backupFolder, newChecksum):
		fullChecksumFile = self.backupFolders['root'] + '/' + self.backupFolders['main'] + '/' + backupFolder + '/' + self.checksumMetaFile

		try:
			self.client.file_delete(fullChecksumFile)
		except dropbox.rest.ErrorResponse as e:
			pass

		f = open('/tmp/' + self.checksumMetaFile, 'w+')
		f.write(json.dumps(newChecksum))
		f.close();

		f = open('/tmp/' + self.checksumMetaFile, 'rb')
		try:
			response = self.client.put_file(fullChecksumFile, f)
		except dropbox.rest.ErrorResponse as e:
			if e.status == 503:
				time.sleep(self.repeatSleep)
				self.updateChecksum(backupFolder, newChecksum)
		except urllib3.exceptions.MaxRetryError as e:
			print (e)
			time.sleep(self.repeatSleep)
			self.updateChecksum(backupFolder, newChecksum)
		finally:
			f.close()

	def upload(self, directory, f):
		pathTo = self.backupFolders['root'] + '/' + self.backupFolders['main'] + '/' + directory + '/' + f
		pathFrom = directory + '/' + f

		fromFile = open(pathFrom, 'rb')

		try:
			#print(pathTo, fromFile);

			#self.client.put_file(pathTo, fromFile, overwrite=True)

			size = os.path.getsize(pathFrom)
			uploader = self.client.get_chunked_uploader(fromFile, size)
			print ("uploading: ", size, ",", pathFrom)
			isOk = True
			while uploader.offset < size:
				try:
					print(uploader.offset)
					upload = uploader.upload_chunked(4*1024*1024)
				except dropbox.rest.ErrorResponse as e:
					print (e)
					isOk = False
					# perform error handling and retry logic

			if isOk:
				uploader.finish(pathTo, overwrite=True)
			else:
				time.sleep(self.repeatSleep)
				return self.upload(directory, f)
		except dropbox.rest.ErrorResponse as e:
			if e.status == 503:
				time.sleep(self.repeatSleep)
				return self.upload(directory, f)
		except urllib3.exceptions.MaxRetryError as e:
			print (e)
			time.sleep(self.repeatSleep)
			return self.upload(directory, f)

		if isOk:
			print('uploaded ' + directory + '/' + f)

		return isOk

	def snapshot(self, directory, f):
		pathFrom = self.backupFolders['root'] + '/' + self.backupFolders['main'] + '/' + directory + '/' + f
		pathTo = self.backupFolders['root'] + '/' + self.backupFolders['snapshots'] + '/' + self.startTime + '/' + directory + '/' + f

		try:
			self.client.file_move(pathFrom, pathTo)
		except dropbox.rest.ErrorResponse as e:
			if e.status == 503:
				time.sleep(self.repeatSleep)
				self.snapshot(directory, f)
		except urllib3.exceptions.MaxRetryError as e:
			print (e)
			time.sleep(self.repeatSleep)
			self.snapshot(directory, f)

		print('snapshot ' + directory + '/' + f + ' ' + self.startTime)

def main():
	global APP, TOKEN_FILE, BACKUP, IGNORED

	try:
		opts, args = getopt.getopt(sys.argv[1:], "hd:", ["help", "dir="])
	except getopt.GetoptError:
		usage()
		sys.exit(2)

	for opt, arg in opts:
		if opt in ("-h", "--help"):
			usage()
			sys.exit()
		elif opt in ("-d", "--dir"):
			BACKUP['folder'] = arg

	dropbox = DropboxManager(APP['key'], APP['secret'], TOKEN_FILE)

	dropboxBackup = DropboxBackup(dropbox, BACKUP)

	try:
		dropboxBackup.backup(BACKUP['folder'], IGNORED)
	except KeyboardInterrupt as e:
		exit()

main()
