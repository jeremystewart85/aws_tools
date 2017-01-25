#!/usr/bin/env python
import boto3
import optparse
from time import sleep

#Command line argument handling
parser = optparse.OptionParser()

test_bucket = "terraftest"

parser.add_option("-b", "--bucket",
                    type='string',
                    action='store',
                    help="Bucket location to check for encryption",
                    dest="bucket")

parser.add_option('-p', '--prefix',
                    type='string',
                    action='store',
                    help="[OPTIONAL] Prefix to check for encryption if you don't want to check the entire bucket",
                    dest="bucket_prefix",
                    default="")

parser.add_option('-t', '--test',
					action="store_true",
					dest="testing",
					help="Testing mode, ignores all other flags. Will run unit tests on a set of generated files to ensure operation is as expected. **Only works on " + test_bucket + " bucket!",
					default=False)

parser.add_option('-e', '--encrypted',
					action="store_true",
					dest="show_encrypted",
					help="Will output currently encrypted files. If combined with the -v flag, it will also check all versions of the files and output their encryption status",
					default=False)

parser.add_option('-u', '--unencrypted',
					action="store_true",
					dest="show_unencrypted",
					help="Will output currently unencrypted files. If combined with the -v flag, it will also check all versions of the files and output their encryption status",
					default=False)

parser.add_option('-v', '--versions',
					action="store_true",
					dest="show_versions",
					help="Include all file versions and their encryption status. Must be combined with -e | --encrypted and/or -u | --unencrypted.",
					default=False)

parser.add_option('-r', '--remove-unencrypted',
					action="store_true",
					dest="remove_unencrypted",
					help="If unencrypted versions of a file are found, remove them. Must be used in combination with -f | --force-encrypt. This will be performed after creating a new encrypted version of the file so the most current version will be encrypted.",
					default=False)

parser.add_option('-f', '--force-encrypt',
					action="store_true",
					dest="force_encrypt",
					help="If unencrypted file is found, encrypt it.",
					default=False)

options, args = parser.parse_args()

if not options.bucket:
    parser.error('Bucket not given')
if options.testing and options.bucket != test_bucket:
    parser.error('Improper test bucket given, only ' + test_bucket + ' allowed.')
if options.remove_unencrypted and not options.force_encrypt:
	parser.error('Option to remove unencrypted files must be combined with the option to force encryption otherwise current files would be deleted!')
if options.show_versions and not (options.show_unencrypted or options.show_encrypted):
	parser.error('Option to show versions must be combined with showing unencrypted and/or encrypted files')

#global s3 objects
s3_client = boto3.client('s3')
#need to use paginator+iterator so we can retrieve a full list of all files in the bucket/prefix location
s3_paginator = s3_client.get_paginator('list_objects_v2')
s3_iterator = s3_paginator.paginate(Bucket=options.bucket,Prefix=options.bucket_prefix)
key_obj_list = []
for page in s3_iterator:
	if "Contents" in page:
		for key_obj in page['Contents']:
			#skip over "folder" objects
			if not key_obj['Key'].endswith('/'):
				key_obj_list.append(key_obj)

#function defs
def clean_test_bucket():
	if options.bucket != test_bucket:
		print "Improper test bucket, will not continue"
	else:
		for key_obj in key_obj_list:
			versions_list = s3_client.list_object_versions(Bucket=options.bucket,Prefix=key_obj['Key'])['Versions']		
			for key_version in versions_list:
				s3_client.delete_object(Bucket=options.bucket,Key=key_obj['Key'],VersionId=key_version['VersionId'])
	return;

def regen_test_bucket():
	if options.bucket != test_bucket:
		print "Improper test bucket, will not continue"		
	else:
		#Always encrypted file, no versions
		s3_client.put_object(
			Bucket=options.bucket,
			Body='always encrypted',
			Key='test_alwaysencrypted/test_always_encrypted1.txt',
			ServerSideEncryption='AES256'
		)
		#Always encrypted file w/ 2 versions
		s3_client.put_object(
			Bucket=options.bucket,
			Body='always encrypted v1',
			Key='test_alwaysencrypted/test_always_encrypted2.txt',
			ServerSideEncryption='AES256',
			Metadata={'version':'1'}
		)
		make_encrypted('test_alwaysencrypted/test_always_encrypted2.txt')

		#File that was later encrypted (only possible w/ versions)
		s3_client.put_object(
			Bucket=options.bucket,
			Body='encrypted after initial version v1',
			Key='test_postencrypted/test_post_encrypted1.txt',
			Metadata={'version':'1'}
		)
		make_encrypted('test_postencrypted/test_post_encrypted1.txt')

		#File that was never encrypted, no versions
		s3_client.put_object(
			Bucket=options.bucket,
			Body='encrypted after initial version',
			Key='test_noencrypt/test_no_encrypt1.txt'
		)
		#File that was never encrypted w/ versions
		s3_client.put_object(
			Bucket=options.bucket,
			Body='encrypted after initial version v1',
			Key='test_noencrypt/test_no_encrypt2.txt',
			Metadata={'version':'1'}
		)
		#can't use function because copy_object requires at least a metadata change
		s3_client.copy_object(
			Bucket=options.bucket,
			CopySource=options.bucket+'/test_noencrypt/test_no_encrypt2.txt',			
			Key='test_noencrypt/test_no_encrypt2.txt',
			Metadata={'version':'2'},
			MetadataDirective='REPLACE'
		)
		#sleep for a moment so the files have time to marinate in s3
		sleep(10)
	return;

def get_encrypted_keys():
	#will return a collection of encrypted keys for a bucket+prefix
	encrypted_list = []
	for key in key_obj_list:		
		key_head = s3_client.head_object(Bucket=options.bucket,Key=key['Key'])
		if 'ServerSideEncryption' in key_head:
			encrypted_list.append(key['Key'])
	return encrypted_list;

def get_unencrypted_keys():
	#will return a collection of unencrypted keys for a bucket+prefix
	unencrypted_list = []
	for key in key_obj_list:		
		key_head = s3_client.head_object(Bucket=options.bucket,Key=key['Key'])
		if 'ServerSideEncryption' not in key_head:			
			unencrypted_list.append(key['Key'])
	return unencrypted_list;

def get_unencrypted_versions( key_name ):
	#will return a collection of unencrypted version ids for a particular key
	unencrypted_version_ids = []
	#print "Checking status of: " + key_name
	versions_list = s3_client.list_object_versions(Bucket=options.bucket,Prefix=key_name,Delimiter=".")['Versions']
	for v in versions_list:
		version_id = v['VersionId']
		try:
			cur_file = s3_client.head_object(Bucket=options.bucket,Key=key_name,VersionId=version_id)
		except Exception as inst:
			print "Exception getting header on file: " + key_name + " VersionId: " + version_id
			raise
		if not 'ServerSideEncryption' in cur_file:			
			unencrypted_version_ids.append(version_id)
	return unencrypted_version_ids

def get_encrypted_versions( key_name ):
	#will return a collection of encrypted version ids for a particular key
	encrypted_version_ids = []
	#print "Checking status of: " + key_name
	versions_list = s3_client.list_object_versions(Bucket=options.bucket,Prefix=key_name,Delimiter=".")['Versions']
	for v in versions_list:
		version_id = v['VersionId']
		try:
			cur_file = s3_client.head_object(Bucket=options.bucket,Key=key_name,VersionId=version_id)
		except Exception as inst:
			print "Exception getting header on file: " + key_name + " VersionId: " + version_id
			raise
		if 'ServerSideEncryption' in cur_file:
			encrypted_version_ids.append(version_id)
	return encrypted_version_ids

def make_encrypted( key_name ):
	s3_client.copy_object(
			Bucket=options.bucket,
			CopySource=options.bucket+'/'+key_name,			
			Key=key_name,
			ServerSideEncryption='AES256'
		)

def rm_unencrypted_versions( key_name, version_id ):
	s3_client.delete_object(
			Bucket=options.bucket,
			Key=key_name,
			VersionId=version_id
		)
	return key_name

#tests only run if -t present in options
if options.testing:
	print "************************"
	print "Refreshing workspace: " + options.bucket
	clean_test_bucket()
	print "************************"
	print "Creating test files in: " + options.bucket
	regen_test_bucket()
	print "************************"
	print "Getting currently encrypted files:"
	print ""
	encrypted_list = get_encrypted_keys()
	for f in encrypted_list:
		print options.bucket + "/" + f + " is encrypted"
		for ev in get_encrypted_versions(f):
			print "\tVersion: " + ev + " is encrypted"
		for uv in get_unencrypted_versions(f):
			print "\tVersion: " + uv + " is unencrypted"
	print "************************"
	print "Getting currently unencrypted files:"
	print ""
	unencrypted_list = get_unencrypted_keys()
	for f in unencrypted_list:
		print options.bucket + "/" + f + " is unencrypted"
		for ev in get_encrypted_versions(f):
			print "\tVersion: " + ev + " is encrypted"
		for uv in get_unencrypted_versions(f):
			print "\tVersion: " + uv + " is unencrypted"
	print ""
	print "************************"
	print "Encrypting all currently unencrypted files:"
	print ""
	for f in unencrypted_list:
		make_encrypted(f)
		for ev in get_encrypted_versions(f):
			print "Key: " + f + " Version: " + ev + " is now encrypted"
	print "************************"
	print "Removing all previous unencrypted versions:"
	print ""
	options.remove_unencrypted = True
	for f in key_obj_list:
		versions=get_unencrypted_versions(f['Key'])
		for v in versions:
			print rm_unencrypted_versions(f['Key'],v) + " Version: " + v + " has been removed"
	print "************************"
	print "Final state:"
	print ""
	#let the files marinate	in S3 for a bit to make sure they have settled
	sleep(5)
	#rescan for all files and print out the status of all versions of the file
	encrypted_list = get_encrypted_keys()
	unencrypted_list = get_unencrypted_keys()
	for f in encrypted_list:
		print options.bucket + "/" + f + " is encrypted"
		for ev in get_encrypted_versions(f):
			print "\tVersion: " + ev + " is encrypted"
		for uv in get_unencrypted_versions(f):
			print "\tVersion: " + uv + " is unencrypted"
	for f in unencrypted_list:
		print options.bucket + "/" + f + " is unencrypted"
		for ev in get_encrypted_versions(f):
			print "\tVersion: " + ev + " is encrypted"
		for uv in get_unencrypted_versions(f):
			print "\tVersion: " + uv + " is unencrypted"
#end test cases

else:
	if options.show_encrypted:
		encrypted_list = get_encrypted_keys()
		for f in encrypted_list:
			print options.bucket + "/" + f + " is encrypted"
			if options.show_versions:
				for ev in get_encrypted_versions(f):
					print "\tVersion: " + ev + " is encrypted"
				for uv in get_unencrypted_versions(f):
					print "\tVersion: " + uv + " is unencrypted"
	if options.show_unencrypted:
		unencrypted_list = get_unencrypted_keys()
		for f in unencrypted_list:
			print options.bucket + "/" + f + " is unencrypted"
			if options.show_versions:
				for ev in get_encrypted_versions(f):
					print "\tVersion: " + ev + " is encrypted"
				for uv in get_unencrypted_versions(f):
					print "\tVersion: " + uv + " is unencrypted"
	if options.force_encrypt:
		unencrypted_list = get_unencrypted_keys()
		for f in unencrypted_list:
			file_size = s3_client.list_objects_v2(Bucket=options.bucket,Delimiter=".",Prefix=f)['Contents']['Size']
			print "Filesize is: " + file_size
			if file_size < 50000000:				
				make_encrypted(f)
			else:
				continue
			for ev in get_encrypted_versions(f):
				print "Key: " + f + " Version: " + ev + " is now encrypted"
	if options.remove_unencrypted:
		for f in key_obj_list:
			versions=get_unencrypted_versions(f['Key'])
			for v in versions:
				print rm_unencrypted_versions(f['Key'],v) + " Version: " + v + " has been removed"
