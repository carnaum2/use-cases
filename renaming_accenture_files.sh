#!/bin/bash

if [ $# -ne 2 ]
then
	echo "ERROR: Invallid number of arguments"
	echo "Usage: $0 <YYYYMM> <type>"
	echo "       Being type = INPUT|INTERMEDIOS|OUTPUT"
	echo "F.e.: $0 201511 INTERMEDIOS"
	exit 1
fi

MONTH="$1"
TYPE="$2"

if [ "$TYPE" == "INPUT" ]
then
	# INPUT
	TYPE_PREFIX="ES.CVM.IN"
elif [ "$TYPE" == "INTERMEDIOS" ]
then
	# CAR
	TYPE_PREFIX="ES.CVM.CAR"
elif [ "$TYPE" == "OUTPUT" ]
then
	# OUTPUT
	TYPE_PREFIX="ES.CVM.OUT"
else
	echo "ERROR: Invalid type $TYPE"
	echo "Please use one of INPUT|INTERMEDIOS|OUTPUT"
	exit 2
fi

ORIG_DIR="$PWD/$MONTH/$TYPE"
DEST_DIR="$TYPE_PREFIX.${MONTH}"

if [ ! -f "${DEST_DIR}.tar.gz.enc.part_aa.enc" ]
then
	FILES=$( /bin/ls $ORIG_DIR/* )
	
	for fullpath in $FILES
	do
		# First, get file name without the path
		fullfile=$(basename "$fullpath")
		#filename="${fullpath##*/}" # Alternatively, you can focus on the last '/' of the path instead of the '.' which should work even if you have unpredictable file extensions
		extension="${fullfile##*.}"
		filename="${fullfile%.*}"

		if [ $extension == "zip" ]
		then
			echo "unzip $fullpath"
			unzip $fullpath
		elif [ $extension == "gz" ]
		then
			echo "gunzip -k $fullpath"
			gunzip -k $fullpath # > $DEST_DIR/$( basename $fullfile .gz )
		fi
	done

	mkdir $DEST_DIR
	
	FILES=$( /bin/ls $ORIG_DIR/*.[Tt][Xx][Tt] )
	for fullpath in $FILES
	do
		# First, get file name without the path
		fullfile=$(basename "$fullpath")
		#filename="${fullpath##*/}" # Alternatively, you can focus on the last '/' of the path instead of the '.' which should work even if you have unpredictable file extensions
		extension="${fullfile##*.}"
		filename="${fullfile%.*}"

		NEW_NAME=$( echo "$fullfile" | sed -r 's/(_)([0-9]{6})/.\2/g' | sed 's/_//g' )
		
		# FIXME: If the file was uncompressed prior to this script (and the corresponding .gz is gone), the following command moves the file permanently from the original dir
		echo mv $fullpath $DEST_DIR/${TYPE_PREFIX}.${NEW_NAME}
		mv $fullpath $DEST_DIR/${TYPE_PREFIX}.${NEW_NAME}
	done
	
	# 7-Zip
	#echo "7za a ${DEST_DIR}.7z $DEST_DIR/ -v5g -tzip -mem=AES256 -mx2 -pB1gD4t43s"
	#7za a ${DEST_DIR}.7z $DEST_DIR/ -v5g -tzip -mem=AES256 -mx2 -pB1gD4t43s
	
	# Compress & Encrypt
	#echo "tar cz $DEST_DIR/ | openssl enc -aes-256-cbc -e -k B1gD4t43s > ${DEST_DIR}.tar.gz.enc"
	#tar cz $DEST_DIR/ | openssl enc -aes-256-cbc -e -k B1gD4t43s > ${DEST_DIR}.tar.gz.enc
	
	# Uncompress & Decrypt
	# openssl enc -aes-256-cbc -d -in ../ES.CVM.CAR.prueba.tar.gz.enc -k B1gD4t43s > ES.CVM.CAR.prueba.tar.gz
	
	# Split
	#echo "split -b 5G ${DEST_DIR}.tar.gz.enc ${DEST_DIR}.tar.gz.enc-part_"
	#split -b 5G ${DEST_DIR}.tar.gz.enc ${DEST_DIR}.tar.gz.enc-part_
	
	# Compress, Encrypt & Split
	#    http://stackoverflow.com/questions/1120095/split-files-using-tar-gz-zip-or-bzip2
	#    http://superuser.com/questions/162624/how-to-password-protect-gzip-files-on-the-command-line
	echo "tar cz $DEST_DIR/ | openssl enc -aes-256-cbc -e -k B1gD4t43s | split -b 5G - ${DEST_DIR}.tar.gz.enc.part_ --additional-suffix=.enc"
	tar cz $DEST_DIR/ | openssl enc -aes-256-cbc -e -k B1gD4t43s | split -b 5G - ${DEST_DIR}.tar.gz.enc.part_ --additional-suffix=.enc
	
	# Uncompress, Decrypt & Concatenate
	echo -e "\nUncompress, Decrypt & Concatenate with:\n\tcat ${DEST_DIR}.tar.gz.enc.part_* | openssl enc -aes-256-cbc -d -k B1gD4t43s | tar xz\n"
	
	rm -rf $DEST_DIR/
else
	echo "Skipping ${DEST_DIR}.tar.gz.enc.part_aa.enc"
fi
