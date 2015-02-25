#!/bin/sh

input=$1
outputdir=$2

if [ ! -f ${input} ]; then
	exit 1
fi

if [ ! -d ${outputdir} ]; then
	mkdir -p ${outputdir}
fi


cat ${input} | WikiExtractor.py -c -o ${outputdir}
