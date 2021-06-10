#!/bin/bash
d=${1:-'data/appsinstalled'}
n=${2:-1024}
for f in ${d}/*.tsv.gz
do  
    zcat ${f} | head -n ${n} | gzip - > ${d}/h${n}_${f##*/}
done