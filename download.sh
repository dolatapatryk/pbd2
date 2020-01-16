#!/bin/sh

rm -rf input
mkdir input
cd input
wget http://www.cs.put.poznan.pl/kjankiewicz/bigdata/projekt2/uk-trafic.zip
echo "pobrano pliki"

unzip uk-trafic.zip
echo "wypakowano"

rm uk-trafic.zip

cd ..
hadoop fs -mkdir -p projekt/
hadoop fs -copyFromLocal input projekt/
echo "skopiowano"
