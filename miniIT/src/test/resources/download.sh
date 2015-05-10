#!/bin/bash
set -x
/bin/pwd
BASE_DIR=.
TARGET_DIR=./target
DOWNLOAD_DIR=${BASE_DIR}/thirdparty
download() {
 url=$1;
 finalName=$TARGET_DIR/$2
 tarName=$(basename $url)
 sparkTar=${DOWNLOAD_DIR}/${tarName}
 rm -rf $BASE_DIR/$finalName
 if [[ ! -f $sparkTar ]];
  then
  curl -Sso $DOWNLOAD_DIR/$tarName $url
 fi
 tar -zxf $DOWNLOAD_DIR/$tarName -C $BASE_DIR
 mv $BASE_DIR/spark-1.3.0-bin-hadoop-2.5.0 $BASE_DIR/$finalName
}
mkdir -p $DOWNLOAD_DIR
download "https://s3-us-west-2.amazonaws.com/streamsets-public/thirdparty/spark-1.3.0-bin-hadoop-2.5.0.tar.gz" "spark"
