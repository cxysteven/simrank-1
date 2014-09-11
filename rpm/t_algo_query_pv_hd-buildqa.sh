#!/bin/bash
##for check
export temppath=$1
cd $temppath/rpm
yum-upload $2-$3-$4*.rpm  --osver 5 --arch noarch --group yum --batch --overwrite
echo "Test Building--Target RPM URL is: http://yum.corp.alimama.com/cgi-bin/yuminfo?name=$2-$3-$4*--RPMNAME: $2-$3-$4*.rpm--$tagname--"`date '+%Y%m%d %H:%M:%S' ` >>${LOGFILE}