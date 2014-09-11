#!/bin/bash
##for check
export temppath=$1
cd $temppath
cd rpm
yum-setbranch  $2-$3-$4 5 noarch current
echo "Release Building--Target RPM URL is: http://yum.corp.alimama.com/cgi-bin/yuminfo?name=$2-$3-$4*--RPMNAME: $2-$3-$4*.rpm--$tagname--"`date '+%Y%m%d %H:%M:%S' ` >>${LOGFILE}
