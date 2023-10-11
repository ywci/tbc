#!/bin/bash
IFACE=`grep iface ../conf/tbc.yaml | awk -F ' ' '{print $2}'`
FILE=`readlink -f $0`
CWD=`dirname "$FILE"`
HOME=`dirname "$CWD"`
CONF=$CWD/conf.h
source "$HOME/conf/build.cfg"
echo "#define IFACE \"$IFACE\"">>"$HOME/tests/conf.h"
