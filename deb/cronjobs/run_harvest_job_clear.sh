#!/bin/bash

ip=$(echo $IP_MASTER | tr -d '\r')
/sbin/ip -o -4 addr list scope global | awk '{print $4}' | cut -d/ -f1 | grep "$ip"

if [ $? -eq 0 ]; then
  logger "Start GovData harvest job cleaner"
  /usr/lib/ckan/env/bin/paster --plugin=ckanext-harvest harvester clearsource_history --config=/etc/ckan/default/production.ini
  logger "Finished GovData harvest job cleaner"
else
  logger "Host isn't master host"
fi
