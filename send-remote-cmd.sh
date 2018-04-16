#!/bin/bash
### Send a command to all nodes in cluster (defined by cluster file)
### Key file is provided by AWS when setting up node

if [[ $1 = "" ]]; then
echo "Missing cluster file!"
exit
fi

##identity_file = "ece-403.pem"


for server in $(cat $1); do
    ssh -i "ece-403.pem" ubuntu@$server "$2"
    echo "Command sent $server"
   
done
echo "Done sent command $2"

