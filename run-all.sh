#!/bin/bash

### HOW TO RUN THIS SCRIPT
### ./run-all.sh "sh /root/sss/bankServer.sh" -0 500 50 8 100 100

if [[ $1 = "" ]]; then
	echo "Missing commands!"
	exit
fi

counter=0
identity_file = REPLACE_WITH_KEY_FILENAME
#Example
echo ${10}
for server in $(cat ${10}); do
	ssh -i $identity_file ec2-user@$server "$1 $counter $3 $4 $5 $6 $7 $8 $9" > results/$counter.txt &
	echo ec2-user@$server "$1 $counter $3 $4 $5 $6 $7 $8 $9" Done!
	counter=$((counter+1))
	#sleep .5
done
echo "Run on all nodes"
