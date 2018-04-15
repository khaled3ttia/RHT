#!/bin/bash
### Kill all Java process (assumes application is Java)

if [[ $1 = "" ]]; then
echo "Missing cluster file!"
exit
fi

identity_file = REPLACE_WITH_KEY_FILENAME
for server in $(cat $1); do
	ssh -i $identity_file ec2-user@$server "killall java" 
	echo "- at $server"
done
echo "Done kill java"

