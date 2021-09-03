#!/bin/bash

set -o errexit
set -o pipefail

cd "$(dirname "$0")"

cargo build
for bin in agent downstairs; do
	rp='opt/ooce/pgsql-13/lib/amd64:/usr/gcc/10/lib/amd64'
	/usr/bin/elfedit \
	    -e "dyn:runpath $rp" \
	    "target/debug/crucible-$bin"
done

cd "$(dirname "$0")/agent"

hosts='one two three'

#
# Deploy software
#
pfexec rm -rf /opt/oxide/crucible
pfexec mkdir -p /opt/oxide/crucible/bin
for x in agent downstairs; do
	pfexec cp ../target/debug/crucible-$x /opt/oxide/crucible/bin/$x
done

#
# Deploy SMF manifests
#
printf -- '-- SMF\n'
svcadm disable -s '*crucible/agent*' || true
svccfg delete crucible/agent || true
pfexec cp downstairs.xml agent.xml /var/svc/manifest/site/
svccfg import /var/svc/manifest/site/agent.xml
svccfg import /var/svc/manifest/site/downstairs.xml


for h in $hosts; do
	printf -- '----- %s -------\n' "$h"

	case $h in
	one)
		uuid=ea330bc1-d18b-e8b0-edd1-d426a798cc4d
		listen="127.0.0.1:16001"
		portbase=17000
		;;
	two)
		uuid=ac341eb2-031b-e9af-f7aa-c9665c8b569d
		listen="127.0.0.1:16002"
		portbase=18000
		;;
	three)
		uuid=8e71ce15-6b06-6896-f4aa-832c1befd67a
		listen="127.0.0.1:16003"
		portbase=19000
		;;
	esac
	printf 'uuid: %s\n' "$uuid"

	#
	# Create data directory
	#
	mkdir -p /var/tmp/CRUCIBLE/$h

	#
	# Create SMF instance for this agent:
	#
	svccfg -s crucible/agent add $h
	svccfg -s crucible/agent:$h addpg config application
	svccfg -s crucible/agent:$h setprop \
	    config/listen = "$listen"
	svccfg -s crucible/agent:$h setprop \
	    config/nexus = '127.0.0.1:12221'
	svccfg -s crucible/agent:$h setprop \
	    config/uuid = "$uuid"
	svccfg -s crucible/agent:$h setprop \
	    config/portbase = "$portbase"
	svccfg -s crucible/agent:$h setprop \
	    config/datadir = "/var/tmp/CRUCIBLE/$h"
	svccfg -s crucible/agent:$h setprop \
	    config/prefix = "downstairs-$h"

	svcadm refresh crucible/agent:$h
	svcadm enable crucible/agent:$h
	sleep 2
	svcs '*crucible*'
	echo
	echo
done
