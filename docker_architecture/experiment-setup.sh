#!/bin/bash

for i in "$@"; do
  case $i in
    -p=*|--processes=*)
      p="${i#*=}"
      shift # past argument=value
      ;;
    -t=*|--data_type=*)
      t="${i#*=}"
      shift # past argument=value
      ;;
    -f=*|--function=*)
      f="${i#*=}"
      shift # past argument=value
      ;;
    --partition=*)
      partition="${i#*=}"
      shift # past argument=value
      ;;
    -d=*|--duration=*)
      d="${i#*=}"
      shift # past argument=value
      ;;
    -*|--*)
      echo "Unknown option $i"
      exit 1
      ;;
    *)
      ;;
  esac
done

mkdir -p experiment

# default config if not specified
if [ -z "$p" ]; then p=4; fi
if [ -z "$f" ]; then f=1; fi
if [ -z "$d" ]; then d=30; fi
if [ -z "$t" ]; then t=1; fi

USER_DOCKER="$(id -u):$(id -g)"

if [ -z "$partition" ] || [ "$partition" = "0" ] ; then
  network_cmd="tc qdisc add dev eth0 root netem delay 100ms 400ms distribution normal && "
else
  partition_duration=$(bc <<< "scale=2; $d*$partition")
  remaining=$(bc <<< "scale=2; $d-$partition_duration")
  partition_start=$(bc <<< "scale=2; $remaining/2")
  partition_end=$(bc <<< "scale=2; $partition_start+$partition_duration")
  network_cmd="tc qdisc add dev eth0 root netem delay 100ms 400ms distribution normal && ((sleep $partition_start && tc qdisc change dev eth0 root netem delay ${partition_duration}s) &) && ((sleep $partition_end && tc qdisc change dev eth0 root netem delay 100ms 400ms distribution normal) &) && "
fi

cat << EOF > ./experiment/docker-compose.yml
services:
EOF


for id in $(seq 1 $p)
do
    peers="["
    for peer_id in $(seq 1 $p)
    do
        if [ $peer_id -ne $id ]; then
            peers+="{ip = '192.167.0.$((peer_id+1))', port='4444'},"
        fi
    done
    peers+="]"

    port_host=$((4444+$id))
    ipv4="192.167.0.$((id+1))"
    mkdir -p ./experiment/process$id
cat << EOF > ./experiment/process$id/config.toml
id = $id
ip = 'process$id'
port = '4444'
peers = $peers
reconciliation_function = $f
data_type = $t
duration = $d
EOF

cat << EOF >> ./experiment/docker-compose.yml
  process$id:
    container_name: process$id
    cap_add:
      - NET_ADMIN
    image: "process"
    ports:
      - "$port_host:4444"
    volumes:
        - ./process$id:/etc/experiment:rw
    command: sh -c "$network_cmd(/usr/bin/experiment/crdt 2>&1 | tee process$id.log)"
    user: ${USER_DOCKER}
    working_dir: /etc/experiment

    networks:
      localnet:
        aliases:
          - process$id
        ipv4_address: $ipv4

EOF
done

cat << EOF >> ./experiment/docker-compose.yml
networks:
  localnet:
    driver: bridge
    ipam:
      driver: default
      config:
        - subnet: 192.167.0.0/16

EOF
