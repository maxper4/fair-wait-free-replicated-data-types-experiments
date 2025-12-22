#!/bin/bash

for i in "$@"; do
  case $i in
    -p=*|--processes=*)
      p="${i#*=}"
      shift # past argument=value
      ;;
    -e=*|--experiment=*)
      e="${i#*=}"
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
if [ -z "$e" ]; then e=0; fi

USER_DOCKER="$(id -u):$(id -g)"

cat << EOF > ./experiment/docker-compose.yml
version: '3'

services:
EOF


for id in $(seq 1 $p)
do
    peers="["
    for peer_id in $(seq 1 $p)
    do
        if [ $peer_id -ne $id ]; then
            peers+="{ip = 'process$peer_id:4444'},"
        fi
    done
    peers+="]"

    port_host=$((4444+$id))
    mkdir -p ./experiment/process$id
cat << EOF > ./experiment/process$id/config.toml
id = $id
ip = 'process$id:4444'
peers = $peers
experiment_type = $e
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
    command: sh -c "tc qdisc add dev eth0 root netem delay 100ms 10000ms distribution normal && /usr/bin/experiment/crdt"
    user: ${USER_DOCKER}
    working_dir: /etc/experiment

    networks:
      localnet:
        aliases:
          - process$id

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
