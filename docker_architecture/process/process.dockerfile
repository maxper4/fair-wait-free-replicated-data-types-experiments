FROM ubuntu:latest

COPY ./target/debug/crdt /usr/bin/experiment/crdt

RUN apt update && apt install -y graphviz iproute2