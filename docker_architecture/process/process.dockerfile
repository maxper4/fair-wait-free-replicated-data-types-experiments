FROM ubuntu:latest
RUN apt update && apt install -y graphviz iproute2
COPY ./target/debug/crdt /usr/bin/experiment/crdt
