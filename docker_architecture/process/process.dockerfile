FROM ubuntu:latest

COPY ./target/debug/crdt /usr/bin/experiment/crdt

RUN apt-get update && apt-get install -y graphviz
