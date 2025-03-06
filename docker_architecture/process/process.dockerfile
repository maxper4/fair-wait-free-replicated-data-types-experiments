FROM ubuntu:latest

COPY ./target/debug/crdt-experiment /usr/bin/experiment/crdt-experiment

RUN apt-get update && apt-get install -y graphviz
