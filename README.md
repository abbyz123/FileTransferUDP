# FileTransferUDP
File transfer server-client model based on UDP protocol

# Description
A server-client model for file transfer based on UDP model. 
A simple application file transfer protocol is implemented 
for a reliable connection between client and server for client 
downloading files from server. 

available commands for client are:
index: request the index of files at server
get [filename]: request the file with [filename] on server

## build
javac FileClientUDP.java
javac FileServerUDP.java

# Usage
## client
java FileClientUDP server_id server_port command [file]

## server
java FileServerUDP file_path
