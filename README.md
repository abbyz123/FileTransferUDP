# FileTransferUDP
File transfer server-client model based on UDP protocol

# Description
A server-client model for file transfer based on UDP model. </br>
A simple application file transfer protocol is implemented  </br>
for a reliable connection between client and server for client </br>
downloading files from server. </br>

available commands for client are: </br>
index: request the index of files at server </br>
get [filename]: request the file with [filename] on server </br>

# Build
javac FileClientUDP.java </br>
javac FileServerUDP.java </br>

# Usage
## client
java FileClientUDP server_id server_port command [file] </br>

## server
java FileServerUDP file_path </br>
