import java.io.File;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FileServerUDP {

    private int PORT;                                                                               // server port
    private DatagramSocket socket;                                                                  // udp socket

    private final int MAX_BUFFER_SIZE = 100;                                                        // max buffer size

    String filePath;                                                                                // file path
    String index;                                                                                   // index

    final int UUID_HDR_LEN = 36;                                                                    // UUID header length
    final int PKT_ID_HDR_LEN = 8;                                                                   // packet id header length
    final int PKT_PAYLOAD_HDR_LEN = 4;                                                              // payload header length
    final int TOTAL_HDR_LEN = (UUID_HDR_LEN + PKT_ID_HDR_LEN + PKT_PAYLOAD_HDR_LEN);                // total header length
    final int FILE_FRAG_LEN = MAX_BUFFER_SIZE - TOTAL_HDR_LEN;                                      // file fragment length

    ConcurrentLinkedQueue<Message> msgQueue;                                                        // message queue
    ConcurrentHashMap<String, byte []> fileMap;                                                     // file map
    ConcurrentHashMap<String, String> clientMap;                                                    // client map

    private class Message {                                                                         // message class
        InetAddress address;
        int PORT;
        FilePacket fp;
    }

    private class MsgReader implements Runnable {
        private Message msg;
        private FilePacket fp;
        public void run() {
            while (true) {
                try {
                    msg = msgQueue.poll();
                    if (msg != null) {
                        fp = msg.fp;                                                 // get File Packet
                        String [] req = fp.payload.split(" ");                            // analyze request

                        if (req[0].compareTo("index") == 0) {                                   // "index" command
                            System.out.format("Client %s from %s requests for index\n", fp.uuidHdr, msg.address.toString());

                            // prepare response packet for "index" and send back to client
                            sendStringResp(index);
                        } else if (req[0].compareTo("get") == 0) {                              // "get" command
                            // print server log in console
                            System.out.format("Client %s from %s requests for file %s\n", fp.uuidHdr, msg.address.toString(), req[1]);

                            // initialize packet id and file position
                            if (fp.pktIdHdr == 0) {
                                // get filename and load file into a byte array
                                String filename = req[1];

                                // add client into client map
                                String clientKey = msg.address.toString() + fp.uuidHdr;
                                clientMap.putIfAbsent(clientKey, filename);

                                // load file into byte array
                                Path path = Paths.get(filename);
                                byte [] filedata = Files.readAllBytes(path);
                                fileMap.putIfAbsent(filename, filedata);
                                sendFileFrag(filedata);

                            } else {
                                System.out.println("Packet Id is not as expected for get command");
                            }
                        } else if (req[0].compareTo("next") == 0) {
                            // get client key
                            String clientKey = msg.address.toString() + fp.uuidHdr;

                            String filename = clientMap.get(clientKey);
                            byte [] filedata = fileMap.get(filename);

                            // send next fragment to client
                            sendFileFrag(filedata);
                        }
                    }
                } catch (IOException e) {
                    try {
                        sendStringResp("error");
                        System.out.println("Error on server. Error message sent to client");
                    } catch (IOException ee) {
                        System.out.println("error sending message to client");
                    }
                }
            }
        }

        // send string response
        private void sendStringResp(String resp) throws IOException {
            byte [] idxMsg = prepOutgoingPkt(fp.uuidHdr, fp.pktIdHdr, resp);
            DatagramPacket dpIndex = new DatagramPacket(idxMsg, idxMsg.length, msg.address, msg.PORT);
            socket.send(dpIndex);
        }

        // send file fragment
        private void sendFileFrag(byte[] filedata) throws IOException {
            int [] range = fragStartEndPos((int)fp.pktIdHdr, filedata.length);

            byte [] fileMsg;
            if (range[0] == -1) {
                fileMsg = prepOutgoingPkt(fp.uuidHdr, fp.pktIdHdr, "end");                              // end of file
            } else {
                byte [] fileFrag = Arrays.copyOfRange(filedata, range[0], range[1]);
                fileMsg = prepOutgoingPkt(fp.uuidHdr, fp.pktIdHdr, fileFrag);                                   // next file fragment
            }
            DatagramPacket dp = new DatagramPacket(fileMsg, fileMsg.length, msg.address, msg.PORT);             // datagram for file fragment
            socket.send(dp);                                                                                    // send next file fragment to client
        }

        private int [] fragStartEndPos(int pktId, int fileLen) {
            int start = pktId * FILE_FRAG_LEN;
            if (start >= fileLen) {
                return new int [] {-1, -1};
            } else if (start + FILE_FRAG_LEN >= fileLen){
                return new int [] {start, fileLen};
            } else {
                return new int [] {start, start + FILE_FRAG_LEN};
            }
        }

        // Prepare out going packet with application layer header
        //  -------------------------------------------------------------------------------
        // |  client uuid (32 byte) | pkt id (8 byte) |  payload length (4 byte) | payload |
        //  -------------------------------------------------------------------------------
        private byte [] prepOutgoingPkt(String UUID, long pktId, String payload) {
            // set header buffer
            ByteBuffer buffer = ByteBuffer.allocate(UUID_HDR_LEN + PKT_ID_HDR_LEN + PKT_PAYLOAD_HDR_LEN + payload.length());

            // set uuid, pktId and payload length
            buffer.put(UUID.getBytes(StandardCharsets.UTF_8));
            buffer.putLong(UUID_HDR_LEN, pktId);
            buffer.putInt(UUID_HDR_LEN + PKT_ID_HDR_LEN, payload.length());
            buffer.put(UUID_HDR_LEN + PKT_ID_HDR_LEN + PKT_PAYLOAD_HDR_LEN, payload.getBytes(StandardCharsets.UTF_8));

            // copy to request buffer to be sent
            byte [] byteArr = buffer.array();

            // return total length for socket
            return byteArr;
        }

        // Prepare out going packet with application layer header
        //  -------------------------------------------------------------------------------
        // |  client uuid (32 byte) | pkt id (8 byte) |  payload length (4 byte) | payload |
        //  -------------------------------------------------------------------------------
        // Overload function for byte array payload
        private byte [] prepOutgoingPkt(String UUID, long pktId, byte [] payload) {
            // set header buffer
            ByteBuffer buffer = ByteBuffer.allocate(UUID_HDR_LEN + PKT_ID_HDR_LEN + PKT_PAYLOAD_HDR_LEN + payload.length);

            // set uuid, pktId and payload length
            buffer.put(UUID.getBytes(StandardCharsets.UTF_8));
            buffer.putLong(UUID_HDR_LEN, pktId);
            buffer.putInt(UUID_HDR_LEN + PKT_ID_HDR_LEN, payload.length);
            buffer.put(UUID_HDR_LEN + PKT_ID_HDR_LEN + PKT_PAYLOAD_HDR_LEN, payload);

            // copy to request buffer to be sent
            byte [] byteArr = buffer.array();

            // return total length for socket
            return byteArr;
        }
    }

    private class MsgWriter implements Runnable {
        public void run() {
            while (true) {
                try {
                    byte [] buffer = new byte [MAX_BUFFER_SIZE];                            // buffer for receiving packet
                    DatagramPacket dp = new DatagramPacket(buffer, buffer.length);          // datagram for receiving packet
                    socket.receive(dp);                                                     // receive packet from socket
                    FilePacket fp = getIncomingPkt(buffer);                                 // analyze packet header and payload

                    Message msg = new Message();                                            // create message for client request
                    msg.address = dp.getAddress();                                          // client address
                    msg.PORT = dp.getPort();                                                // client port
                    msg.fp = fp;                                                            // file packet header

                    msgQueue.add(msg);                                                      // add message to queue
                } catch (IOException e) {
                    System.out.println("Error receiving message");
                }
            }
        }

        // Analyze incoming packet with application layer header
        //  -------------------------------------------------------------------------------
        // |  client uuid (32 byte) | pkt id (8 byte) |  payload length (4 byte) | payload |
        //  -------------------------------------------------------------------------------
        private FilePacket getIncomingPkt(byte [] bufferArray) {
            ByteBuffer buffer = ByteBuffer.wrap(bufferArray);           // buffer wrapping the buffer array
            FilePacket ret = new FilePacket();                          // FilePacket object for analyzed packet

            // get UUID header
            byte [] uuidHdr = new byte [UUID_HDR_LEN];
            buffer.get(uuidHdr, 0, UUID_HDR_LEN);
            ret.uuidHdr = new String(uuidHdr, StandardCharsets.UTF_8);

            // get packet id and payload length
            ret.pktIdHdr = buffer.getLong(UUID_HDR_LEN);
            ret.payloadLenHdr = buffer.getInt(UUID_HDR_LEN + PKT_ID_HDR_LEN);

            // get payload
            int from = (UUID_HDR_LEN + PKT_ID_HDR_LEN + PKT_PAYLOAD_HDR_LEN);
            int to = from + ret.payloadLenHdr;
            byte [] payload = Arrays.copyOfRange(bufferArray, from, to);
            ret.payload = new String(payload, StandardCharsets.UTF_8);

            return ret;
        }
    }



    public FileServerUDP(String path, int PORT) throws IOException {
        this.PORT = PORT;
        this.socket = new DatagramSocket(PORT);
        this.filePath = path;

        Stream<Path> paths = Files.walk(Paths.get(filePath));                   // walk through the file path and find all .txt files
        index = paths.map(p -> p.toString()).filter(p -> p.endsWith(".txt")).collect(Collectors.joining(" "));

        msgQueue = new ConcurrentLinkedQueue<>();
        fileMap = new ConcurrentHashMap<>();
        clientMap = new ConcurrentHashMap<>();
    }

    // help function show usage
    private static void help() {
        System.out.println("Usage: java FileServer ip_address port");
    }

    private class FilePacket {
        public String uuidHdr;
        public long pktIdHdr;
        public int payloadLenHdr;
        public String payload;
    }

    // run server
    void run() {
        new Thread(new MsgWriter()).start();                                        // start a message writer
        new Thread(new MsgReader()).start();                                        // start a message reader
    }

    public static void main (String [] args) {
        if (args.length != 2) {                                                     // insufficient argument. Show usage again
            help();
        } else {
            String path = args[0];                                                  // take path for all txt files
            int PORT = Integer.parseInt(args[1]);                                   // take port number

            try {
                FileServerUDP server = new FileServerUDP(path, PORT);               // initiate server
                server.run();                                                       // start server
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
