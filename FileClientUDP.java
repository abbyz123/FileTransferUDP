import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.UUID;

public class FileClientUDP {
    private InetAddress serverAddr;                     // server address
    private int PORT;                                   // server port
    private DatagramSocket socket;                      // udp socket

    private final int MAX_BUFFER_SIZE = 100;           // max buffer size
    private byte [] bufferReq;                          // buffer for request
    private byte [] bufferResp;                         // buffer for response

    private String content;                             // content downloaded from server

    private UUID uuid;                                  // client uuid
    private long pktId;                                 // packet id

    final int UUID_HDR_LEN = 36;                            // UUID header length
    final int PKT_ID_HDR_LEN = 8;                           // packet id header length
    final int PKT_PAYLOAD_HDR_LEN = 4;                      // payload header length

    int timeOutCounter = 0;                                 // time out counter
    final int MAX_TIMEOUT_COUNT = 10;                       // maximum count of time out before close connection
    final int TIMEOUT_MS = 1000;                            // time out waiting time in milliseconds

    String reSendBuffer;                                    // resend buffer

    public FileClientUDP(InetAddress serverAddr, int PORT) throws SocketException {
        this.serverAddr = serverAddr;                       // set server address
        this.PORT = PORT;                                   // set server port

        this.socket = new DatagramSocket();                 // initialize socket
        this.socket.connect(serverAddr, PORT);              // connect to server

        this.bufferReq = new byte [MAX_BUFFER_SIZE];        // initialize buffer for request
        this.bufferResp = new byte [MAX_BUFFER_SIZE];       // initialize buffer for response

        this.uuid = UUID.randomUUID();                      // randomly generate UUID for each client
        this.content = new String();                        // initialize content string
    }


    // send message with payload to server
    private void send(String payload) throws IOException {
        int length = prepOutgoingPkt(payload);
        DatagramPacket reqPkt = new DatagramPacket(bufferReq, length, serverAddr, PORT);
        socket.send(reqPkt);
    }

    // receive server response
    private FilePacket recv() throws IOException {
        DatagramPacket respPkt = new DatagramPacket(bufferResp, bufferResp.length);
        socket.receive(respPkt);

        FilePacket pkt = getIncomingPkt();
        return pkt;
    }

    // Prepare out going packet with application layer header
    //  -------------------------------------------------------------------------------
    // |  client uuid (32 byte) | pkt id (8 byte) |  payload length (4 byte) | payload |
    //  -------------------------------------------------------------------------------
    private int prepOutgoingPkt(String payload) {
        // set header buffer
        ByteBuffer buffer = ByteBuffer.allocate(UUID_HDR_LEN + PKT_ID_HDR_LEN + PKT_PAYLOAD_HDR_LEN + payload.length());

        // set uuid, pktId and payload length
        buffer.put(uuid.toString().getBytes(StandardCharsets.UTF_8));
        buffer.putLong(UUID_HDR_LEN, pktId);
        buffer.putInt(UUID_HDR_LEN + PKT_ID_HDR_LEN, payload.length());
        buffer.put(UUID_HDR_LEN + PKT_ID_HDR_LEN + PKT_PAYLOAD_HDR_LEN, payload.getBytes(StandardCharsets.UTF_8));

        // total length
        int totalLen = UUID_HDR_LEN + PKT_ID_HDR_LEN + PKT_PAYLOAD_HDR_LEN + payload.length();

        // copy to request buffer to be sent
        byte [] byteArr = buffer.array();
        System.arraycopy(byteArr, 0, bufferReq, 0, totalLen);

        // return total length for socket
        return totalLen;
    }

    private class FilePacket {
        public String uuidHdr;          // UUID header
        public long pktIdHdr;           // Packet ID header
        public int payloadLenHdr;       // Payload length header
        public String payload;          // Payload
    }

    private FilePacket getIncomingPkt() {
        ByteBuffer buffer = ByteBuffer.wrap(bufferResp);                                // buffer wrapping the byte array
        FilePacket ret = new FilePacket();                                              // FilePacket object containing the analyzed packet

        // get UUID header
        byte [] uuidHdr = new byte [UUID_HDR_LEN];
        buffer.get(uuidHdr, 0, UUID_HDR_LEN);
        ret.uuidHdr = new String(uuidHdr, StandardCharsets.UTF_8);

        // get Packet ID and payload length
        ret.pktIdHdr = buffer.getLong(UUID_HDR_LEN);
        ret.payloadLenHdr = buffer.getInt(UUID_HDR_LEN + PKT_ID_HDR_LEN);

        // get payload
        int from = (UUID_HDR_LEN + PKT_ID_HDR_LEN + PKT_PAYLOAD_HDR_LEN);
        int to = from + ret.payloadLenHdr;
        byte [] payload = Arrays.copyOfRange(bufferResp, from, to);
        ret.payload = new String(payload, StandardCharsets.UTF_8);

        return ret;
    }

    // send request to server with "get" command
    //  -------------------------------------------------------------------------------
    // |  client uuid (32 byte) | pkt id (8 byte) |  payload length (4 byte) | get file |
    //  -------------------------------------------------------------------------------
    private void get(String filename) throws IOException {
        boolean cmdSent = false;

        String respPayload = "";
        while (true) {
            try {
                if (!cmdSent) {
                    reSendBuffer = "get " + filename;                                           // sent command and update resend buffer
                    cmdSent = true;
                } else {
                    reSendBuffer = "next";                                                      // next command and update resend buffer
                }
                send(reSendBuffer);                                                             // send content of resend buffer to server
                socket.setSoTimeout(TIMEOUT_MS);                                                // set time out

                FilePacket pkt = recv();                                                        // receive response
                if (pkt.uuidHdr.compareTo(uuid.toString()) == 0 && pkt.pktIdHdr == pktId) {     // check if the response is for this user and packet id is expected
                    timeOutCounter = 0;                                                         // reset time out counter if header is checked out
                    respPayload = pkt.payload;                                                  // save payload as file fragment
                    if (respPayload.compareTo("end") == 0) {                                    // end of file
                        break;
                    } else if (respPayload.compareTo("error") == 0) {
                        System.out.println("File error on server.");
                        break;
                    }
                    System.out.format("%s", respPayload);                                       // print file fragment to console

                    pktId++;                                                                    // update expected packet id
                } else {
                    System.out.println("wrong header");
                    timeOutCounter++;                                                           // resend and increase timeout counter
                }
            } catch (SocketTimeoutException e) {
                if (timeOutCounter == MAX_TIMEOUT_COUNT) {                                      // break connection if maximum timeout count is reached
                    System.out.println("Max time out count reached. Server connection is closed");
                    break;
                } else {
                    System.out.println("time out " + timeOutCounter);
                    timeOutCounter++;                                                           // resend and increase timeout counter
                    continue;                                                                   // keep resend if maximum time count is not reached yet
                }
            }
        }
    }

    // send request to server with "index" command
    //  -------------------------------------------------------------------------------
    // |  client uuid (32 byte) | pkt id (8 byte) |  payload length (4 byte) | index  |
    //  -------------------------------------------------------------------------------
    private void index() throws IOException {
        send("index");
        socket.setSoTimeout(1000);

        FilePacket pkt = recv();
        if (pkt.uuidHdr.compareTo(uuid.toString()) == 0 && pkt.pktIdHdr == pktId) {
            System.out.println(pkt.payload);
        }
    }

    // help function show usage
    private static void help() {
        System.out.println("Usage: java FileClient ip_address port command [file]");
    }

    // commands function show available commands
    private static void commands() {
        System.out.println("available commands: ");
        System.out.println("index: show all files on server");
        System.out.println("get: download file from server");
    }

    // parse commands
    private void parseCmd(String [] args) throws IOException {

        // parse command
        if (args[2].compareTo("index") == 0) {          // "index" command to ask for file list from server
            index();                                    // send "index" command to server
        } else if (args[2].compareTo("get") == 0) {
            if (args.length < 4) {
                help();                                 // No filename following "get" command
            } else {
                get(args[3]);                           // send "get file" command to server
            }
        } else {
            commands();
        }
    }

    public static void main (String [] args) {
        // show usage if argument number is wrong
        if (args.length < 3) {
            System.out.println("Insufficient number of arguments");
            help();
            return;
        }

        try {
            InetAddress ipAddr = InetAddress.getByName(args[0]);            // get server address from argument
            int PORT = Integer.parseInt(args[1]);                           // get port number from argument
            FileClientUDP client = new FileClientUDP(ipAddr, PORT);         // initialize UDP client
            client.parseCmd(args);                                          // parse command and send request to server
        } catch (IOException e) {
            System.out.println("Can not build connection.");                // show error log if connection is lost or error
        }
    }
}
