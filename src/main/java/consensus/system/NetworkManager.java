package consensus.system;

import consensus.Paxos;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static consensus.Paxos.Message.Type.*;

public class NetworkManager {
    private static final String DEFAULT_HOST = "";
    private final int nodePort;
    private final String hubIP;
    private final int hubPort;
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private final Map<String, ConsensusSystem> systemIdToSystem = new HashMap<>();
    private final String owner;
    private final int ownerIndex;

    public NetworkManager(String owner, int ownerIndex, int nodePort, String hubIP, int hubPort) {
        this.owner = owner;
        this.ownerIndex = ownerIndex;
        this.nodePort = nodePort;
        this.hubIP = hubIP;
        this.hubPort = hubPort;
        start(nodePort);
        sendAppRegistration();
    }

    /**
     * When the node starts, an app registration message is sent ot the hub, so that the hub
     * is aware of this node.
     */
    private void sendAppRegistration() {
        Paxos.Message appRegistration = Paxos.Message.newBuilder()
                .setType(APP_REGISTRATION)
                .setAppRegistration(Paxos.AppRegistration.newBuilder()
                        .setOwner(owner)
                        .setIndex(ownerIndex)
                        .build())
                .build();
        NetworkManager.sendMessage(appRegistration, hubIP, hubPort, nodePort);
    }

    /**
     * sends a message to the given destination, by wrapping it in a network message.
     * @param message
     * @param destinationIP
     * @param destinationPort
     * @param nodePort
     */
    public static void sendMessage(Paxos.Message message, String destinationIP, int destinationPort, int nodePort) {
        try {
            Socket socket = new Socket(destinationIP, destinationPort);
            OutputStream outputStream = socket.getOutputStream();
            outputStream.write(sentMessageToBytes(message, nodePort));
            outputStream.flush();
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Prepares a message by wrapping it to a network message and converting it to a binary array
     * with its length in the first 4 bytes, followed by the actual message in byte array form.
     * @param message
     * @param nodePort
     * @return
     */
    private static byte[] sentMessageToBytes(Paxos.Message message, int nodePort) {
        Paxos.Message sentMessage = Paxos.Message.newBuilder()
                .setType(NETWORK_MESSAGE)
                .setNetworkMessage(Paxos.NetworkMessage.newBuilder()
                        .setMessage(PL_SEND.equals(message.getType()) ? message.getPlSend().getMessage() : message)
                        .setSenderHost(PL_SEND.equals(message.getType()) ? message.getPlSend().getDestination().getHost() : DEFAULT_HOST)
                        .setSenderListeningPort(nodePort)
                        .build())
                .setAbstractionId(message.getAbstractionId())
                .setSystemId(message.getSystemId())
                .build();
        byte[] messageBytes = sentMessage.toByteArray();
        return ByteBuffer.allocate(Integer.BYTES + messageBytes.length)
                .putInt(messageBytes.length).put(messageBytes).array();
    }

    /**
     * Starts a server socket that listens for any incoming messages, placing them in the corresponding system queue.
     * @param nodePort
     */
    public void start(int nodePort) {
        try {
            ServerSocket serverSocket = new ServerSocket(nodePort);
            executorService.execute(() -> {
                while (true) {
                    try {
                        Socket messageSocket = serverSocket.accept();
                        DataInputStream dataInputStream = new DataInputStream(messageSocket.getInputStream());

                        // first 4 bytes: length of the message
                        // the rest: the actual message
                        int length = dataInputStream.readInt();
                        if (length > 0) {
                            byte[] messageBytes = new byte[length];
                            dataInputStream.readFully(messageBytes, 0, messageBytes.length);
                            Paxos.Message message = Paxos.Message.parseFrom(messageBytes);
                            placeMessageInQueue(message);
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Created a new system if an AppPropose message is received, otherwise the message is placed
     * in the corresponding system queue (based on the message's systemId)
     * @param receivedMessage
     */
    private void placeMessageInQueue(Paxos.Message receivedMessage) {
        Paxos.NetworkMessage networkMessage = receivedMessage.getNetworkMessage();
        Paxos.Message internalMessage = networkMessage.getMessage();
        String systemId = receivedMessage.getSystemId();

        if (internalMessage.getType().equals(APP_PROPOSE)) {
            ConsensusSystem system = new ConsensusSystem(nodePort, hubIP, hubPort, systemId);
            systemIdToSystem.put(systemId, system);
            system.trigger(internalMessage);
        } else {
            ConsensusSystem system = systemIdToSystem.get(systemId);
            if (system != null) {
                Paxos.ProcessId sender = system.identifyProcess(networkMessage);
                if (sender != null) {
                    Paxos.Message plDeliver = Paxos.Message.newBuilder()
                            .setType(PL_DELIVER)
                            .setAbstractionId(receivedMessage.getAbstractionId())
                            .setPlDeliver(Paxos.PlDeliver.newBuilder()
                                    .setMessage(internalMessage)
                                    .setSender(sender)
                                    .build())
                            .build();
                    system.trigger(plDeliver);
                }
            }
        }
    }
}
