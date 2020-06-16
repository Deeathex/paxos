package consensus;

import consensus.system.NetworkManager;

public class Main {
    private static final String OWNER = "node";
    private static final int NODE_PORT = 5010;
    private static final int HUB_PORT = 5000;
    private static final String HUB_IP = "localhost";

    public static void main(String[] args) {
        for (int i = 1; i <= 3; i++) {
            new NetworkManager(OWNER, i, NODE_PORT + i, HUB_IP, HUB_PORT);
        }
    }
}
