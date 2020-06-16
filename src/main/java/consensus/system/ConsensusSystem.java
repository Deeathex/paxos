package consensus.system;

import consensus.Paxos;
import consensus.abstractions.Abstraction;
import consensus.abstractions.Application;
import consensus.abstractions.EpochConsensus;
import consensus.utils.ValueUtils;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConsensusSystem {
    private final int nodePort;
    private final String hubIP;
    private final int hubPort;
    private final String systemId;

    private final List<Paxos.ProcessId> processList = new CopyOnWriteArrayList<>();
    private final List<Abstraction> abstractionList = new CopyOnWriteArrayList<>();
    private final List<Paxos.Message> messageQueue = new CopyOnWriteArrayList<>();

    private Paxos.ProcessId currentProcess;

    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    public ConsensusSystem(int nodePort, String hubIP, int hubPort, String systemId) {
        this.nodePort = nodePort;
        this.hubIP = hubIP;
        this.hubPort = hubPort;
        this.systemId = systemId;

        this.executorService.execute(this::init);
        addAbstraction(new Application(this));
    }

    /**
     * Handles the message distribution from the queue to the corresponding abstraction
     * on a different thread to be nonblocking.
     */
    public void init() {
        while (true) {
            boolean handled = false;
            int messageIndex = 0;
            while (!messageQueue.isEmpty() && !handled && messageIndex < messageQueue.size()) {
                for (Abstraction abstraction : abstractionList) {
                    if (abstraction.handle(messageQueue.get(messageIndex))) {
                        handled = true;
                    }
                }
                if (handled) {
                    messageQueue.remove(messageIndex);
                } else {
                    messageIndex++;
                }
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Adds a message to the queue.
     * @param message
     */
    public void trigger(Paxos.Message message) {
        message = message.toBuilder().setSystemId(systemId).build();
        messageQueue.add(message);
    }

    /**
     * Identify the process based on its port from the network message.
     * @param networkMessage
     * @return
     */
    public Paxos.ProcessId identifyProcess(Paxos.NetworkMessage networkMessage) {
        for (Paxos.ProcessId processId : processList) {
            if (networkMessage.getSenderListeningPort() == processId.getPort()) {
                return processId;
            }
        }
        return null;
    }

    public void addAbstraction(Abstraction abstraction) {
        abstractionList.add(abstraction);
    }

    public void setProcesses(List<Paxos.ProcessId> processesList) {
        processList.addAll(processesList);
        for (Paxos.ProcessId processId : processList) {
            if (nodePort == processId.getPort()) {
                currentProcess = processId;
                break;
            }
        }
    }

    public int getNodePort() {
        return nodePort;
    }

    public String getHubIP() {
        return hubIP;
    }

    public int getHubPort() {
        return hubPort;
    }

    public Paxos.ProcessId getCurrentProcess() {
        return currentProcess;
    }

    public List<Paxos.ProcessId> getProcessList() {
        return processList;
    }

    public String getSystemId() {
        return systemId;
    }

    public void startNewEpoch(int ets, int timestamp, Paxos.Value value) {
        Paxos.EpState_ epState = Paxos.EpState_.newBuilder()
                .setValueTimestamp(timestamp)
                .setValue(ValueUtils.makeCopy(value))
                .build();
        addAbstraction(new EpochConsensus(this, ets, epState));
    }

    public Paxos.ProcessId getMinRankProcess() {
        Paxos.ProcessId minRankProcess = getProcessList().get(0);
        for (Paxos.ProcessId processId : getProcessList()) {
            if (processId.getRank() < minRankProcess.getRank()) {
                minRankProcess = processId;
            }
        }
        return minRankProcess;
    }
}
