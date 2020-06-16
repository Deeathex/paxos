package consensus.abstractions;

import consensus.system.ConsensusSystem;
import consensus.system.NetworkManager;

import static consensus.Paxos.*;
import static consensus.Paxos.Message.Type.APP_DECIDE;
import static consensus.Paxos.Message.Type.UC_PROPOSE;

public class Application implements Abstraction {
    private final ConsensusSystem system;

    public Application(ConsensusSystem system) {
        this.system = system;
    }

    @Override
    public boolean handle(Message message) {
        switch (message.getType()) {
            case APP_PROPOSE:
                handleAppPropose(message.getAppPropose());
                return true;
            case UC_DECIDE:
                handleUcDecide(message.getUcDecide());
                return true;
        }
        return false;
    }

    /**
     * Starts the consensus algorithm for the current node and initializes the abstractions.
     * Through UC propose, the value to be proposed by the current node is sent to the uniform
     * consensus algorithm. (The value to be proposed by every node, comes from the hub)
     *
     * @param appPropose the message
     */
    private void handleAppPropose(AppPropose appPropose) {
        system.setProcesses(appPropose.getProcessesList());

        system.addAbstraction(new PerfectLink(system));
        system.addAbstraction(new EventuallyPerfectFailureDetector(system));
        system.addAbstraction(new EventualLeaderDetector(system));
        system.addAbstraction(new BestEffortBroadcast(system));
        system.addAbstraction(new EpochChange(system));
        system.addAbstraction(new UniformConsensus(system));

        system.trigger(Message.newBuilder()
                .setType(UC_PROPOSE)
                .setUcPropose(UcPropose.newBuilder()
                        .setValue(appPropose.getValue())
                        .build())
                .build());
    }

    /**
     * Handle the uniform consensus decided value and send it to the hub.
     *
     * @param ucDecide the message
     */
    private void handleUcDecide(UcDecide ucDecide) {
        Message appDecide = Message.newBuilder()
                .setType(APP_DECIDE)
                .setSystemId(system.getSystemId())
                .setAppDecide(AppDecide.newBuilder()
                        .setValue(ucDecide.getValue())
                        .build())
                .build();
        NetworkManager.sendMessage(appDecide, system.getHubIP(), system.getHubPort(), system.getNodePort());
    }
}
