package consensus.utils;

import consensus.Paxos;

import java.util.ArrayList;
import java.util.List;

public class ProcessListUtils {
    public static boolean contains(List<Paxos.ProcessId> processList, Paxos.ProcessId containedProcess) {
        for (Paxos.ProcessId processId : processList) {
            if (processId.getPort() == containedProcess.getPort()) {
                return true;
            }
        }
        return false;
    }

    public static void remove(List<Paxos.ProcessId> processList, Paxos.ProcessId removedProcess) {
        int removeIndex = -1;
        for (int i = 0; i < processList.size(); i++) {
            if (processList.get(i).getPort() == removedProcess.getPort()) {
                removeIndex = i;
                break;
            }
        }
        if (removeIndex != -1) {
            processList.remove(removeIndex);
        }
    }

    public static List<Paxos.ProcessId> difference(List<Paxos.ProcessId> list1, List<Paxos.ProcessId> list2) {
        List<Paxos.ProcessId> difference = new ArrayList<>(list1);
        for (Paxos.ProcessId processId : list2) {
            remove(difference, processId);
        }
        return difference;
    }
}
