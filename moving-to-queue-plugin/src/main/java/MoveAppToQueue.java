import detection.AnalysisContainer;
import detection.KeyedMessage;
import feedback.AbstractFeedback;
import utils.ShellCommandExecutor;

import java.io.IOException;
import java.util.*;

public class MoveAppToQueue extends AbstractFeedback {

    final int timeoutThreshold = 8;
    final Double CLUSTER_MEMORY = 64 * 1024 * 1024 * 1024D;
    final int minimumMovingInterval = 5;
    int lastMovingTime = minimumMovingInterval;
    Map<String, Integer> pendingAppCounterMap;
    Map<String, Integer> underUtilizedAppCounterMap;
    Map<String, Double> appMemoryUsage;
    List<SchedulerQueue> leafQueues;

    public MoveAppToQueue(String name, Integer interval) {
        super(name, interval);
        pendingAppCounterMap = new HashMap<>();
        underUtilizedAppCounterMap = new HashMap<>();
        appMemoryUsage = new HashMap<>();
        leafQueues = new LinkedList<>();
        initQueues();
        System.out.print("MoveAppToQueue plugin is loaded\n");
    }

    @Override
    public void action(List<Map<String, AnalysisContainer>> list) {
        // Find "app.state" message from this window. Then, app/remove the corresponding entry in the pendingAppCounterMap.
        Map<String, Double> currentMemoryUsage = new HashMap<>();
        for(Map<String, AnalysisContainer> containerMap: list) {
            for(Map.Entry<String, AnalysisContainer> entry: containerMap.entrySet()) {
                // if the app is stuck in ACCEPTED state,
                // add it to the pending queue
                updatePendingAppState(entry);

                // update the app memory usages in this window.
                String containerId = entry.getKey();
                if (containerId.matches("container.*")) {
                    String appId = containerToAppId(containerId);
                    currentMemoryUsage.putIfAbsent(appId, 0D);
                    Double memoryUsage = entry.getValue().memory;
                    Double memorySum = currentMemoryUsage.get(appId);
                    currentMemoryUsage.put(appId, memorySum + memoryUsage);
                }
            }
        }

        // Update the pendingAppCounterMap periodically
        updatePendingApp();

        // Update the underUtilizedMap periodically
        updateUnderUtilizedApp(currentMemoryUsage);

        maybeMoveApp();
    }

    private void updatePendingAppState(Map.Entry<String, AnalysisContainer> entry) {
        String appId = entry.getKey();
        if (!appId.matches("app.*")) {
            return;
        }
        AnalysisContainer value = entry.getValue();
        List<KeyedMessage> messageList = value.instantMessages.get("app.state");
        if (messageList != null) {
            for (KeyedMessage message : messageList) {
                Double doubleValue = message.value;
                String messageKey = message.key;
                if (messageKey.equals("app.state")) {
                    System.out.printf("get app state log. app: %s, value: %f\n", appId, doubleValue);
                    int compareResult = Double.compare(doubleValue, 4);
                    if (compareResult == 1) {
                        System.out.printf("remove app: %s from pending.\n", appId);
                        pendingAppCounterMap.remove(appId);
                    } else if (compareResult == 0) {
                        System.out.printf("move app: %s to pending.\n", appId);
                        pendingAppCounterMap.put(appId, 1);
                    }
                }
            }
        }
    }

    private void updatePendingApp() {
        for (String id: pendingAppCounterMap.keySet()) {
            pendingAppCounterMap.compute(id, (key, value) -> value + 1);
        }
    }

    private void updateUnderUtilizedApp(Map<String, Double> currentMemoryMap) {
        for (Map.Entry<String, Double> entry: currentMemoryMap.entrySet()) {
            String appId = entry.getKey();
            Double newUsage = entry.getValue();
            Double usage = appMemoryUsage.get(appId);
            if (usage != null) {
                if (usage < CLUSTER_MEMORY * 0.8) {
                    if (newUsage - usage > 0 && (newUsage - usage) < usage * 0.1) {
                        underUtilizedAppCounterMap.compute(appId, (key, value)-> value == null ? 0 : value + 1);
                    } else {
                        underUtilizedAppCounterMap.compute(appId, (key, value) -> value == null ? 0 : Math.max(0, value - 1));
                    }
                }
            }
        }
        appMemoryUsage.putAll(currentMemoryMap);
    }

    private void maybeMoveApp() {
        lastMovingTime += 1;
        if (lastMovingTime <= minimumMovingInterval) {
            return;
        }
        String appToMove = null;
        for (Map.Entry<String, Integer> counterEntry: pendingAppCounterMap.entrySet()) {
            if (counterEntry.getValue() > timeoutThreshold) {
                appToMove = counterEntry.getKey();
                break;
            }
        }

        if (appToMove != null) {
            pendingAppCounterMap.remove(appToMove);
        } else {
            // if no app is in ACCEPTED state,
            // we further find if any app that is under-utilized.
            for (Map.Entry<String, Integer> counterEntry: underUtilizedAppCounterMap.entrySet()) {
                if (counterEntry.getValue() > timeoutThreshold) {
                    appToMove = counterEntry.getKey();
                    break;
                }
            }
            if (appToMove != null) {
                underUtilizedAppCounterMap.remove(appToMove);
            }
        }

        if (appToMove != null) {
            moveApp(appToMove);
            lastMovingTime = 0;
        }
    }

    private void moveApp(String appId) {
        String queueToMove = findOptimalQueue();
        System.out.printf("Moving App: %s to queue: %s\n", appId, queueToMove);

        String command = "/home/eddie/hookup/script/move-app-to-queue.sh " + appId + " " + queueToMove;
        ShellCommandExecutor executor = new utils.ShellCommandExecutor(command);
        try {
            executor.execute();
        } catch (IOException e) {
            e.printStackTrace();
        }
        String result = executor.getOutput();
        if (result.matches(".*successfully.*")) {
            System.out.printf("Successfully moved App: %s to queue: %s\n", appId, queueToMove);
        } else {
            System.out.printf("%s\n", result);
        }
    }

    /**
     * execute shell script to find the available queue.
     *
     * @return the name of the available queue.
     */
    private String findOptimalQueue() {
        String optimalQueueName = "";
        double optimalRemaining = -10d;
        for (SchedulerQueue queue: leafQueues) {
            queue.remainingCapacity = getQueueRemainingCapacity(queue.name);
            if (optimalRemaining < queue.remainingCapacity) {
                optimalQueueName = queue.name;
            }
        }
        return optimalQueueName;
    }

    private void initQueues() {
        leafQueues.add(new SchedulerQueue("default", true));
        leafQueues.add(new SchedulerQueue("alpha", true));
        leafQueues.add(new SchedulerQueue("beta", true));
        leafQueues.add(new SchedulerQueue("gama", true));
    }

    private Double getQueueRemainingCapacity(String name) {
        String command = "/home/eddie/hadoop-2.7.3/bin/yarn queue -status " + name;
        ShellCommandExecutor executor = new utils.ShellCommandExecutor(command);
        try {
            executor.execute();
        } catch (IOException e) {
            e.printStackTrace();
        }
        String result = executor.getOutput();
        String[] resultLines = result.split("\\n");
        if (resultLines.length != 8) {
            isRunning = false;
            return -1d;
        }
        double capacity = parseCapacityFromLine(resultLines[3]);
        double current = parseCapacityFromLine(resultLines[4]) / 100;
        double maximum = parseCapacityFromLine(resultLines[5]);
        double remaining = maximum - capacity * current;

        return remaining;

    }

    private double parseCapacityFromLine(String line) {
        String doubleStr = line.split(":")[1].replace("%", "").trim();
        double value = Double.valueOf(doubleStr);
        return value;

    }

    private class SchedulerQueue {
        public String name;
        public Boolean isLeaf;
        public Double remainingCapacity;

        public SchedulerQueue(String name, Boolean isLeaf) {
            this.name = name;
            this.isLeaf = isLeaf;
            remainingCapacity = 0d;
        }
    }

    private String containerToAppId(String containerId) {
        String[] parts = containerId.split("_");
        String appId = "application_" + parts[1] + "_" + parts[2];

        return appId;
    }
}
