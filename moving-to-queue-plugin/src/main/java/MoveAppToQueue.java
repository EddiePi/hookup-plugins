import detection.AnalysisContainer;
import detection.KeyedMessage;
import feedback.AbstractFeedback;
import utils.ShellCommandExecutor;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class MoveAppToQueue extends AbstractFeedback {

    final int timeoutThreshold = 15;
    Map<String, Integer> pendingAppMap;
    List<SchedulerQueue> leafQueues;

    public MoveAppToQueue(String name, Integer interval) {
        super(name, interval);
        pendingAppMap = new HashMap<>();
        leafQueues = new LinkedList<>();
        initQueues();
    }

    @Override
    public void action(List<Map<String, AnalysisContainer>> list) {
        // Find "app.state" message from this window. Then, app/remove the corresponding entry in the pendingAppMap.
        for(Map<String, AnalysisContainer> containerMap: list) {
            for(Map.Entry<String, AnalysisContainer> entry: containerMap.entrySet()) {
                String key = entry.getKey();
                if (!key.matches("app.*")) {
                    continue;
                }
                AnalysisContainer value = entry.getValue();
                List<KeyedMessage> messageList = value.instantMessages.get("app.state");
                if (messageList != null) {
                    for (KeyedMessage message : messageList) {
                        Double doubleValue = message.value;
                        String messageKey = message.key;
                        if (messageKey.equals("app.state")) {
                            int compareResult = Double.compare(doubleValue, 4);
                            if (compareResult == 1) {
                                pendingAppMap.remove(messageKey);
                            } else if (compareResult == 0) {
                                pendingAppMap.put(messageKey, 1);
                            }
                        }
                    }
                }
            }
        }

        // Update the pendingAppMap periodically
        for (String key: pendingAppMap.keySet()) {
            Integer intValue = pendingAppMap.get(key);
            if (intValue > timeoutThreshold) {
                pendingAppMap.remove(key);
                moveApp(key);
            } else {
                intValue += 1;
                pendingAppMap.put(key, intValue);
            }
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
}
