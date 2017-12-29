import detection.AnalysisContainer;
import detection.KeyedMessage;
import feedback.AbstractFeedback;
import utils.ShellCommandExecutor;

import java.io.IOException;
import java.util.*;

/**
 * Created by Eddie on 2017/12/27.
 */
public class TimeOutRestart extends AbstractFeedback {

    Map<String, Double> appState;
    Map<String, Integer> containerTimeoutMap;
    private String yarnHome = "/home/eddie/hadoop-2.7.3/bin/yarn";

    public TimeOutRestart(String name, Integer interval) {
        super(name, interval);

        appState = new HashMap<>();
        containerTimeoutMap = new HashMap<>();
    }

    @Override
    public void action(List<Map<String, AnalysisContainer>> list) {
        for (Map<String, AnalysisContainer> containerMap: list) {
            for (Map.Entry<String, AnalysisContainer> entry: containerMap.entrySet()) {
                // this loop layer is for container
                String id = entry.getKey();
                AnalysisContainer value = entry.getValue();

                if (id.matches("app.*")) {
                    List<KeyedMessage> messageList = value.instantMessages.get("app.state");
                    if (messageList != null) {
                        for (KeyedMessage message : messageList) {
                            Double doubleValue = message.value;
                            String messageKey = message.key;
                            if (messageKey.equals("app.state")) {
                                if (Double.compare(doubleValue, 5) == 1) {
                                    appState.remove(id);
                                } else if (Double.compare(doubleValue, 5) == 0) {
                                    appState.put(id, doubleValue);
                                    containerTimeoutMap.put(id, 0);
                                }
                            }
                        }
                    }
                }

                // add 1 to all container id count
                Set<String> containerKeySet = containerTimeoutMap.keySet();
                for (String containerId : containerKeySet) {
                    Integer count = containerTimeoutMap.get(containerId) + 1;
                    containerTimeoutMap.put(containerId, count);
                }

                // if we receive a new log message of a container, reset the count
                if (id.matches("container.*") && !id.matches(".*_000001")) {
                    containerTimeoutMap.put(id, 0);
                }

                maybeRestartApplications();
            }
        }
    }



    private String containerToAppId(String containerId) {
        String[] parts = containerId.split("_");
        String appId = "application_" + parts[1] + "_" + parts[2];

        return appId;
    }

    private void maybeRestartApplications() {
        Set<String> appToRestart = new HashSet<>();
        for (Map.Entry<String, Integer> entry: containerTimeoutMap.entrySet()) {
            if (entry.getValue() > 10) {
                String appId = containerToAppId(entry.getKey());
                appToRestart.add(appId);
            }
        }
        for (String appId: appToRestart) {
            String command = "";
            try {
                command = getAppCommand(appId);
            } catch (IOException e) {
                e.printStackTrace();
            }
            killApp(appId);

            // start the app again
            ShellCommandExecutor executor = new ShellCommandExecutor(command);
            try {
                executor.execute();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    private String getAppCommand(String appId) throws IOException {
        String appCommand;

        // find the type of the app
        ShellCommandExecutor executor = new ShellCommandExecutor(yarnHome + " application -status " + appId);
        executor.execute();
        String[] lines = executor.getOutput().split("\n");
        if (lines.length < 10) {
            return "";
        }
        String appType = "";
        if (lines[3].matches(".*Application-Name.*")) {
            appType = lines[3].split(":")[1].trim();
        }

        // find the start command of the app
        executor = new ShellCommandExecutor("ps -aux | grep -n 'python2.*" + appType + "'");
        executor.execute();
        lines = executor.getOutput().split("\n");
        appCommand = lines[0].substring(lines[0].indexOf("python2"));
        return appCommand;
    }


    private void killApp(String appId) {
        ShellCommandExecutor executor = new ShellCommandExecutor(yarnHome + " application -kill " + appId);
    }
}