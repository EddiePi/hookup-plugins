import detection.AnalysisContainer;
import detection.KeyedMessage;
import feedback.AbstractFeedback;
import utils.ShellCommandExecutor;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class TimeOutRestart extends AbstractFeedback {

    Map<String, Double> appState;
    Map<String, Integer> containerTimeoutMap;
    private String HADOOP_HOME = "/home/eddie/hadoop-2.7.3";

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
            String appStartCommand = "";
            String outputDir;
            try {
                appStartCommand = getAppCommand(appId);
            } catch (IOException e) {
                e.printStackTrace();
            }
            outputDir = getAppOutputDir(appStartCommand);
            killApp(appId);
            String realCommand = appStartCommand;

            List<String> commands = new ArrayList<>();
            commands.add("/bin/bash");
            commands.add("-c");
            if (outputDir != null) {
                realCommand = HADOOP_HOME + "/bin/hadoop fs -rm -r " + outputDir + " && " + realCommand;
            }
            commands.add(realCommand);
            ProcessBuilder pb = new ProcessBuilder(commands);
            pb.environment().put("YARN_CONF_DIR", HADOOP_HOME + "/etc/hadoop");
            pb.redirectError(new File("/dev/null"));
            pb.redirectInput(new File("/dev/null"));
            try {
                Process p = pb.start();
                p.waitFor();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    // public for test
    public String getAppCommand(String appId) throws IOException {
        String appCommand;

        // find the type of the app
        ShellCommandExecutor executor = new ShellCommandExecutor(HADOOP_HOME + "/bin/yarn application -status " + appId);
        executor.execute();
        String[] lines = executor.getOutput().split("\n");
        if (lines.length < 10) {
            System.out.print("connect to RM failed or no such appId");
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

    private String getAppOutputDir(String startCommand) {
        String[] parts = startCommand.split("\\s+");
        String outputDir = parts[parts.length - 1];
        if (outputDir.toLowerCase().matches(".*output.*")) {
            return outputDir;
        }
        return null;
    }

    // public for test
    public void killApp(String appId) {
        ShellCommandExecutor executor = new ShellCommandExecutor(yarnHome + " application -kill " + appId);
    }
}
