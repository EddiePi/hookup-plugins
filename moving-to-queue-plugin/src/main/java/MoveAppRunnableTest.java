import org.junit.Before;
import org.junit.Test;
import utils.ShellCommandExecutor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by eddie on 1/9/18.
 */
public class MoveAppRunnableTest {

    List<String> leafQueues;
    @Before
    public void setUp() throws Exception {
        leafQueues = new ArrayList<>();
        leafQueues.add("default");
        leafQueues.add("alpha");
        leafQueues.add("beta");
        leafQueues.add("gama");
    }

    @Test
    public void findOptimalQueueTest() throws Exception {
        System.out.printf("opt: %s\n", findOptimalQueue());
    }

    public String findOptimalQueue() {
        String optimalQueueName = "";
        double optimalRemaining = -10d;
        for (String queue: leafQueues) {
            double remainingCapacity = getQueueRemainingCapacity(queue);
            System.out.printf("queue: %s, capa: %f\n", queue, remainingCapacity);
            if (optimalRemaining < remainingCapacity) {
                optimalRemaining = remainingCapacity;
                optimalQueueName = queue;
            }
        }
        return optimalQueueName;
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

}