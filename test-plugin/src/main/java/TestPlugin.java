import detection.AnalysisContainer;
import feedback.AbstractFeedback;

import java.util.List;
import java.util.Map;

/**
 * Created by Eddie on 2017/12/12.
 */
public class TestPlugin extends AbstractFeedback {
    public TestPlugin(String name, Integer interval) {
        super(name, interval);
    }

    @Override
    public void action(List<Map<String, AnalysisContainer>> list) {
        for(Map<String, AnalysisContainer> containerMap: list) {
            for(Map.Entry<String, AnalysisContainer> containerEntry: containerMap.entrySet()) {
                String containerId = containerEntry.getKey();
                AnalysisContainer container = containerEntry.getValue();
                System.out.printf("containerId: %s, timestamp: %d, memory usage %d\n", containerId, container.getTimestamp(), container.memory);
            }
        }
    }
}
