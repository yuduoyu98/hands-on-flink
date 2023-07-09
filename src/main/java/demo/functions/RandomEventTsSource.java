package demo.functions;

import demo.bean.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class RandomEventTsSource implements SourceFunction<Event> {

    /**
     * 乱序程度:
     *  Level1：不乱序
     *  Level2：基本不乱序
     *  Level3：乱序较多
     */
    private final Level disOrderLevel;

    /**
     * 生成速度
     */
    private final Level generateSpeedLevel;

    /**
     * 数据密度
     */
    private final Level dataDensityLevel;

    public enum Level {
        LEVEL_1,
        LEVEL_2,
        LEVEL_3
    }

    public int nextEventTsOffset(Level dataDensityLevel, Level disOrderLevel) {
        int nextEventTsUpperBound = getNextEventTsUpperBound(dataDensityLevel);
        int outOfOrderParam = getOutOfOrderParam(disOrderLevel);
        return random.nextInt(nextEventTsUpperBound * 2) - nextEventTsUpperBound / (1 + random.nextInt(outOfOrderParam));
    }

    private static int getOutOfOrderParam(Level disOrderLevel) {
        switch (disOrderLevel) {
            case LEVEL_1:
                return Integer.MAX_VALUE;
            case LEVEL_2:
                return 5;
            case LEVEL_3:
                return 2;
            default:
                throw new IllegalArgumentException("数据乱序程度为空");
        }
    }

    private static int getNextEventTsUpperBound(Level dataDensityLevel) {
        switch (dataDensityLevel) {
            case LEVEL_1:
                return 10;
            case LEVEL_2:
                return 5;
            case LEVEL_3:
                return 3;
            default:
                throw new IllegalArgumentException("数据密度等级为空");
        }
    }

    private static int getSleepSecondsUpperBoundPerEvent(Level generateSpeedLevel) {
        switch (generateSpeedLevel) {
            case LEVEL_1:
                return 3;
            case LEVEL_2:
                return 1;
            case LEVEL_3:
                return 0;
            default:
                throw new IllegalArgumentException("数据生成速度等级为空");
        }
    }

    private final Random random;

    private volatile boolean isRunning = true;

    private static final String[] keywords;

    static {
        keywords = new String[]{
                "flink",
                "java",
                "spark",
                "hbase",
                "clickhouse",
                "doris"
        };
    }

    public RandomEventTsSource(Level disOrderLevel, Level generateSpeedLevel, Level dataDensityLevel) {
        this.disOrderLevel = disOrderLevel;
        this.generateSpeedLevel = generateSpeedLevel;
        this.dataDensityLevel = dataDensityLevel;
        this.ts = System.currentTimeMillis() / (24 * 3600) * (24 * 3600);
        this.random = new Random();
    }

    private Long ts;

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {

        while (isRunning) {
            String id = UUID.randomUUID().toString();
            int count = 1 + random.nextInt(4);
            int index = random.nextInt(keywords.length);
            Event event = new Event(id, ts, count, keywords[index]);
            ctx.collect(event);
            //计算与之前ts的偏移量
            int offset = nextEventTsOffset(dataDensityLevel, disOrderLevel);
            ts += offset * 1000L;
            int sleepSecondsUpperBoundPerEvent = getSleepSecondsUpperBoundPerEvent(generateSpeedLevel);
            if (sleepSecondsUpperBoundPerEvent > 0) {
                TimeUnit.SECONDS.sleep(random.nextInt(sleepSecondsUpperBoundPerEvent));
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
