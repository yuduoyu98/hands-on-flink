package demo.functions;

import demo.bean.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class RandomEventTsSource implements SourceFunction<Event> {

    /**
     * 乱序程度
     */
    private final Integer disOrderLevel;

    private final Random random;

    private volatile boolean isRunning = true;

    private int skuCount;

    public RandomEventTsSource(Integer disOrderLevel, int skuCount) {
        this.disOrderLevel = disOrderLevel;
        this.ts = System.currentTimeMillis() / (24 * 3600) * (24 * 3600);
        this.random = new Random();
        this.skuCount = skuCount;
    }

    private Long ts;

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {

        while (isRunning) {
            String id = UUID.randomUUID().toString();
            double sell = random.nextDouble() * random.nextInt(100);
            int skuId = 1 + random.nextInt(skuCount);
            Event event = new Event(id, ts, sell, skuId);
            ctx.collect(event);
            //计算与之前ts的偏移量
            int offset = random.nextInt(disOrderLevel * 2) - disOrderLevel / (1 + random.nextInt(2));
            ts += offset * 1000L;
            TimeUnit.SECONDS.sleep(random.nextInt(4));
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
