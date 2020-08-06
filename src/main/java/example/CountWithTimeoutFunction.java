package example;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CountWithTimeoutFunction extends KeyedProcessFunction<Tuple, Tuple4<String, Double, Long, Long>, Tuple4<String, Double, Long, String>> {
    /**
     * The state that is maintained by this process function
     */

//    private static int TIMEOUT = 600000;
    Logger LOG = LoggerFactory.getLogger(CountWithTimeoutFunction.class);
    private static int MINUTE = 1;

    private ValueState<CountWithTimestamp> state;
    private ValueState<Long> timeDiff;

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor("countState", CountWithTimestamp.class));
        timeDiff = getRuntimeContext().getState(new ValueStateDescriptor("timeDiffState", Long.TYPE));
    }

    @Override
    public void processElement(Tuple4<String, Double, Long, Long> value, final Context ctx, Collector<Tuple4<String, Double, Long, String>> out) throws Exception {
        if(ctx.timestamp() > ctx.timerService().currentWatermark() + (60000 * MINUTE) && ctx.timerService().currentWatermark() != 0){
            onTimer(ctx.timestamp(), new OnTimerContext() {
                @Override
                public TimeDomain timeDomain() {
                    return null;
                }

                @Override
                public Tuple getCurrentKey() {
                    return ctx.getCurrentKey();
                }

                @Override
                public Long timestamp() {
                    return ctx.timestamp();
                }

                @Override
                public TimerService timerService() {
                    return ctx.timerService();
                }

                @Override
                public <X> void output(OutputTag<X> outputTag, X x) {

                }
            }, out);
        }
        // retrieve the current count
        CountWithTimestamp current = state.value();
        if (current == null) {
            current = new CountWithTimestamp();
            current.key = value.f0;
            // set the state's timestamp to the record's assigned event time timestamp
            current.firstModified = value.f2;
//            current.firstModified = ctx.timestamp();
            ////////////////// TIMER SETTING
            // schedule the next timer 60 seconds from the current processing time
            timeDiff.update(ctx.timerService().currentProcessingTime() - ctx.timestamp());
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + (60000 * MINUTE));
        }
        if(value.f2 >= current.firstModified && value.f2 <= current.firstModified + (60000 * MINUTE)) {
            timeDiff.update(ctx.timerService().currentProcessingTime() - ctx.timestamp());
            // update the state's count
            current.count++;

            // update the state's count
            current.txn_amt += value.f1;
        }

//        LOG.info("KEY:" + current.key + " COUNT:" + current.count + " TXN_AMT:" + current.txn_amt + " lastModified:" + current.firstModified);

        // write the state back
        state.update(current);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple4<String, Double, Long, String>> out) throws Exception {
        CountWithTimestamp result = state.value();
        long timeDif = 0;
        try {
            timeDif = timeDiff.value();
        }catch (NullPointerException ne){
            LOG.info("TIMEDIFF VALUE: " + timeDiff.value());
        }
        if (result != null) {
            if ((timestamp - timeDif) >= (result.firstModified + (60000 * MINUTE)) || (timestamp - timeDif) > ctx.timerService().currentProcessingTime()) {
                LOG.info("=========================");
                out.collect(new Tuple4<String, Double, Long, String>(result.key, result.txn_amt, result.count, result.firstModified+"_"+(result.firstModified + (60000 * MINUTE))));
                state.clear();
                timeDiff.clear();
            }else {
                ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + (Math.abs((timestamp - timeDif) - (result.firstModified + (60000 * MINUTE)))));
            }
        }
    }
}
