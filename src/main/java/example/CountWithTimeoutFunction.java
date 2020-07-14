package example;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CountWithTimeoutFunction extends KeyedProcessFunction<Tuple, Tuple4<String, Double, Long, Long>, Tuple4<String, Double, Long, String>> {
    /**
     * The state that is maintained by this process function
     */

//    private static int TIMEOUT = 600000;
    Logger LOG = LoggerFactory.getLogger(CountWithTimeoutFunction.class);

    private ValueState<CountWithTimestamp> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor("countState", CountWithTimestamp.class));
    }

    @Override
    public void processElement(Tuple4<String, Double, Long, Long> value, Context ctx, Collector<Tuple4<String, Double, Long, String>> out) throws Exception {

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
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 600000);
        }
        if(value.f2 >= current.firstModified) {
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

        // get the state for the key that scheduled the timer
        CountWithTimestamp result = state.value();

//        LOG.info("==================== TIMEOUT: " + result.key);
        // emit the state on timeout
        LOG.info("==KEY: " + result.key + ", AMT: " + result.txn_amt + ", COUNT: " + result.count);
        out.collect(new Tuple4<String, Double, Long, String>(result.key, result.txn_amt, result.count, result.firstModified+"_"+(result.firstModified + 600000)));
        state.clear();
    }
}
