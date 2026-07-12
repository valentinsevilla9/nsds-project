package smartgrid.spark;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsWithStateFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

// El componente 2 lee el topic "measurements" y hace dos analisis en
// paralelo sobre el mismo stream, cada uno respondiendo a una parte
// distinta del enunciado (agregacion por distrito con estado + ventana
// deslizante).
public class EnergyAnalytics {

        // para probar distintos tamanos/intervalos de ventana,
        // es lo que pide el enunciado que se explore
        private static final String WINDOW_DURATION = "2 minutes";
        private static final String SLIDE_DURATION = "30 seconds";
        private static final String WATERMARK_DELAY = "30 seconds";

        public static void main(String[] args) throws TimeoutException, StreamingQueryException, java.io.IOException {
                String master = args.length > 0 ? args[0] : "local[4]";
                String kafkaBroker = args.length > 1 ? args[1] : "localhost:9092";

                SparkSession spark = SparkSession.builder().master(master).appName("SmartGrid-EnergyAnalytics")
                                .getOrCreate();
                spark.sparkContext().setLogLevel("ERROR");

                // el JSON que publica MeasurementService tiene estos 5 campos
                List<StructField> fields = new ArrayList<>();
                fields.add(DataTypes.createStructField("nodeId", DataTypes.StringType, false));
                fields.add(DataTypes.createStructField("nodeType", DataTypes.StringType, false));
                fields.add(DataTypes.createStructField("districtId", DataTypes.StringType, false));
                fields.add(DataTypes.createStructField("value", DataTypes.DoubleType, false));
                fields.add(DataTypes.createStructField("timestamp", DataTypes.LongType, false));
                StructType measurementSchema = DataTypes.createStructType(fields);

                // measurements usa districtId como key de Kafka, asi que todos los
                // mensajes de un mismo distrito van siempre a la misma particion
                Dataset<Row> rawStream = spark.readStream().format("kafka")
                                .option("kafka.bootstrap.servers", kafkaBroker)
                                .option("subscribe", "measurements")
                                .option("startingOffsets", "earliest")
                                .load();

                // parseamos el JSON y nos quedamos tanto con el timestamp crudo (para
                // el estado de carga) como con su version Timestamp (para el windowing por
                // event-time de la Query 2)
                Dataset<Row> measurements = rawStream.selectExpr("CAST(value AS STRING) as json")
                                .select(from_json(col("json"), measurementSchema).as("data"))
                                .select(col("data.nodeId"), col("data.nodeType"), col("data.districtId"),
                                                col("data.value"), col("data.timestamp"),
                                                col("data.timestamp").divide(1000).cast(DataTypes.TimestampType)
                                                                .as("eventTime"));

                // Query 1: estado de carga por distrito. mapGroupsWithState guarda,
                // por cada distrito, cuanta carga tiene acumulada (kWh) durante toda
                // la vida del stream, no solo del micro-batch actual.

                // La logica de como se actualiza esa carga esta en el lambda de abajo: se
                // integra el balance del batch (kW) por las horas reales que han pasado desde
                // la ultima vez, con el mismo modelo fisico que usa district_simulation.c
                KeyValueGroupedDataset<String, MeasurementBean> byDistrict = measurements
                                .select("nodeId", "nodeType", "districtId", "value", "timestamp")
                                .as(Encoders.bean(MeasurementBean.class))
                                .groupByKey((MapFunction<MeasurementBean, String>) MeasurementBean::getDistrictId,
                                                Encoders.STRING());

                MapGroupsWithStateFunction<String, MeasurementBean, DistrictChargeState, DistrictChargeUpdate> updateCharge = (
                                districtId, values, state) -> {
                        DistrictChargeState current = state.exists() ? state.get() : new DistrictChargeState();

                        double batchBalanceKw = 0.0;
                        long numMeasurements = 0;
                        long maxEventTimeMillis = current.getLastEventTimeMillis();

                        while (values.hasNext()) {
                                MeasurementBean m = values.next();
                                batchBalanceKw += m.getValue();
                                numMeasurements++;
                                if (m.getTimestamp() > maxEventTimeMillis)
                                        maxEventTimeMillis = m.getTimestamp();
                        }

                        // la primera vez que vemos este distrito no hay nada con que
                        // comparar el tiempo, asi que no integramos todavia
                        if (current.getLastEventTimeMillis() > 0) {
                                double elapsedHours = (maxEventTimeMillis - current.getLastEventTimeMillis())
                                                / 3_600_000.0;
                                if (elapsedHours > 0) {
                                        double newCharge = current.getAccumulatorChargeKwh()
                                                        + batchBalanceKw * elapsedHours;
                                        current.setAccumulatorChargeKwh(Math.max(0.0, newCharge)); // nunca por debajo
                                                                                                   // de 0
                                }
                        }
                        current.setLastEventTimeMillis(maxEventTimeMillis);
                        state.update(current);

                        return new DistrictChargeUpdate(districtId, batchBalanceKw, numMeasurements,
                                        current.getAccumulatorChargeKwh());
                };

                byDistrict.mapGroupsWithState(updateCharge, Encoders.bean(DistrictChargeState.class),
                                Encoders.bean(DistrictChargeUpdate.class), GroupStateTimeout.NoTimeout())
                                .writeStream()
                                .outputMode("update")
                                .format("console")
                                .option("truncate", false)
                                .queryName("Q1_DistrictStateOfCharge")
                                .start();

                // Query 2: balance medio por distrito en una ventana deslizante, con
                // watermark para no bloquearnos si llega algo desordenado
                measurements.withWatermark("eventTime", WATERMARK_DELAY)
                                .groupBy(window(col("eventTime"), WINDOW_DURATION, SLIDE_DURATION), col("districtId"))
                                .agg(avg("value").as("avgBalance_kW"), sum("value").as("totalBalance_kW"),
                                                count("nodeId").as("numMeasurements"))
                                .writeStream()
                                .outputMode("update")
                                .format("console")
                                .option("truncate", false)
                                .queryName("Q2_SlidingWindow")
                                .start();

                // las dos queries corren indefinidamente hasta que se para el proceso
                spark.streams().awaitAnyTermination();
                spark.close();
        }
}
