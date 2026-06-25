package smartgrid.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

/**
 * EnergyAnalytics — Componente 2, Proyecto #1 Smart Power Grid
 *
 * Lee el topic Kafka "measurements" publicado por el MeasurementService
 * y realiza dos análisis en tiempo real:
 *
 * Query 1 — Agregación por distrito:
 * Mantiene el balance energético acumulado de cada distrito
 * (suma de valores de todos los nodos: positivo=producción, negativo=consumo)
 * Usa outputMode "update" para mostrar solo los distritos que cambian.
 *
 * Query 2 — Ventana temporal deslizante:
 * Calcula el balance promedio por distrito en una ventana de 2 minutos
 * que desliza cada 30 segundos. Usa event-time con watermark de 30s
 * para tolerar eventos fuera de orden.
 *
 * Uso: java EnergyAnalytics [master] [kafkaBroker]
 * master: local[4] (por defecto)
 * kafkaBroker: localhost:9092 (por defecto)
 */
public class EnergyAnalytics {

        // Ventana configurable: cambiad estos valores para el análisis de rendimiento
        private static final String WINDOW_DURATION = "2 minutes";
        private static final String SLIDE_DURATION = "30 seconds";
        private static final String WATERMARK_DELAY = "30 seconds";

        public static void main(String[] args) throws TimeoutException, StreamingQueryException, java.io.IOException {
                final String master = args.length > 0 ? args[0] : "local[4]";
                final String kafkaBroker = args.length > 1 ? args[1] : "localhost:9092";

                final SparkSession spark = SparkSession
                                .builder()
                                .master(master)
                                .appName("SmartGrid-EnergyAnalytics")
                                .getOrCreate();
                spark.sparkContext().setLogLevel("ERROR");

                // ── Schema del JSON en el topic "measurements" ────────────────────────
                // Campos publicados por MeasurementService:
                // nodeId, nodeType, districtId, value (kW), timestamp (epoch ms)

                final List<StructField> fields = new ArrayList<>();
                fields.add(DataTypes.createStructField("nodeId", DataTypes.StringType, false));
                fields.add(DataTypes.createStructField("nodeType", DataTypes.StringType, false));
                fields.add(DataTypes.createStructField("districtId", DataTypes.StringType, false));
                fields.add(DataTypes.createStructField("value", DataTypes.DoubleType, false));
                fields.add(DataTypes.createStructField("timestamp", DataTypes.LongType, false));
                final StructType measurementSchema = DataTypes.createStructType(fields);

                // ── Leer desde Kafka ──────────────────────────────────────────────────
                // El topic "measurements" usa districtId como key para que todos los
                // mensajes del mismo distrito vayan a la misma partición Kafka.

                final Dataset<Row> rawStream = spark
                                .readStream()
                                .format("kafka")
                                .option("kafka.bootstrap.servers", kafkaBroker)
                                .option("subscribe", "measurements")
                                .option("startingOffsets", "earliest")
                                .load();

                // Parsear el JSON del campo value de Kafka y convertir timestamp a tipo
                // Timestamp
                final Dataset<Row> measurements = rawStream
                                .selectExpr("CAST(value AS STRING) as json")
                                .select(from_json(col("json"), measurementSchema).as("data"))
                                .select(
                                                col("data.nodeId"),
                                                col("data.nodeType"),
                                                col("data.districtId"),
                                                col("data.value"),
                                                // Convertir epoch millis a Timestamp para event-time windowing
                                                (col("data.timestamp").divide(1000)).cast(DataTypes.TimestampType)
                                                                .as("eventTime"));

                // ── Query 1: Agregación por distrito ─────────────────────────────────
                // Suma el balance energético acumulado por distrito.
                // outputMode "update": solo muestra filas que cambian en cada micro-batch.

                measurements
                                .groupBy("districtId")
                                .agg(
                                                sum("value").as("totalBalance_kW"),
                                                count("nodeId").as("numMeasurements"),
                                                avg("value").as("avgValue_kW"))
                                .writeStream()
                                .outputMode("update")
                                .format("console")
                                .option("truncate", false)
                                .queryName("Q1_DistrictAggregation")
                                .start();

                // ── Query 2: Ventana temporal deslizante ──────────────────────────────
                // Balance promedio por distrito en ventana de 2 minutos, deslizando cada 30s.
                // Watermark de 30s para tolerar eventos fuera de orden.

                measurements
                                .withWatermark("eventTime", WATERMARK_DELAY)
                                .groupBy(
                                                window(col("eventTime"), WINDOW_DURATION, SLIDE_DURATION),
                                                col("districtId"))
                                .agg(
                                                avg("value").as("avgBalance_kW"),
                                                sum("value").as("totalBalance_kW"),
                                                count("nodeId").as("numMeasurements"))
                                .writeStream()
                                .outputMode("update")
                                .format("console")
                                .option("truncate", false)
                                .queryName("Q2_SlidingWindow")
                                .start();

                // Esperar a que ambas queries terminen (corren indefinidamente)
                spark.streams().awaitAnyTermination();
                spark.close();
        }
}