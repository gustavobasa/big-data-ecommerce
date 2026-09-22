package com.ecommerce.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;

/**
 * Job da Semana 2: consome os eventos que o Flume vai gravando em disco,
 * aplica watermark (tolerando eventos atrasados) e conta cliques/add_to_cart
 * por produto numa janela deslizante -> "trend topics" em tempo real.
 *
 * Args (opcionais):
 *   arg0 = diretório de entrada  (padrão: /data/flume-output)
 *   arg1 = diretório de saída    (padrão: /data/flink-output)
 */
public class EcommerceStreamJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String inputDir = args.length > 0 ? args[0] : "/data/flume-output";
        String outputDir = args.length > 1 ? args[1] : "/data/flink-output";

        // ---------- SOURCE: lê continuamente os arquivos que o Flume gera ----------
        FileSource<String> source = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), new Path(inputDir))
                .monitorContinuously(Duration.ofSeconds(5))
                .build();

        DataStream<String> linhas = env.fromSource(
                source, WatermarkStrategy.noWatermarks(), "flume-output-source");

        // ---------- PARSE: JSON -> objeto tipado (descarta o que não interessa) ----------
        DataStream<EventoEcommerce> eventos = linhas
                .map(new ParseEventoFn())
                .filter(e -> e != null);

        // ---------- WATERMARK: tolera até 40s de atraso (gerador usa até 30s) ----------
        DataStream<EventoEcommerce> comWatermark = eventos.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<EventoEcommerce>forBoundedOutOfOrderness(Duration.ofSeconds(40))
                        .withTimestampAssigner(new EventTimestampAssigner())
        );

        // ---------- JANELA DESLIZANTE: cliques/add_to_cart por produto ----------
        DataStream<String> trending = comWatermark
                .keyBy(new ProdutoKeySelector())
                .window(SlidingEventTimeWindows.of(Duration.ofMinutes(1), Duration.ofSeconds(30)))
                .process(new ContadorPorProduto());

        trending.print(); // aparece no log/stdout do TaskManager

        trending.sinkTo(
                FileSink.forRowFormat(new Path(outputDir), new SimpleStringEncoder<String>("UTF-8"))
                        .build()
        );

        env.execute("Ecommerce - Trending Products (janela deslizante + watermark)");
    }

    // ==================== Modelo do evento ====================
    public static class EventoEcommerce implements Serializable {
        public String eventType;
        public String productId;
        public long eventTimeMillis;

        public EventoEcommerce() {}

        public EventoEcommerce(String eventType, String productId, long eventTimeMillis) {
            this.eventType = eventType;
            this.productId = productId;
            this.eventTimeMillis = eventTimeMillis;
        }
    }

    // ==================== Parser JSON -> EventoEcommerce ====================
    public static class ParseEventoFn implements MapFunction<String, EventoEcommerce> {
        @Override
        public EventoEcommerce map(String line) {
            if (line == null || line.isBlank()) return null;
            try {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode node = mapper.readTree(line);

                String eventType = node.path("event_type").asText(null);
                if (!"click".equals(eventType) && !"add_to_cart".equals(eventType)) {
                    return null; // só nos interessa cliques/carrinho pra "trend topics"
                }

                String productId = node.path("product_id").asText(null);
                String eventTimeStr = node.path("event_time").asText(null);
                if (productId == null || eventTimeStr == null) return null;

                long ts = Instant.parse(eventTimeStr).toEpochMilli();
                return new EventoEcommerce(eventType, productId, ts);
            } catch (Exception ex) {
                // linha incompleta (arquivo sendo escrito) ou malformada -> ignora
                return null;
            }
        }
    }

    // ==================== Extrator de timestamp pro watermark ====================
    public static class EventTimestampAssigner
            implements SerializableTimestampAssigner<EventoEcommerce> {
        @Override
        public long extractTimestamp(EventoEcommerce evento, long recordTimestamp) {
            return evento.eventTimeMillis;
        }
    }

    // ==================== Chave de particionamento (por produto) ====================
    public static class ProdutoKeySelector implements KeySelector<EventoEcommerce, String> {
        @Override
        public String getKey(EventoEcommerce evento) {
            return evento.productId;
        }
    }

    // ==================== Agregação da janela ====================
    public static class ContadorPorProduto
            extends ProcessWindowFunction<EventoEcommerce, String, String, TimeWindow> {
        @Override
        public void process(String produtoId, Context ctx, Iterable<EventoEcommerce> elementos,
                             Collector<String> out) {
            long total = 0;
            for (EventoEcommerce e : elementos) total++;

            String inicio = Instant.ofEpochMilli(ctx.window().getStart()).toString();
            String fim = Instant.ofEpochMilli(ctx.window().getEnd()).toString();

            out.collect(String.format(
                    "janela=[%s -> %s] produto=%s eventos=%d",
                    inicio, fim, produtoId, total));
        }
    }
}
