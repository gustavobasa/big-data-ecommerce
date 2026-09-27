package com.ecommerce.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.OpenContext;
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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;

import java.io.Serializable;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
        // Reinício automático: se o job falhar (ex.: colisão de leitura com o
        // Flume ainda escrevendo um arquivo), o Flink tenta de novo sozinho
        // em vez de derrubar o pipeline de vez - importante pra manter tudo
        // rodando durante a gravação da demonstração.
        Configuration conf = new Configuration();
        conf.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        conf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 2147483647);
        conf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(5));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        String inputDir = args.length > 0 ? args[0] : "/data/flume-output";
        String outputDir = args.length > 1 ? args[1] : "/data/flink-output";

        // ---------- SOURCE: lê continuamente os arquivos que o Flume gera ----------
        // Intervalo de 20s (maior que o rollInterval de 10s do Flume) para reduzir
        // a chance de o Flink listar um arquivo que o Flume ainda está escrevendo.
        FileSource<String> source = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), new Path(inputDir))
                .monitorContinuously(Duration.ofSeconds(20))
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

    // ==================== Agregação da janela + alerta no HBase ====================
    public static class ContadorPorProduto
            extends ProcessWindowFunction<EventoEcommerce, String, String, TimeWindow> {

        // Limiar de eventos na janela a partir do qual disparamos um alerta de "trending"
        private static final long LIMIAR_ALERTA = 5;

        // Endpoint REST do HBase dentro da rede do docker-compose (serviço "hbase", porta 8080)
        private static final String HBASE_REST_URL =
                System.getenv().getOrDefault("HBASE_REST_URL", "http://hbase:8080");
        private static final String TABELA_ALERTAS = "flink_alertas";

        private transient HttpClient httpClient;
        private transient ObjectMapper mapper;

        @Override
        public void open(OpenContext openContext) {
            httpClient = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(5))
                    .build();
            mapper = new ObjectMapper();
        }

        @Override
        public void process(String produtoId, Context ctx, Iterable<EventoEcommerce> elementos,
                             Collector<String> out) {
            long total = 0;
            for (EventoEcommerce e : elementos) total++;

            long inicioMillis = ctx.window().getStart();
            long fimMillis = ctx.window().getEnd();
            String inicio = Instant.ofEpochMilli(inicioMillis).toString();
            String fim = Instant.ofEpochMilli(fimMillis).toString();

            out.collect(String.format(
                    "janela=[%s -> %s] produto=%s eventos=%d",
                    inicio, fim, produtoId, total));

            if (total >= LIMIAR_ALERTA) {
                enviarAlertaHBase(produtoId, total, inicioMillis, inicio, fim);
            }
        }

        /**
         * Grava um alerta de "produto em alta" na tabela flink_alertas do HBase,
         * via API REST (mesmo padrão usado pelo job Spark em hbase_rest.py) - evita
         * ter que casar versões de client jar do HBase entre Flink/Spark/HBase.
         */
        private void enviarAlertaHBase(String produtoId, long total, long inicioMillis,
                                        String inicio, String fim) {
            try {
                String rowKey = produtoId + "-" + inicioMillis;

                List<Map<String, String>> celulas = List.of(
                        celula("produto", produtoId),
                        celula("eventos", String.valueOf(total)),
                        celula("janela_inicio", inicio),
                        celula("janela_fim", fim)
                );

                Map<String, Object> linha = new LinkedHashMap<>();
                linha.put("key", b64(rowKey));
                linha.put("Cell", celulas);

                Map<String, Object> payload = Map.of("Row", List.of(linha));
                String json = mapper.writeValueAsString(payload);

                String url = HBASE_REST_URL + "/" + TABELA_ALERTAS + "/" + rowKey;
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .timeout(Duration.ofSeconds(5))
                        .header("Content-Type", "application/json")
                        .PUT(HttpRequest.BodyPublishers.ofString(json))
                        .build();

                HttpResponse<String> response =
                        httpClient.send(request, HttpResponse.BodyHandlers.ofString());

                if (response.statusCode() >= 300) {
                    System.err.println("[alerta-hbase] resposta inesperada (" +
                            response.statusCode() + ") para produto=" + produtoId);
                }
            } catch (Exception ex) {
                // Não deixamos uma falha no HBase derrubar o job de streaming;
                // só registramos o erro e seguimos processando as próximas janelas.
                System.err.println("[alerta-hbase] falha ao gravar alerta para produto="
                        + produtoId + ": " + ex.getMessage());
            }
        }

        private static Map<String, String> celula(String coluna, String valor) {
            Map<String, String> c = new LinkedHashMap<>();
            c.put("column", b64("cf:" + coluna));
            c.put("$", b64(valor));
            return c;
        }

        private static String b64(String valor) {
            return Base64.getEncoder().encodeToString(valor.getBytes());
        }
    }
}