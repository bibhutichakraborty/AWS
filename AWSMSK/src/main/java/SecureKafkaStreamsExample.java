import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class SecureKafkaStreamsExample {

    public static void main(final String[] args) {
        final String secureBootstrapServers = args.length > 0 ? args[0] : "b-1.msk101.uphz3y.c18.kafka.us-east-1.amazonaws.com:9092,b-3.msk101.uphz3y.c18.kafka.us-east-1.amazonaws.com:9092,b-2.msk101.uphz3y.c18.kafka.us-east-1.amazonaws.com:9092";
        final Properties streamsConfiguration = new Properties();
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "secure-kafka-streams-app");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "secure-kafka-streams-app-client");
        // Where to find secure (!) Kafka broker(s).  In the VM, the broker listens on port 9093 for
        // SSL connections.
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, secureBootstrapServers);
        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
        // Security settings.
        // 1. These settings must match the security settings of the secure Kafka cluster.
        // 2. The SSL trust store and key store files must be locally accessible to the application.
        //    Typically, this means they would be installed locally in the client machine (or container)
        //    on which the application runs.  To simplify running this example, however, these files
        //    were generated and stored in the VM in which the secure Kafka broker is running.  This


        final StreamsBuilder builder = new StreamsBuilder();
        // Write the input data as-is to the output topic.
        builder.stream("msk.dbo.Persons").to("OutputLog");
        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        // Always (and unconditionally) clean local state prior to starting the processing topology.
        // We opt for this unconditional call here because this will make it easier for you to play around with the example
        // when resetting the application for doing a re-run (via the Application Reset Tool,
        // https://docs.confluent.io/platform/current/streams/developer-guide/app-reset-tool.html).
        //
        // The drawback of cleaning up local state prior is that your app must rebuilt its local state from scratch, which
        // will take time and will require reading all the state-relevant data from the Kafka cluster over the network.
        // Thus in a production scenario you typically do not want to clean up always as we do here but rather only when it
        // is truly needed, i.e., only under certain conditions (e.g., the presence of a command line flag for your app).
        // See `ApplicationResetExample.java` for a production-like example.
        streams.cleanUp();
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                streams.close();
            }
        }));
    }

}