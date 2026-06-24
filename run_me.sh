  java -jar target/artemis-stress-tool-1.0.0.jar \
    --broker-url ssl://localhost:61617 \
    --queue STRESS.TEST \
    --threads 10 \
    --messages 50000 \
    --keystore ./scripts/certs/client-keystore.jks \
    --keystore-password changeit \
    --truststore ./certs/truststore.jks \
    --truststore-password changeit


      java -jar target/artemis-stress-tool-1.0.0.jar \
        --queue STRESS.TEST \
        --threads 10 \
        --messages 50000 \
        --keystore ./scripts/certs-d/client-p12 \
        --keystore-password clientpass \
        --truststore ./certs-d/truststore.p12 \
        --truststore-password clientpass \
        --keystore-type PKCS12