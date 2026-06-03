#!/usr/bin/env bash
# =============================================================================
# generate-certs.sh
# Generates a self-signed CA, broker certificate, and client certificate
# for testing mTLS authentication with Apache Artemis.
#
# Usage:
#   chmod +x generate-certs.sh
#   ./generate-certs.sh
#
# Output:
#   certs/
#   ├── ca.crt              – CA certificate (PEM)
#   ├── broker-keystore.jks – Broker keystore (import into Artemis)
#   ├── client-keystore.jks – Client keystore (pass to stress tool)
#   └── truststore.jks      – Shared truststore (both sides)
# =============================================================================
set -euo pipefail

OUTDIR="certs"
PASS="changeit"
VALIDITY=3650   # 10 years
CA_ALIAS="test-ca"
BROKER_ALIAS="artemis-broker"
CLIENT_ALIAS="stress-tool-client"

mkdir -p "$OUTDIR"
cd "$OUTDIR"

echo "============================================================"
echo "  Generating test certificates for Artemis mTLS"
echo "============================================================"

# ── 1. CA Key + Self-Signed Certificate ──────────────────────────────────────
echo "[1/6] Generating CA key pair..."
openssl genrsa -out ca.key 4096
openssl req -new -x509 -days $VALIDITY -key ca.key -out ca.crt \
    -subj "/CN=Stress-Tool-Test-CA/O=Stress/C=FR"

# ── 2. Broker Certificate ─────────────────────────────────────────────────────
echo "[2/6] Generating broker certificate..."
openssl genrsa -out broker.key 2048
openssl req -new -key broker.key -out broker.csr \
    -subj "/CN=localhost/O=Artemis/C=FR"
openssl x509 -req -days $VALIDITY -in broker.csr -CA ca.crt -CAkey ca.key \
    -CAcreateserial -out broker.crt \
    -extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1")
echo "   Broker certificate signed by CA"

# ── 3. Client Certificate ─────────────────────────────────────────────────────
echo "[3/6] Generating client certificate..."
openssl genrsa -out client.key 2048
openssl req -new -key client.key -out client.csr \
    -subj "/CN=stress-tool/O=StressTool/C=FR"
openssl x509 -req -days $VALIDITY -in client.csr -CA ca.crt -CAkey ca.key \
    -CAcreateserial -out client.crt
echo "   Client certificate signed by CA"

# ── 4. Import broker cert into broker keystore ────────────────────────────────
echo "[4/6] Building broker-keystore.jks..."
openssl pkcs12 -export -in broker.crt -inkey broker.key -chain -CAfile ca.crt \
    -name "$BROKER_ALIAS" -out broker.p12 -passout pass:"$PASS"
keytool -importkeystore \
    -srckeystore broker.p12 -srcstoretype PKCS12 -srcstorepass "$PASS" \
    -destkeystore broker-keystore.jks -deststoretype JKS -deststorepass "$PASS" \
    -noprompt
echo "   broker-keystore.jks created"

# ── 5. Import client cert into client keystore ────────────────────────────────
echo "[5/6] Building client-keystore.jks..."
openssl pkcs12 -export -in client.crt -inkey client.key -chain -CAfile ca.crt \
    -name "$CLIENT_ALIAS" -out client.p12 -passout pass:"$PASS"
keytool -importkeystore \
    -srckeystore client.p12 -srcstoretype PKCS12 -srcstorepass "$PASS" \
    -destkeystore client-keystore.jks -deststoretype JKS -deststorepass "$PASS" \
    -noprompt
echo "   client-keystore.jks created"

# ── 6. Build shared truststore with CA cert ───────────────────────────────────
echo "[6/6] Building truststore.jks..."
keytool -import -alias "$CA_ALIAS" -file ca.crt \
    -keystore truststore.jks -storetype JKS -storepass "$PASS" -noprompt
echo "   truststore.jks created"

# ── Cleanup intermediary files ────────────────────────────────────────────────
rm -f *.p12 *.csr *.srl

echo ""
echo "============================================================"
echo "  Done! Files in ./$OUTDIR:"
ls -lh "$OUTDIR"/ 2>/dev/null || ls -lh .
echo ""
echo "  Artemis broker.xml SSL acceptor config:"
echo "  -----------------------------------------"
cat <<'EOF'
  <acceptor name="ssl">
    tcp://0.0.0.0:61617?sslEnabled=true;
    keyStorePath=/path/to/certs/broker-keystore.jks;
    keyStorePassword=changeit;
    trustStorePath=/path/to/certs/truststore.jks;
    trustStorePassword=changeit;
    needClientAuth=true;
    enabledProtocols=TLSv1.3
  </acceptor>
EOF
echo ""
echo "  Stress tool usage:"
echo "  -----------------------------------------"
echo "  java -jar artemis-stress-tool.jar \\"
echo "    --broker-url ssl://localhost:61617 \\"
echo "    --queue STRESS.TEST \\"
echo "    --threads 10 \\"
echo "    --messages 50000 \\"
echo "    --keystore ./certs/client-keystore.jks \\"
echo "    --keystore-password changeit \\"
echo "    --truststore ./certs/truststore.jks \\"
echo "    --truststore-password changeit"
echo "============================================================"
