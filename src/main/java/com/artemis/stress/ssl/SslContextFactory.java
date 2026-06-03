package com.artemis.stress.ssl;

import com.artemis.stress.config.StressConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyStore;

/**
 * Builds an {@link SSLContext} from keystore / truststore configuration for
 * mutual TLS (mTLS) authentication with Apache Artemis.
 *
 * <p>Supports both JKS and PKCS12 store formats.
 * The resulting context is used to configure system SSL properties so that
 * the Artemis JMS client picks them up automatically.</p>
 */
public class SslContextFactory {

    private static final Logger log = LoggerFactory.getLogger(SslContextFactory.class);

    /**
     * Configures JVM-level SSL system properties.
     * The Artemis Core/JMS client reads these automatically when creating
     * SSL connections.
     *
     * @param config the stress tool configuration
     */
    public static void configureSystemSslProperties(StressConfig config) {
        if (config.getKeystorePath() == null) {
            log.debug("No keystore configured — skipping SSL property setup");
            return;
        }

        log.info("Configuring SSL: keystore={} type={} tls={}",
                config.getKeystorePath(), config.getKeystoreType(), config.getTlsVersion());

        System.setProperty("javax.net.ssl.keyStore", config.getKeystorePath());
        System.setProperty("javax.net.ssl.keyStorePassword", config.getKeystorePassword());
        System.setProperty("javax.net.ssl.keyStoreType", config.getKeystoreType());

        if (config.getTruststorePath() != null) {
            System.setProperty("javax.net.ssl.trustStore", config.getTruststorePath());
            System.setProperty("javax.net.ssl.trustStorePassword", config.getTruststorePassword());
            System.setProperty("javax.net.ssl.trustStoreType", config.getKeystoreType());
        }

        // Restrict to the configured TLS version
        System.setProperty("jdk.tls.client.protocols", config.getTlsVersion());

        log.info("SSL system properties configured successfully");
    }

    /**
     * Builds an {@link SSLContext} programmatically — useful for unit tests or
     * when injecting the context directly rather than relying on system props.
     *
     * @param config the stress tool configuration
     * @return a fully initialised SSLContext for mTLS
     */
    public static SSLContext buildSslContext(StressConfig config) {
        try {
            // ── Load KeyStore (client certificate + private key) ──────────────
            KeyStore keyStore = loadKeyStore(
                    config.getKeystorePath(),
                    config.getKeystorePassword(),
                    config.getKeystoreType());

            KeyManagerFactory kmf = KeyManagerFactory.getInstance(
                    KeyManagerFactory.getDefaultAlgorithm());
            char[] keyPass = config.getKeyPassword() != null
                    ? config.getKeyPassword().toCharArray()
                    : config.getKeystorePassword().toCharArray();
            kmf.init(keyStore, keyPass);

            // ── Load TrustStore (broker certificate / CA chain) ───────────────
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(
                    TrustManagerFactory.getDefaultAlgorithm());
            if (config.getTruststorePath() != null) {
                KeyStore trustStore = loadKeyStore(
                        config.getTruststorePath(),
                        config.getTruststorePassword(),
                        config.getKeystoreType());
                tmf.init(trustStore);
            } else {
                tmf.init((KeyStore) null); // Use JVM default trust store
            }

            // ── Assemble SSLContext ───────────────────────────────────────────
            SSLContext ctx = SSLContext.getInstance(config.getTlsVersion());
            ctx.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);

            log.info("SSLContext built successfully (protocol={})", config.getTlsVersion());
            return ctx;

        } catch (Exception e) {
            throw new SslConfigurationException(
                    "Failed to build SSLContext from keystore=" + config.getKeystorePath(), e);
        }
    }

    /**
     * Loads a KeyStore from disk.
     */
    private static KeyStore loadKeyStore(String path, String password, String type)
            throws Exception {
        KeyStore ks = KeyStore.getInstance(type);
        try (FileInputStream fis = new FileInputStream(path)) {
            ks.load(fis, password.toCharArray());
        } catch (IOException e) {
            throw new SslConfigurationException(
                    "Cannot read keystore/truststore at path: " + path, e);
        }
        log.debug("Loaded KeyStore type={} path={} aliases={}",
                type, path, countAliases(ks));
        return ks;
    }

    private static int countAliases(KeyStore ks) {
        try {
            return ks.size();
        } catch (Exception ignored) {
            return -1;
        }
    }

    // ─── Custom exception ────────────────────────────────────────────────────

    public static class SslConfigurationException extends RuntimeException {
        public SslConfigurationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
