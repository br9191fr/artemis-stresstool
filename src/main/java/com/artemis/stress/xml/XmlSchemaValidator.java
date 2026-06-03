package com.artemis.stress.xml;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.LongAdder;

/**
 * Thread-safe XML schema validator.
 *
 * <p>A {@link Schema} object is expensive to create (parses the XSD) but is
 * fully thread-safe once built — it can be shared across all producer threads.
 * A lightweight {@link Validator} is created per-validation call (cheap) to
 * avoid thread-safety issues with {@code javax.xml.validation.Validator}.</p>
 *
 * <p>Two modes:
 * <ul>
 *   <li><b>Bundled schema</b> — loads {@code stress-message.xsd} from the
 *       classpath when no external path is provided.</li>
 *   <li><b>External schema</b> — loads a user-supplied XSD from the
 *       filesystem when {@code schemaPath} is set in the config.</li>
 * </ul>
 * </p>
 *
 * <p>When {@link #validate(String)} is called, any schema violation throws a
 * {@link XmlValidationException} containing the full error message.
 * Counters track how many validations passed / failed for reporting.</p>
 */
public class XmlSchemaValidator {

    private static final Logger log = LoggerFactory.getLogger(XmlSchemaValidator.class);

    /** Classpath location of the bundled XSD. */
    private static final String BUNDLED_XSD = "/stress-message.xsd";

    private final Schema schema;
    private final String schemaSource;

    // Metrics
    private final LongAdder passCount  = new LongAdder();
    private final LongAdder failCount  = new LongAdder();

    /**
     * Builds a validator using the bundled {@code stress-message.xsd}.
     */
    public XmlSchemaValidator() {
        this(null);
    }

    /**
     * Builds a validator from an external XSD file, or falls back to the
     * bundled schema when {@code externalSchemaPath} is {@code null}.
     *
     * @param externalSchemaPath filesystem path to a custom XSD, or {@code null}
     */
    public XmlSchemaValidator(String externalSchemaPath) {
        SchemaFactory sf = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);

        // Harden against XXE in the schema itself
        try {
            sf.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
            sf.setProperty(XMLConstants.ACCESS_EXTERNAL_DTD, "");
            sf.setProperty(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
        } catch (SAXException e) {
            log.warn("Could not set all security features on SchemaFactory: {}", e.getMessage());
        }

        try {
            if (externalSchemaPath != null) {
                schema = sf.newSchema(new File(externalSchemaPath));
                schemaSource = externalSchemaPath;
            } else {
                URL url = XmlSchemaValidator.class.getResource(BUNDLED_XSD);
                if (url == null) {
                    throw new IllegalStateException(
                            "Bundled XSD not found on classpath: " + BUNDLED_XSD);
                }
                schema = sf.newSchema(url);
                schemaSource = "classpath:" + BUNDLED_XSD;
            }
        } catch (SAXException e) {
            throw new IllegalArgumentException(
                    "Failed to parse XML schema from " +
                    (externalSchemaPath != null ? externalSchemaPath : "classpath"), e);
        }

        log.info("XML schema validator ready — source={}", schemaSource);
    }

    /**
     * Validates the given XML string against the loaded schema.
     *
     * <p>This method is <em>thread-safe</em>: a new {@link Validator} instance
     * is created on every call (as recommended by the JDK docs).</p>
     *
     * @param xml the XML document as a string
     * @throws XmlValidationException if the document does not conform to the schema
     */
    public void validate(String xml) {
        Validator validator = schema.newValidator();

        // Harden against XXE in the instance document
        try {
            validator.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
            validator.setProperty(XMLConstants.ACCESS_EXTERNAL_DTD, "");
            validator.setProperty(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
        } catch (SAXException e) {
            log.debug("Could not set all security features on Validator: {}", e.getMessage());
        }

        try {
            validator.validate(new StreamSource(
                    new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8))));
            passCount.increment();
        } catch (SAXException e) {
            failCount.increment();
            throw new XmlValidationException(
                    "Schema validation failed [" + schemaSource + "]: " + e.getMessage(), e);
        } catch (IOException e) {
            // ByteArrayInputStream never throws IOException in practice
            failCount.increment();
            throw new XmlValidationException("Unexpected I/O error during validation", e);
        }
    }

    // ─── Counters ─────────────────────────────────────────────────────────────

    public long getPassCount() { return passCount.sum(); }
    public long getFailCount() { return failCount.sum(); }
    public String getSchemaSource() { return schemaSource; }

    // ─── Custom exception ─────────────────────────────────────────────────────

    public static class XmlValidationException extends RuntimeException {
        public XmlValidationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
