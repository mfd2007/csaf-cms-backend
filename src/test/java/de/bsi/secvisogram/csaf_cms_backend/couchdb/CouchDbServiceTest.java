package de.bsi.secvisogram.csaf_cms_backend.couchdb;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ibm.cloud.cloudant.v1.model.Document;
import de.bsi.secvisogram.csaf_cms_backend.json.AdvisoryJsonService;
import de.bsi.secvisogram.csaf_cms_backend.model.WorkflowState;
import de.bsi.secvisogram.csaf_cms_backend.rest.response.AdvisoryInformationResponse;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.xml.bind.DatatypeConverter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Test for the CouchDB service. Starts up a couchDB container via testcontainers (https://www.testcontainers.org/)
 */
@SpringBootTest
@Testcontainers
public class CouchDbServiceTest {

    private final String[] DOCUMENT_TITLE = {"csaf", "document", "title"};

    @Autowired
    private CouchDbService couchDbService;
    private final AdvisoryJsonService jsonService = new AdvisoryJsonService();

    static final String couchDbVersion = "3.2.2";
    static final String user = "testUser";
    static final String password = "testPassword";
    static final String dbName = "test-db";
    static final int initialPort = 5984;

    @Container
    static GenericContainer<?> couchDb = new GenericContainer<>("couchdb:" + couchDbVersion)
            .withEnv("COUCHDB_USER", user)
            .withEnv("COUCHDB_PASSWORD", password)
            .withCommand()
            .withExposedPorts(initialPort);

    @DynamicPropertySource
    static void registerCouchDbProperties(DynamicPropertyRegistry registry) {
        registry.add("csaf.couchdb.host", () -> couchDb.getHost());
        registry.add("csaf.couchdb.port", () -> couchDb.getMappedPort(initialPort));
        registry.add("csaf.couchdb.ssl", () -> false);
        registry.add("csaf.couchdb.user", () -> user);
        registry.add("csaf.couchdb.password", () -> password);
        registry.add("csaf.couchdb.dbname", () -> dbName);
    }

    @BeforeAll
    private static void createTestDB() throws IOException {
        // initializes a DB for testing purposes via PUT request to couchDB API
        URL url = new URL("http://" + user + ":" + password + "@" + couchDb.getHost() + ":" + couchDb.getMappedPort(initialPort) + "/" + dbName);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("PUT");
        String auth = user + ":" + password;
        String basicAuth = "Basic " + DatatypeConverter.printBase64Binary(auth.getBytes(StandardCharsets.UTF_8));
        connection.setRequestProperty("Authorization", basicAuth);
        connection.getResponseCode();
    }

    @Test
    public void getServerVersionTest() {

        Assertions.assertEquals(couchDbVersion, this.couchDbService.getServerVersion());
    }

    @Test
    public void writeNewCsafDocumentToDb() throws IOException {

        long countBefore = this.couchDbService.getDocumentCount();
        final String jsonFileName = "exxcellent-2021AB123.json";
        try (InputStream csafJsonStream = CouchDbServiceTest.class.getResourceAsStream(jsonFileName)) {

            final String owner = "Musterman";
            ObjectNode objectNode = jsonService.convertCsafToJson(csafJsonStream, owner, WorkflowState.Draft);
            final UUID uuid = UUID.randomUUID();
            final String revision = this.couchDbService.writeCsafDocument(uuid, objectNode);
            Assertions.assertNotNull(revision);
        }
        Assertions.assertEquals(countBefore + 1, this.couchDbService.getDocumentCount());
    }

    @Test
    @SuppressFBWarnings(value = "PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS", justification = "getDocumentCount is increased")
    public void updateCsafDocumentToDb() throws IOException {

        long countBefore = this.couchDbService.getDocumentCount();
        final UUID uuid = UUID.randomUUID();
        final String revision;
        try (InputStream csafJsonStream = CouchDbServiceTest.class.getResourceAsStream("exxcellent-2021AB123.json")) {

            final String owner = "Musterman";
            ObjectNode objectNode = jsonService.convertCsafToJson(csafJsonStream, owner, WorkflowState.Draft);
            revision = this.couchDbService.writeCsafDocument(uuid, objectNode);
            Assertions.assertNotNull(revision);
            Assertions.assertEquals(countBefore + 1, this.couchDbService.getDocumentCount());
        }

        try (InputStream csafChangeJsonStream = CouchDbServiceTest.class.getResourceAsStream("exxcellent-2022CC.json")) {
            final String owner = "Musterfrau";
            ObjectNode objectNode = jsonService.convertCsafToJson(csafChangeJsonStream, owner, WorkflowState.Draft);
            this.couchDbService.updateCsafDocument(uuid.toString(), revision, objectNode);
            Assertions.assertEquals(countBefore + 1, this.couchDbService.getDocumentCount());
        }
        final JsonNode response = this.couchDbService.readCsafDocument(uuid.toString());
        JsonNode changedTrackingId = response.at("/csaf/document/tracking/id");
        Assertions.assertEquals("exxcellent-2022CC", changedTrackingId.asText());

    }

    @Test
    @SuppressFBWarnings(value = "PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS", justification = "getDocumentCount is increased")
    public void deleteCsafDocumentFromDb() throws IOException, DatabaseException {

        long countBefore = this.couchDbService.getDocumentCount();
        final UUID uuid = UUID.randomUUID();
        final String revision;
        try (InputStream csafJsonStream = CouchDbServiceTest.class.getResourceAsStream("exxcellent-2021AB123.json")) {

            final String owner = "Musterman";
            ObjectNode objectNode = jsonService.convertCsafToJson(csafJsonStream, owner, WorkflowState.Draft);
            revision = this.couchDbService.writeCsafDocument(uuid, objectNode);
            Assertions.assertNotNull(revision);
            Assertions.assertEquals(countBefore + 1, this.couchDbService.getDocumentCount());

            this.couchDbService.deleteCsafDocument(uuid.toString(), revision);
        }
    }

    @Test
    @SuppressFBWarnings(value = "PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS", justification = "getDocumentCount is increased")
    public void deleteCsafDocumentFromDb_invalidRevision() throws IOException {

        long countBefore = this.couchDbService.getDocumentCount();
        final UUID uuid = UUID.randomUUID();
        final String revision;
        try (InputStream csafJsonStream = CouchDbServiceTest.class.getResourceAsStream("exxcellent-2021AB123.json")) {

            final String owner = "Musterman";
            ObjectNode objectNode = jsonService.convertCsafToJson(csafJsonStream, owner, WorkflowState.Draft);
            revision = this.couchDbService.writeCsafDocument(uuid, objectNode);
            Assertions.assertNotNull(revision);
            Assertions.assertEquals(countBefore + 1, this.couchDbService.getDocumentCount());

            Assertions.assertThrows(DatabaseException.class,
                    () -> this.couchDbService.deleteCsafDocument(uuid.toString(), "invalid revision"));
        }
    }

    @Test
    @SuppressFBWarnings(value = "PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS", justification = "getDocumentCount is increased")
    public void deleteCsafDocumentFromDb_invalidUuid() throws IOException {

        long countBefore = this.couchDbService.getDocumentCount();
        final UUID uuid = UUID.randomUUID();
        final String revision;
        try (InputStream csafJsonStream = CouchDbServiceTest.class.getResourceAsStream("exxcellent-2021AB123.json")) {

            final String owner = "Musterman";
            ObjectNode objectNode = jsonService.convertCsafToJson(csafJsonStream, owner, WorkflowState.Draft);
            revision = this.couchDbService.writeCsafDocument(uuid, objectNode);
            Assertions.assertNotNull(revision);
            Assertions.assertEquals(countBefore + 1, this.couchDbService.getDocumentCount());

            Assertions.assertThrows(DatabaseException.class,
                    () -> this.couchDbService.deleteCsafDocument("invalid user id", revision));
        }
    }

    @Test
    public void readAllCsafDocumentsFromDbTest() {

        final List<AdvisoryInformationResponse> revisions = this.couchDbService.readAllCsafDocuments();
        System.out.println(revisions.size());
        System.out.println(revisions.get(0).getTitle());
    }

    @Test
    public void readCsafDocumentTest() throws IOException {

        final List<AdvisoryInformationResponse> revisions = this.couchDbService.readAllCsafDocuments();

        final JsonNode response = this.couchDbService.readCsafDocument(revisions.get(0).getAdvisoryId());
        System.out.println(response);
    }


    @Test
    public void getStringFieldValueTest() {

        Document document = new Document.Builder().build();
        Assertions.assertNull(CouchDbService.getStringFieldValue(DOCUMENT_TITLE, document));
        document = new Document.Builder().add("csaf", null).build();
        Assertions.assertNull(CouchDbService.getStringFieldValue(DOCUMENT_TITLE, document));
        document = new Document.Builder().add("csaf", Map.of("document", Map.of("title", "TestTitle"))).build();
        Assertions.assertEquals(CouchDbService.getStringFieldValue(DOCUMENT_TITLE, document), "TestTitle");
    }
}
