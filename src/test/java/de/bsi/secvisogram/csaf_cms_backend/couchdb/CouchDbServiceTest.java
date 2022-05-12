package de.bsi.secvisogram.csaf_cms_backend.couchdb;

import static de.bsi.secvisogram.csaf_cms_backend.couchdb.CouchDBFilterCreator.expr2CouchDBFilter;
import static de.bsi.secvisogram.csaf_cms_backend.fixture.TestModelRoot.ROOT_PRIMITIVE_FIELDS;
import static de.bsi.secvisogram.csaf_cms_backend.model.filter.OperatorExpression.*;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.gson.internal.LazilyParsedNumber;
import com.ibm.cloud.cloudant.v1.model.Document;
import de.bsi.secvisogram.csaf_cms_backend.fixture.TestModelRoot;
import de.bsi.secvisogram.csaf_cms_backend.json.AdvisoryJsonService;
import de.bsi.secvisogram.csaf_cms_backend.model.WorkflowState;
import de.bsi.secvisogram.csaf_cms_backend.model.filter.OperatorExpression;
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
        registry.add("csaf.couchdb.ssl", () -> Boolean.FALSE);
        registry.add("csaf.couchdb.user", () -> user);
        registry.add("csaf.couchdb.password", () -> password);
        registry.add("csaf.couchdb.dbname", () -> dbName);
    }

    @BeforeAll
    @SuppressFBWarnings(value = "URLCONNECTION_SSRF_FD", justification = "URL is built from dynamic values but not user input")
    private static void createTestDB() throws IOException {
        // initializes a DB for testing purposes via PUT request to couchDB API
        URL url = new URL("http://"
                + couchDb.getHost() + ":"
                + couchDb.getMappedPort(initialPort)
                + "/" + dbName);
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
    @SuppressFBWarnings(value = "PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS", justification = "document count should increase")
    public void writeNewCsafDocumentToDb() throws IOException {

        long countBefore = this.couchDbService.getDocumentCount();

        UUID uuid = UUID.randomUUID();
        String revision = insertTestDocument(uuid);

        Assertions.assertNotNull(revision);

        Assertions.assertEquals(countBefore + 1, this.couchDbService.getDocumentCount());
    }

    @Test
    public void updateCsafDocumentToDb() throws IOException {


        final UUID uuid = UUID.randomUUID();
        String revision = insertTestDocument(uuid);

        long countBeforeUpdate = this.couchDbService.getDocumentCount();
        final JsonNode responseBeforeUpdate = this.couchDbService.readCsafDocument(uuid.toString());
        JsonNode trackingIdBeforeUpdate = responseBeforeUpdate.at("/csaf/document/tracking/id");
        Assertions.assertEquals("exxcellent-2021AB123", trackingIdBeforeUpdate.asText());

        String newOwner = "Musterfrau";
        try (InputStream csafJsonStream = CouchDbServiceTest.class.getResourceAsStream("exxcellent-2022CC.json")) {
            ObjectNode objectNode = jsonService.convertCsafToJson(csafJsonStream, newOwner, WorkflowState.Draft);
            this.couchDbService.updateCsafDocument(uuid.toString(), revision, objectNode);
        }

        long countAfterUpdate = this.couchDbService.getDocumentCount();
        final JsonNode responseAfterUpdate = this.couchDbService.readCsafDocument(uuid.toString());
        Assertions.assertEquals(countBeforeUpdate, countAfterUpdate);

        JsonNode trackingIdAfterUpdate = responseAfterUpdate.at("/csaf/document/tracking/id");
        Assertions.assertEquals("exxcellent-2022CC", trackingIdAfterUpdate.asText());

    }

    @Test
    @SuppressFBWarnings(value = "PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS", justification = "document should not change")
    public void deleteCsafDocumentFromDb() throws IOException, DatabaseException {

        long countBefore = this.couchDbService.getDocumentCount();

        final UUID uuid = UUID.randomUUID();
        String revision = insertTestDocument(uuid);

        Assertions.assertEquals(countBefore + 1, this.couchDbService.getDocumentCount());
        this.couchDbService.deleteCsafDocument(uuid.toString(), revision);

        Assertions.assertEquals(countBefore, this.couchDbService.getDocumentCount());
    }

    @Test
    public void deleteCsafDocumentFromDb_invalidRevision() throws IOException {

        final UUID uuid = UUID.randomUUID();
        insertTestDocument(uuid);

        Assertions.assertThrows(DatabaseException.class,
                () -> this.couchDbService.deleteCsafDocument(uuid.toString(), "invalid revision"));
    }

    @Test
    public void deleteCsafDocumentFromDb_invalidUuid() throws IOException {

        final UUID uuid = UUID.randomUUID();
        final String revision = insertTestDocument(uuid);

        Assertions.assertThrows(DatabaseException.class,
                () -> this.couchDbService.deleteCsafDocument("invalid user id", revision));
    }

    @Test
    public void readAllCsafDocumentsFromDbTest() {

        long docCount = this.couchDbService.getDocumentCount();

        final List<AdvisoryInformationResponse> infos = this.couchDbService.readAllCsafDocuments();
        Assertions.assertEquals(docCount, infos.size());
    }

    @Test
    public void readCsafDocumentTest() throws IOException {

        final UUID uuid = UUID.randomUUID();
        insertTestDocument(uuid);

        final JsonNode response = this.couchDbService.readCsafDocument(uuid.toString());
        JsonNode readTitle = response.at("/csaf/document/title");
        Assertions.assertEquals("TestRSc", readTitle.asText());
    }


    private String insertTestDocument(UUID documentUuid) throws IOException {
        String owner = "Mustermann";
        String jsonFileName = "exxcellent-2021AB123.json";
        try (InputStream csafJsonStream = CouchDbServiceTest.class.getResourceAsStream(jsonFileName)) {
            ObjectNode objectNode = jsonService.convertCsafToJson(csafJsonStream, owner, WorkflowState.Draft);
            return this.couchDbService.writeCsafDocument(documentUuid, objectNode);
        }
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

    /**
     * Test find with Native Cloudant selecto
     * @throws IOException unexpected exception
     */
    @Test
    public void findDocumentsTest_native() throws IOException {

        this.couchDbService.deleteDatabase(this.couchDbService.getDbName());
        this.couchDbService.createDatabase(this.couchDbService.getDbName());

        final TestModelRoot node1 = new TestModelRoot().setFirstString("Hans").setSecondString("Dampf").setDecimalValue(12.55);
        this.couchDbService.writeDocument(UUID.randomUUID(), node1);

        final TestModelRoot node2 = new TestModelRoot().setFirstString("Franz").setSecondString("Dampf");
        this.couchDbService.writeDocument(UUID.randomUUID(), node2);

        List<Document> foundDocs = this.couchDbService.findDocuments(Map.of("secondString", "Dampf"),
                ROOT_PRIMITIVE_FIELDS);

        assertThat(foundDocs.size(), equalTo(2));

        foundDocs = this.couchDbService.findDocuments(Map.of("firstString", "Hans"),
                ROOT_PRIMITIVE_FIELDS);
        assertThat(foundDocs.size(), equalTo(1));
    }

    @Test
    public void findDocumentsTest_operatorEqual() throws IOException {

        this.couchDbService.deleteDatabase(this.couchDbService.getDbName());
        this.couchDbService.createDatabase(this.couchDbService.getDbName());

        final TestModelRoot node1 = new TestModelRoot().setFirstString("zzz").setSecondString("AAA").setDecimalValue(12.55);
        this.couchDbService.writeDocument(UUID.randomUUID(), node1);

        final TestModelRoot node2 = new TestModelRoot().setFirstString("yyy").setSecondString("AAA");
        this.couchDbService.writeDocument(UUID.randomUUID(), node2);

        final TestModelRoot node3 = new TestModelRoot().setFirstString("xxx").setSecondString("BBB");
        this.couchDbService.writeDocument(UUID.randomUUID(), node3);

        Map<String, Object> filter = expr2CouchDBFilter(equal("AAA","secondString"));
        List<Document> foundDocs = this.couchDbService.findDocuments(filter, ROOT_PRIMITIVE_FIELDS);
        assertThat(mapAttribute(foundDocs, "firstString"), containsInAnyOrder("yyy","zzz"));

        Map<String, Object> filterNe = expr2CouchDBFilter(notEqual("yyy","firstString"));
        foundDocs = this.couchDbService.findDocuments(filterNe, ROOT_PRIMITIVE_FIELDS);
        assertThat(mapAttribute(foundDocs, "firstString"), containsInAnyOrder("xxx","zzz"));
    }

    @Test
    public void findDocumentsTest_operatorsGreaterAndLess() throws IOException {

        this.couchDbService.deleteDatabase(this.couchDbService.getDbName());
        this.couchDbService.createDatabase(this.couchDbService.getDbName());

        this.couchDbService.writeDocument(UUID.randomUUID(), new TestModelRoot().setFirstString("AAA"));
        this.couchDbService.writeDocument(UUID.randomUUID(), new TestModelRoot().setFirstString("BBB"));
        this.couchDbService.writeDocument(UUID.randomUUID(), new TestModelRoot().setFirstString("CCC"));

        OperatorExpression gteExpr = greaterOrEqual("BBB","firstString");
        List<Document> foundDocs = this.couchDbService.findDocuments(expr2CouchDBFilter(gteExpr), ROOT_PRIMITIVE_FIELDS);
        assertThat(mapAttribute(foundDocs, "firstString"), containsInAnyOrder("BBB", "CCC"));

        OperatorExpression gtExpr = greater("BBB","firstString");
        foundDocs = this.couchDbService.findDocuments(expr2CouchDBFilter(gtExpr), ROOT_PRIMITIVE_FIELDS);
        assertThat(mapAttribute(foundDocs, "firstString"), containsInAnyOrder("CCC"));

        OperatorExpression lteExpr = lessOrEqual("BBB","firstString");
        foundDocs = this.couchDbService.findDocuments(expr2CouchDBFilter(lteExpr), ROOT_PRIMITIVE_FIELDS);
        assertThat(mapAttribute(foundDocs, "firstString"), containsInAnyOrder("AAA", "BBB"));

        OperatorExpression ltExpr = less("BBB","firstString");
        foundDocs = this.couchDbService.findDocuments(expr2CouchDBFilter(ltExpr), ROOT_PRIMITIVE_FIELDS);
        assertThat(mapAttribute(foundDocs, "firstString"), containsInAnyOrder("AAA"));
    }

     @Test
    public void findDocumentsTest_numericValue() throws IOException {

        this.couchDbService.deleteDatabase(this.couchDbService.getDbName());
        this.couchDbService.createDatabase(this.couchDbService.getDbName());

        final TestModelRoot node1 = new TestModelRoot().setDecimalValue(12.55);
        this.couchDbService.writeDocument(UUID.randomUUID(), node1);

        final TestModelRoot node2 = new TestModelRoot().setDecimalValue(2374.332);
        this.couchDbService.writeDocument(UUID.randomUUID(), node2);

        Map<String, Object> filter = expr2CouchDBFilter(equal(12.55,"decimalValue"));
        List<Document> foundDocs = this.couchDbService.findDocuments(filter, ROOT_PRIMITIVE_FIELDS);
        assertThat(mapAttributeDouble(foundDocs, "decimalValue"), containsInAnyOrder(12.55));
    }

    @Test
    public void findDocumentsTest_booleanValue() throws IOException {

        this.couchDbService.deleteDatabase(this.couchDbService.getDbName());
        this.couchDbService.createDatabase(this.couchDbService.getDbName());

        final TestModelRoot node1 = new TestModelRoot().setBooleanValue(true);
        this.couchDbService.writeDocument(UUID.randomUUID(), node1);

        final TestModelRoot node2 = new TestModelRoot().setBooleanValue(false);
        this.couchDbService.writeDocument(UUID.randomUUID(), node2);

        Map<String, Object> filter = expr2CouchDBFilter(equal(true,"booleanValue"));
        List<Document> foundDocs = this.couchDbService.findDocuments(filter, ROOT_PRIMITIVE_FIELDS);
        assertThat(mapAttribute(foundDocs, "booleanValue"), containsInAnyOrder(true));
    }

    private List<Object> mapAttribute(List<Document> foundDocs, String attributeName) {
        return foundDocs.stream()
                .map(doc -> doc.get(attributeName))
                .collect(toList());
    }

    private List<Object> mapAttributeDouble(List<Document> foundDocs, String attributeName) {
        return foundDocs.stream()
                .map(doc -> ((LazilyParsedNumber)doc.get(attributeName)).doubleValue())
                .collect(toList());
    }

}
