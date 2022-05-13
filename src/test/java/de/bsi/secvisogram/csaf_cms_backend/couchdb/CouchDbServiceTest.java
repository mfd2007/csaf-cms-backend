package de.bsi.secvisogram.csaf_cms_backend.couchdb;

import static de.bsi.secvisogram.csaf_cms_backend.couchdb.CouchDBFilterCreator.expr2CouchDBFilter;
import static de.bsi.secvisogram.csaf_cms_backend.fixture.TestModelArray.ENTRY_VALUE;
import static de.bsi.secvisogram.csaf_cms_backend.fixture.TestModelFirstLevel.SECOND_LEVEL;
import static de.bsi.secvisogram.csaf_cms_backend.fixture.TestModelRoot.*;
import static de.bsi.secvisogram.csaf_cms_backend.fixture.TestModelSecondLevel.LEVEL_2_VALUE;
import static de.bsi.secvisogram.csaf_cms_backend.model.filter.OperatorExpression.*;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.gson.internal.LazilyParsedNumber;
import com.ibm.cloud.cloudant.v1.model.Document;
import de.bsi.secvisogram.csaf_cms_backend.fixture.TestModelRoot;
import de.bsi.secvisogram.csaf_cms_backend.json.AdvisoryJsonService;
import de.bsi.secvisogram.csaf_cms_backend.model.WorkflowState;
import de.bsi.secvisogram.csaf_cms_backend.model.filter.AndExpression;
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
 * Test for the CouchDB service. Starts up a couchDB container via
 * (<a href="https://www.testcontainers.org/">testcontainers/</a>)
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

        this.writeToDb(new TestModelRoot().setFirstString("Hans").setSecondString("Dampf").setDecimalValue(12.55));
        this.writeToDb(new TestModelRoot().setFirstString("Franz").setSecondString("Dampf"));

        List<Document> foundDocs = this.couchDbService.findDocuments(Map.of(SECOND_STRING, "Dampf"),
                ROOT_PRIMITIVE_FIELDS);

        assertThat(foundDocs.size(), equalTo(2));

        foundDocs = this.couchDbService.findDocuments(Map.of(FIRST_STRING, "Hans"),
                ROOT_PRIMITIVE_FIELDS);
        assertThat(foundDocs.size(), equalTo(1));
    }

    @Test
    public void findDocumentsTest_operatorEqual() throws IOException {

        this.couchDbService.deleteDatabase(this.couchDbService.getDbName());
        this.couchDbService.createDatabase(this.couchDbService.getDbName());

        this.writeToDb(new TestModelRoot().setFirstString("zzz").setSecondString("AAA").setDecimalValue(12.55));
        this.writeToDb(new TestModelRoot().setFirstString("yyy").setSecondString("AAA"));
        this.writeToDb(new TestModelRoot().setFirstString("xxx").setSecondString("BBB"));

        Map<String, Object> filter = expr2CouchDBFilter(equal("AAA", SECOND_STRING));
        List<Document> foundDocs = this.couchDbService.findDocuments(filter, ROOT_PRIMITIVE_FIELDS);
        assertThat(mapAttribute(foundDocs, FIRST_STRING), containsInAnyOrder("yyy", "zzz"));

        Map<String, Object> filterNe = expr2CouchDBFilter(notEqual("yyy", FIRST_STRING));
        foundDocs = this.couchDbService.findDocuments(filterNe, ROOT_PRIMITIVE_FIELDS);
        assertThat(mapAttribute(foundDocs, FIRST_STRING), containsInAnyOrder("xxx", "zzz"));
    }

    @Test
    public void findDocumentsTest_operatorContainsIgnoreCase() throws IOException {

        this.couchDbService.deleteDatabase(this.couchDbService.getDbName());
        this.couchDbService.createDatabase(this.couchDbService.getDbName());

        this.writeToDb(new TestModelRoot().setFirstString("123Abc45"));
        this.writeToDb(new TestModelRoot().setFirstString("123abC45"));
        this.writeToDb(new TestModelRoot().setFirstString("123abD45"));

        Map<String, Object> filter = expr2CouchDBFilter(containsIgnoreCase("3abc4", FIRST_STRING));
        List<Document> foundDocs = this.couchDbService.findDocuments(filter, ROOT_PRIMITIVE_FIELDS);
        assertThat(mapAttribute(foundDocs, FIRST_STRING), containsInAnyOrder("123Abc45", "123abC45"));
    }

    @Test
    public void findDocumentsTest_operatorsGreaterAndLess() throws IOException {

        this.couchDbService.deleteDatabase(this.couchDbService.getDbName());
        this.couchDbService.createDatabase(this.couchDbService.getDbName());

        this.writeToDb(new TestModelRoot().setFirstString("AAA"));
        this.writeToDb(new TestModelRoot().setFirstString("BBB"));
        this.writeToDb(new TestModelRoot().setFirstString("CCC"));

        OperatorExpression gteExpr = greaterOrEqual("BBB", FIRST_STRING);
        List<Document> foundDocs = this.couchDbService.findDocuments(expr2CouchDBFilter(gteExpr), ROOT_PRIMITIVE_FIELDS);
        assertThat(mapAttribute(foundDocs, FIRST_STRING), containsInAnyOrder("BBB", "CCC"));

        OperatorExpression gtExpr = greater("BBB", FIRST_STRING);
        foundDocs = this.couchDbService.findDocuments(expr2CouchDBFilter(gtExpr), ROOT_PRIMITIVE_FIELDS);
        assertThat(mapAttribute(foundDocs, FIRST_STRING), containsInAnyOrder("CCC"));

        OperatorExpression lteExpr = lessOrEqual("BBB", FIRST_STRING);
        foundDocs = this.couchDbService.findDocuments(expr2CouchDBFilter(lteExpr), ROOT_PRIMITIVE_FIELDS);
        assertThat(mapAttribute(foundDocs, FIRST_STRING), containsInAnyOrder("AAA", "BBB"));

        OperatorExpression ltExpr = less("BBB", FIRST_STRING);
        foundDocs = this.couchDbService.findDocuments(expr2CouchDBFilter(ltExpr), ROOT_PRIMITIVE_FIELDS);
        assertThat(mapAttribute(foundDocs, FIRST_STRING), containsInAnyOrder("AAA"));
    }

     @Test
    public void findDocumentsTest_numericValue() throws IOException {

        this.couchDbService.deleteDatabase(this.couchDbService.getDbName());
        this.couchDbService.createDatabase(this.couchDbService.getDbName());

        this.writeToDb(new TestModelRoot().setDecimalValue(12.55));
        this.writeToDb(new TestModelRoot().setDecimalValue(2374.332));

        Map<String, Object> filter = expr2CouchDBFilter(equal(12.55, DECIMAL_VALUE));
        List<Document> foundDocs = this.couchDbService.findDocuments(filter, ROOT_PRIMITIVE_FIELDS);
        assertThat(mapAttributeDouble(foundDocs, DECIMAL_VALUE), containsInAnyOrder(12.55));
    }

    @Test
    public void findDocumentsTest_booleanValue() throws IOException {

        this.couchDbService.deleteDatabase(this.couchDbService.getDbName());
        this.couchDbService.createDatabase(this.couchDbService.getDbName());

        this.writeToDb(new TestModelRoot().setBooleanValue(true));
        this.writeToDb(new TestModelRoot().setBooleanValue(false));

        Map<String, Object> filter = expr2CouchDBFilter(equal(true, BOOLEAN_VALUE));
        List<Document> foundDocs = this.couchDbService.findDocuments(filter, ROOT_PRIMITIVE_FIELDS);
        assertThat(mapAttribute(foundDocs, BOOLEAN_VALUE), containsInAnyOrder(true));
    }

    @Test
    public void findDocumentsTest_operatorAnd() throws IOException {

        this.couchDbService.deleteDatabase(this.couchDbService.getDbName());
        this.couchDbService.createDatabase(this.couchDbService.getDbName());

        this.writeToDb(new TestModelRoot().setFirstString("zzz").setSecondString("AAA").setDecimalValue(11.11));
        this.writeToDb(new TestModelRoot().setFirstString("zzz").setSecondString("AAA").setDecimalValue(22.22));
        this.writeToDb(new TestModelRoot().setFirstString("zzz").setSecondString("BBB").setDecimalValue(11.11));
        this.writeToDb(new TestModelRoot().setFirstString("zzz").setSecondString("BBB").setDecimalValue(22.22));
        this.writeToDb(new TestModelRoot().setFirstString("xxx").setSecondString("AAA").setDecimalValue(11.11));
        this.writeToDb(new TestModelRoot().setFirstString("xxx").setSecondString("AAA").setDecimalValue(22.22));
        this.writeToDb(new TestModelRoot().setFirstString("xxx").setSecondString("BBB").setDecimalValue(11.11));
        this.writeToDb(new TestModelRoot().setFirstString("xxx").setSecondString("BBB").setDecimalValue(22.22));

        AndExpression andExpr = new AndExpression(equal("xxx", FIRST_STRING),
                equal("AAA", SECOND_STRING), equal(22.22, DECIMAL_VALUE));
        Map<String, Object> filter = expr2CouchDBFilter(andExpr);
        List<Document> foundDocs = this.couchDbService.findDocuments(filter, ROOT_PRIMITIVE_FIELDS);
        assertThat(foundDocs.size(), equalTo(1));
        assertThat(foundDocs.get(0).get(FIRST_STRING), equalTo("xxx"));
        assertThat(foundDocs.get(0).get(SECOND_STRING), equalTo("AAA"));
        assertThat(((LazilyParsedNumber) foundDocs.get(0).get(DECIMAL_VALUE)).doubleValue(), equalTo(22.22));
    }

    @Test
    public void findDocumentsTest_multLevelSelector() throws IOException {

        this.couchDbService.deleteDatabase(this.couchDbService.getDbName());
        this.couchDbService.createDatabase(this.couchDbService.getDbName());

        this.writeToDb(new TestModelRoot().setFirstString("AAA").setLevelValues("Lev1A", "Lev2A"));
        this.writeToDb(new TestModelRoot().setFirstString("BBB").setLevelValues("Lev1B", "Lev2B"));
        this.writeToDb(new TestModelRoot().setFirstString("CCC").setLevelValues("Lev1C", "Lev2A"));
        this.writeToDb(new TestModelRoot().setFirstString("DDD").setLevelValues("Lev1D", "Lev2B"));

        Map<String, Object> filter = expr2CouchDBFilter(equal("Lev2B", FIRST_LEVEL, SECOND_LEVEL, LEVEL_2_VALUE));
        List<Document> foundDocs = this.couchDbService.findDocuments(filter, ROOT_PRIMITIVE_FIELDS);
        assertThat(mapAttribute(foundDocs, FIRST_STRING), containsInAnyOrder("BBB", "DDD"));
    }

    @Test
    public void findDocumentsTest_searchInArray() throws IOException {

        this.couchDbService.deleteDatabase(this.couchDbService.getDbName());
        this.couchDbService.createDatabase(this.couchDbService.getDbName());

        this.writeToDb(new TestModelRoot().setFirstString("AAA").addListValues("ListVal1", "ListVal2", "ListVal3"));
        this.writeToDb(new TestModelRoot().setFirstString("BBB").addListValues("ListVal1", "ListVal5", "ListVal6"));
        this.writeToDb(new TestModelRoot().setFirstString("CCC").addListValues("ListVal7", "ListVal8", "ListVal2"));
        this.writeToDb(new TestModelRoot().setFirstString("DDD").addListValues("ListVal9", "ListVal3", "ListVal20"));

        Map<String, Object> filter = expr2CouchDBFilter(equal("ListVal2", ARRAY_VALUES, ENTRY_VALUE),
            ARRAY_FIELD_SELECTOR);
        List<Document> foundDocs = this.couchDbService.findDocuments(filter, ROOT_PRIMITIVE_FIELDS);
        assertThat(mapAttribute(foundDocs, FIRST_STRING), containsInAnyOrder("AAA", "CCC"));
    }


    private List<Object> mapAttribute(List<Document> foundDocs, String attributeName) {
        return foundDocs.stream()
                .map(doc -> doc.get(attributeName))
                .collect(toList());
    }

    private List<Object> mapAttributeDouble(List<Document> foundDocs, String attributeName) {
        return foundDocs.stream()
                .map(doc -> ((LazilyParsedNumber) doc.get(attributeName)).doubleValue())
                .collect(toList());
    }

    public void writeToDb(Object objectToWrite) throws JsonProcessingException {

        this.couchDbService.writeDocument(UUID.randomUUID(), objectToWrite);
    }

}
