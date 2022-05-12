package de.bsi.secvisogram.csaf_cms_backend.couchdb;

import static de.bsi.secvisogram.csaf_cms_backend.json.AdvisoryJsonService.convertCsafToJson;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ibm.cloud.cloudant.v1.model.Document;
import de.bsi.secvisogram.csaf_cms_backend.CouchDBExtension;
import de.bsi.secvisogram.csaf_cms_backend.model.WorkflowState;
import de.bsi.secvisogram.csaf_cms_backend.rest.response.AdvisoryInformationResponse;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.xml.bind.DatatypeConverter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Test for the CouchDB service. The required CouchDB container is started in the CouchDBExtension.
 */
@SpringBootTest
@ExtendWith(CouchDBExtension.class)
public class CouchDbServiceTest {

    private final String[] DOCUMENT_TITLE = {"csaf", "document", "title"};

    @Autowired
    private CouchDbService couchDbService;

    @Test
    public void getServerVersionTest() {

        Assertions.assertEquals(CouchDBExtension.couchDbVersion, this.couchDbService.getServerVersion());
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
            ObjectNode objectNode = convertCsafToJson(csafJsonStream, newOwner, WorkflowState.Draft);
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
            ObjectNode objectNode = convertCsafToJson(csafJsonStream, owner, WorkflowState.Draft);
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
}
