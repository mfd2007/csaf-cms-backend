package de.bsi.secvisogram.csaf_cms_backend.rest.response;

import de.bsi.secvisogram.csaf_cms_backend.model.WorkflowState;
import io.swagger.v3.oas.annotations.media.Schema;

/**
 * Advisory content response
 */
@Schema(name="AdvisoryDocument")
public class AdvisoryResponse extends  AdvisoryInformationResponse {

    private String revision;
    private String csaf;

    public AdvisoryResponse(String advisoryId, WorkflowState status, String csafJsonWithComments) {
        super(advisoryId, status);
        this.csaf = csafJsonWithComments;
    }

    @Schema(description  = "The current CASF document enhanced with comment ids ", example = "{" +
            "document: { $comment: [23454], category: generic_csaf,...")
    public String getCsaf() {
      return this.csaf;
    }

    public void setCsaf(String csaf) {
        this.csaf = csaf;
    }

    @Schema(description  = "Document revision for optimistic concurrency", example = "2-efaa5db9409b2d4300535c70aaf6a66b")
    public String getRevision() {
        return revision;
    }

    public void setRevision(String revision) {
        this.revision = revision;
    }
}
