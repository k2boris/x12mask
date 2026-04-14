package com.k2view.x12mask.extract;

import com.k2view.x12mask.model.TableDataset;
import com.k2view.x12mask.model.X12Message;
import com.k2view.x12mask.model.X12Segment;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Full extraction service for this application pipeline.
 *
 * Design intent:
 * - Extract all biz_* and map_* tables from incoming X12 in one pass.
 * - Keep extraction deterministic, heavily traceable, and easy to debug.
 * - Favor correctness and transparency over compact code.
 */
public final class ExtractionService {
    private static final Logger LOG = Logger.getLogger(ExtractionService.class.getName());

    public TableDataset extract(String sourceFileName, String transactionId, X12Message msg) {
        LOG.info(() -> "Starting extraction for source=" + sourceFileName + ", transaction_id=" + transactionId);

        TableDataset dataset = new TableDataset(sourceFileName, transactionId);

        ExtractionState st = new ExtractionState(transactionId);

        // Mandatory roots
        st.bizTransactionEntity.add(buildTransactionEntityRow(transactionId, msg));
        st.mapParserProfile.add(buildParserProfileRow(transactionId));

        // Build full locator table first (needed for runtime bindings).
        st.mapX12Locator.addAll(buildLocatorRows(transactionId, msg));
        st.indexLocators();

        // One-pass semantic extraction from segments
        for (X12Segment s : msg.segments()) {
            processSegment(s, st);
        }

        // Static mapping rules defined by profile (from agreed spec)
        st.mapExtractionRule.addAll(buildExtractionRules(transactionId));
        st.mapPhiClassificationRule.addAll(buildPhiRules(transactionId));
        st.mapReplacementManifestRule.addAll(buildReplacementRules(transactionId));

        // Resolve collected runtime binding intents to concrete locator rows
        st.finalizeBindings();

        // Publish every table into dataset (no placeholders; real extracted lists)
        dataset.putTable("biz_transaction_entity", st.bizTransactionEntity);
        dataset.putTable("biz_submitter_entity", st.bizSubmitterEntity);
        dataset.putTable("biz_receiver_entity", st.bizReceiverEntity);
        dataset.putTable("biz_billing_provider_entity", st.bizBillingProviderEntity);
        dataset.putTable("biz_payto_provider_entity", st.bizPaytoProviderEntity);
        dataset.putTable("biz_payer_entity", st.bizPayerEntity);
        dataset.putTable("biz_person_entity", st.bizPersonEntity);
        dataset.putTable("biz_person_name", st.bizPersonName);
        dataset.putTable("biz_person_demographic", st.bizPersonDemographic);
        dataset.putTable("biz_person_identifier", st.bizPersonIdentifier);
        dataset.putTable("biz_person_address", st.bizPersonAddress);
        dataset.putTable("biz_provider_entity", st.bizProviderEntity);
        dataset.putTable("biz_service_facility_entity", st.bizServiceFacilityEntity);
        dataset.putTable("biz_claim_entity", st.bizClaimEntity);
        dataset.putTable("biz_claim_policy_context", st.bizClaimPolicyContext);
        dataset.putTable("biz_claim_flags", st.bizClaimFlags);
        dataset.putTable("biz_claim_date", st.bizClaimDate);
        dataset.putTable("biz_claim_identifier", st.bizClaimIdentifier);
        dataset.putTable("biz_claim_attachment", st.bizClaimAttachment);
        dataset.putTable("biz_claim_condition", st.bizClaimCondition);
        dataset.putTable("biz_claim_note", st.bizClaimNote);
        dataset.putTable("biz_claim_diagnosis", st.bizClaimDiagnosis);
        dataset.putTable("biz_claim_provider_role", st.bizClaimProviderRole);
        dataset.putTable("biz_claim_service_facility", st.bizClaimServiceFacility);
        dataset.putTable("biz_service_line_entity", st.bizServiceLineEntity);
        dataset.putTable("biz_service_line_identifier", st.bizServiceLineIdentifier);

        dataset.putTable("map_parser_profile", st.mapParserProfile);
        dataset.putTable("map_x12_locator", st.mapX12Locator);
        dataset.putTable("map_extraction_rule", st.mapExtractionRule);
        dataset.putTable("map_phi_classification_rule", st.mapPhiClassificationRule);
        dataset.putTable("map_canonical_binding", st.mapCanonicalBinding);
        dataset.putTable("map_replacement_manifest_rule", st.mapReplacementManifestRule);

        LOG.info(() -> "Extraction completed; table count=" + dataset.tables().size()
                + ", claims=" + st.bizClaimEntity.size()
                + ", lines=" + st.bizServiceLineEntity.size()
                + ", locators=" + st.mapX12Locator.size());

        return dataset;
    }

    private void processSegment(X12Segment s, ExtractionState st) {
        String id = s.segmentId();
        List<String> e = s.elements();

        switch (id) {
            case "NM1" -> processNm1(s, e, st);
            case "PER" -> processPer(e, st);
            case "N3" -> processN3(e, st);
            case "N4" -> processN4(e, st);
            case "REF" -> processRef(s, e, st);
            case "PRV" -> processPrv(e, st);
            case "SBR" -> processSbr(e, st);
            case "CLM" -> processClm(e, st);
            case "DTP" -> processDtp(e, st);
            case "PWK" -> processPwk(e, st);
            case "K3" -> processK3(e, st);
            case "CRC" -> processCrc(e, st);
            case "HI" -> processHi(e, st);
            case "LX" -> processLx(e, st);
            case "SV1" -> processSv1(s, e, st);
            default -> {
                // No-op for segments not mapped to canonical tables in this version.
            }
        }
    }

    private void processNm1(X12Segment s, List<String> e, ExtractionState st) {
        String entityCode = val(e, 1);

        switch (entityCode) {
            case "41" -> {
                Map<String, Object> row = row(
                        "submitter_id", "SUB-1",
                        "transaction_id", st.transactionId,
                        "name", val(e, 3),
                        "identifier_type", val(e, 8),
                        "identifier_value", val(e, 9),
                        "contact_name", null,
                        "phone", null,
                        "email", null
                );
                st.bizSubmitterEntity.clear();
                st.bizSubmitterEntity.add(row);
                st.lastPerTarget = "submitter";
            }
            case "40" -> {
                Map<String, Object> row = row(
                        "receiver_id", "RCV-1",
                        "transaction_id", st.transactionId,
                        "name", val(e, 3),
                        "identifier_type", val(e, 8),
                        "identifier_value", val(e, 9)
                );
                st.bizReceiverEntity.clear();
                st.bizReceiverEntity.add(row);
                st.lastPerTarget = null;
            }
            case "85" -> {
                Map<String, Object> row = row(
                        "billing_provider_id", "BP-1",
                        "transaction_id", st.transactionId,
                        "organization_name", val(e, 3),
                        "npi", val(e, 9),
                        "taxonomy", null,
                        "tax_id", null,
                        "address_line1", null,
                        "address_line2", null,
                        "city", null,
                        "state", null,
                        "postal_code", null,
                        "contact_name", null,
                        "contact_phone", null
                );
                st.bizBillingProviderEntity.clear();
                st.bizBillingProviderEntity.add(row);
                st.lastAddressTarget = row;
                st.lastPerTarget = "billing_provider";
            }
            case "87" -> {
                Map<String, Object> row = row(
                        "payto_provider_id", "PTP-1",
                        "transaction_id", st.transactionId,
                        "organization_name", val(e, 3),
                        "npi", val(e, 9),
                        "address_line1", null,
                        "address_line2", null,
                        "city", null,
                        "state", null,
                        "postal_code", null
                );
                st.bizPaytoProviderEntity.clear();
                st.bizPaytoProviderEntity.add(row);
                st.lastAddressTarget = row;
                st.lastPerTarget = null;
            }
            case "PR" -> {
                String payerIdentifier = val(e, 9);
                String payerId = st.payerIdByIdentifier.computeIfAbsent(payerIdentifier, k -> "PAY-" + st.nextPayerId.incrementAndGet());
                if (!st.seenPayerIds.contains(payerId)) {
                    st.seenPayerIds.add(payerId);
                    st.bizPayerEntity.add(row(
                            "payer_id", payerId,
                            "transaction_id", st.transactionId,
                            "name", val(e, 3),
                            "payer_identifier", payerIdentifier
                    ));
                }
                st.currentClaimPayerId = payerId;
                st.lastPerTarget = null;
            }
            case "IL", "QC" -> {
                String personId = "P-" + st.nextPersonId.incrementAndGet();
                st.bizPersonEntity.add(row(
                        "person_id", personId,
                        "transaction_id", st.transactionId,
                        "person_type", "IL".equals(entityCode) ? "subscriber" : "patient",
                        "mastering_note", null
                ));

                String personNameId = "PN-" + st.nextPersonNameId.incrementAndGet();
                st.bizPersonName.add(row(
                        "person_name_id", personNameId,
                        "transaction_id", st.transactionId,
                        "person_id", personId,
                        "first_name", val(e, 4),
                        "last_name", val(e, 3),
                        "middle_name", val(e, 5)
                ));

                String idVal = val(e, 9);
                if (!idVal.isBlank()) {
                    String piId = "PI-" + st.nextPersonIdentifierId.incrementAndGet();
                    st.bizPersonIdentifier.add(row(
                            "person_identifier_id", piId,
                            "transaction_id", st.transactionId,
                            "person_id", personId,
                            "identifier_type", "IL".equals(entityCode) ? "member_id" : "dependent_member_id",
                            "identifier_value", idVal,
                            "assigning_authority", st.currentClaimPayerId
                    ));
                }

                st.lastPersonId = personId;
                st.lastAddressTarget = null;
                st.lastPerTarget = null;
                if ("IL".equals(entityCode)) {
                    st.currentSubscriberPersonId = personId;
                    st.currentPatientPersonId = personId; // default; may be overridden by QC
                } else {
                    st.currentPatientPersonId = personId;
                }
            }
            case "DN", "82" -> {
                String npi = val(e, 9);
                String providerId = st.providerIdByNpi.computeIfAbsent(npi, k -> "PRV-" + st.nextProviderId.incrementAndGet());
                if (!st.seenProviderIds.contains(providerId)) {
                    st.seenProviderIds.add(providerId);
                    st.bizProviderEntity.add(row(
                            "provider_id", providerId,
                            "transaction_id", st.transactionId,
                            "last_name", val(e, 3),
                            "first_name", val(e, 4),
                            "npi", npi,
                            "taxonomy", null
                    ));
                }

                if (st.currentClaimId != null) {
                    st.bizClaimProviderRole.add(row(
                            "claim_provider_role_id", "CPR-" + st.nextClaimProviderRoleId.incrementAndGet(),
                            "transaction_id", st.transactionId,
                            "claim_id", st.currentClaimId,
                            "provider_id", providerId,
                            "role_code", entityCode
                    ));
                }
                st.lastProviderNpi = npi;
            }
            case "77" -> {
                String npi = val(e, 9);
                String sfId = st.serviceFacilityIdByNpi.computeIfAbsent(npi, k -> "SF-" + st.nextServiceFacilityId.incrementAndGet());
                if (!st.seenServiceFacilityIds.contains(sfId)) {
                    st.seenServiceFacilityIds.add(sfId);
                    Map<String, Object> row = row(
                            "service_facility_id", sfId,
                            "transaction_id", st.transactionId,
                            "organization_name", val(e, 3),
                            "npi", npi,
                            "address_line1", null,
                            "city", null,
                            "state", null,
                            "postal_code", null
                    );
                    st.bizServiceFacilityEntity.add(row);
                    st.lastAddressTarget = row;
                }

                if (st.currentClaimId != null) {
                    st.bizClaimServiceFacility.add(row(
                            "claim_service_facility_id", "CSF-" + st.nextClaimServiceFacilityId.incrementAndGet(),
                            "transaction_id", st.transactionId,
                            "claim_id", st.currentClaimId,
                            "service_facility_id", sfId
                    ));
                }
            }
            default -> {
                // no-op
            }
        }
    }

    private void processPer(List<String> e, ExtractionState st) {
        if ("submitter".equals(st.lastPerTarget) && !st.bizSubmitterEntity.isEmpty()) {
            Map<String, Object> row = st.bizSubmitterEntity.get(0);
            row.put("contact_name", val(e, 2));
            row.put("phone", val(e, 4));
            row.put("email", val(e, 6));
            return;
        }
        if ("billing_provider".equals(st.lastPerTarget) && !st.bizBillingProviderEntity.isEmpty()) {
            Map<String, Object> row = st.bizBillingProviderEntity.get(0);
            row.put("contact_name", val(e, 2));
            row.put("contact_phone", val(e, 4));
        }
    }

    private void processN3(List<String> e, ExtractionState st) {
        if (st.lastPersonId != null) {
            Map<String, Object> row = row(
                    "person_address_id", "PA-" + st.nextPersonAddressId.incrementAndGet(),
                    "transaction_id", st.transactionId,
                    "person_id", st.lastPersonId,
                    "address_line1", val(e, 1),
                    "city", null,
                    "state", null,
                    "postal_code", null
            );
            st.bizPersonAddress.add(row);
            st.lastPersonAddressRow = row;
            return;
        }

        if (st.lastAddressTarget != null) {
            st.lastAddressTarget.put("address_line1", val(e, 1));
            if (st.lastAddressTarget.containsKey("address_line2")) {
                st.lastAddressTarget.put("address_line2", val(e, 2));
            }
        }
    }

    private void processN4(List<String> e, ExtractionState st) {
        if (st.lastPersonAddressRow != null) {
            st.lastPersonAddressRow.put("city", val(e, 1));
            st.lastPersonAddressRow.put("state", val(e, 2));
            st.lastPersonAddressRow.put("postal_code", val(e, 3));
            return;
        }

        if (st.lastAddressTarget != null) {
            st.lastAddressTarget.put("city", val(e, 1));
            st.lastAddressTarget.put("state", val(e, 2));
            st.lastAddressTarget.put("postal_code", val(e, 3));
        }
    }

    private void processRef(X12Segment s, List<String> e, ExtractionState st) {
        String qualifier = val(e, 1);
        String value = val(e, 2);

        // Billing-provider tax id (header)
        if ("EI".equals(qualifier) && !st.bizBillingProviderEntity.isEmpty() && st.currentClaimId == null) {
            st.bizBillingProviderEntity.get(0).put("tax_id", value);
            return;
        }

        // Line-level REF inside 2400 sequence
        if (st.currentServiceLineId != null && "6R".equals(qualifier)) {
            String sliId = "SLI-" + st.nextServiceLineIdentifierId.incrementAndGet();
            st.bizServiceLineIdentifier.add(row(
                    "service_line_identifier_id", sliId,
                    "transaction_id", st.transactionId,
                    "service_line_id", st.currentServiceLineId,
                    "qualifier", qualifier,
                    "identifier_value", value
            ));
            st.bindingRequests.add(new BindingRequest(
                    s.ordinal(),
                    s.segmentId(),
                    2,
                    null,
                    qualifier,
                    "biz_service_line_identifier",
                    sliId,
                    "identifier_value"
            ));
            return;
        }

        // Claim-level REF
        if (st.currentClaimId != null) {
            String ciId = "CI-" + st.nextClaimIdentifierId.incrementAndGet();
            st.bizClaimIdentifier.add(row(
                    "claim_identifier_id", ciId,
                    "transaction_id", st.transactionId,
                    "claim_id", st.currentClaimId,
                    "qualifier", qualifier,
                    "identifier_value", value,
                    "meaning", refMeaning(qualifier)
            ));
            st.bindingRequests.add(new BindingRequest(
                    s.ordinal(),
                    s.segmentId(),
                    2,
                    null,
                    qualifier,
                    "biz_claim_identifier",
                    ciId,
                    "identifier_value"
            ));
        }
    }

    private void processPrv(List<String> e, ExtractionState st) {
        if (st.lastProviderNpi == null) {
            return;
        }
        String taxonomy = val(e, 3);
        for (Map<String, Object> row : st.bizProviderEntity) {
            if (st.lastProviderNpi.equals(row.get("npi")) && (row.get("taxonomy") == null || String.valueOf(row.get("taxonomy")).isBlank())) {
                row.put("taxonomy", taxonomy);
                break;
            }
        }
        if (!st.bizBillingProviderEntity.isEmpty() && st.currentClaimId == null) {
            st.bizBillingProviderEntity.get(0).put("taxonomy", taxonomy);
        }
    }

    private void processSbr(List<String> e, ExtractionState st) {
        st.pendingPolicy = new LinkedHashMap<>();
        st.pendingPolicy.put("payer_responsibility", val(e, 1));
        st.pendingPolicy.put("subscriber_relationship_code", val(e, 2));
        st.pendingPolicy.put("group_number", val(e, 3));
        st.pendingPolicy.put("plan_name", val(e, 4));
        st.pendingPolicy.put("insurance_type", val(e, 5));
        st.pendingPolicy.put("benefit_plan_name", val(e, 6));
        st.pendingPolicy.put("patient_relationship_code", null);
    }

    private void processClm(List<String> e, ExtractionState st) {
        st.currentClaimId = "CLM-" + st.nextClaimId.incrementAndGet();
        st.currentServiceLineId = null;
        st.currentLineNumber = 0;

        String clm05 = val(e, 5);
        String[] clm05Parts = clm05.split(":", -1);
        String placeOfService = clm05Parts.length > 0 ? clm05Parts[0] : null;
        String facilityCodeQualifier = clm05Parts.length > 1 ? clm05Parts[1] : null;
        String claimFrequencyType = clm05Parts.length > 2 ? clm05Parts[2] : null;

        st.bizClaimEntity.add(row(
                "claim_id", st.currentClaimId,
                "transaction_id", st.transactionId,
                "claim_submitter_id", val(e, 1),
                "payer_id", st.currentClaimPayerId,
                "subscriber_person_id", st.currentSubscriberPersonId,
                "patient_person_id", st.currentPatientPersonId,
                "total_charge_amount", toDecimal(val(e, 2)),
                "place_of_service", placeOfService,
                "facility_code_qualifier", facilityCodeQualifier,
                "claim_frequency_type_code", claimFrequencyType
        ));

        String cpcId = "CPC-" + st.nextClaimPolicyContextId.incrementAndGet();
        Map<String, Object> p = st.pendingPolicy == null ? Map.of() : st.pendingPolicy;
        st.bizClaimPolicyContext.add(row(
                "claim_policy_context_id", cpcId,
                "transaction_id", st.transactionId,
                "claim_id", st.currentClaimId,
                "payer_responsibility", p.get("payer_responsibility"),
                "subscriber_relationship_code", p.get("subscriber_relationship_code"),
                "group_number", p.get("group_number"),
                "plan_name", p.get("plan_name"),
                "insurance_type", p.get("insurance_type"),
                "benefit_plan_name", p.get("benefit_plan_name"),
                "patient_relationship_code", p.get("patient_relationship_code"),
                "same_as_subscriber", bool01(st.currentSubscriberPersonId != null && st.currentSubscriberPersonId.equals(st.currentPatientPersonId))
        ));

        st.bizClaimFlags.add(row(
                "claim_flag_id", "CF-" + st.nextClaimFlagId.incrementAndGet(),
                "transaction_id", st.transactionId,
                "claim_id", st.currentClaimId,
                "provider_accept_assignment", boolYN(val(e, 6)),
                "benefits_assigned", boolYN(val(e, 7)),
                "release_of_information", boolYN(val(e, 8)),
                "delay_reason_code_present", bool01(!val(e, 20).isBlank())
        ));
    }

    private void processDtp(List<String> e, ExtractionState st) {
        String qualifier = val(e, 1);
        String format = val(e, 2);
        String value = val(e, 3);

        if ("434".equals(qualifier) && st.currentClaimId != null) {
            st.bizClaimDate.add(row(
                    "claim_date_id", "CD-" + st.nextClaimDateId.incrementAndGet(),
                    "transaction_id", st.transactionId,
                    "claim_id", st.currentClaimId,
                    "date_type", "statement",
                    "qualifier", qualifier,
                    "date_format", format,
                    "date_value", value
            ));
            return;
        }

        if ("472".equals(qualifier) && st.currentServiceLineRow != null) {
            st.currentServiceLineRow.put("date_of_service", value);
        }
    }

    private void processPwk(List<String> e, ExtractionState st) {
        if (st.currentClaimId == null) {
            return;
        }
        st.bizClaimAttachment.add(row(
                "claim_attachment_id", "CA-" + st.nextClaimAttachmentId.incrementAndGet(),
                "transaction_id", st.transactionId,
                "claim_id", st.currentClaimId,
                "report_type_code", val(e, 1),
                "transmission_code", val(e, 2),
                "control_number_qualifier", val(e, 5),
                "control_number", val(e, 6)
        ));
    }

    private void processK3(List<String> e, ExtractionState st) {
        if (st.currentClaimId == null) {
            return;
        }
        st.bizClaimNote.add(row(
                "claim_note_id", "CN-" + st.nextClaimNoteId.incrementAndGet(),
                "transaction_id", st.transactionId,
                "claim_id", st.currentClaimId,
                "note_type", "K3",
                "note_text", val(e, 1)
        ));
    }

    private void processCrc(List<String> e, ExtractionState st) {
        if (st.currentClaimId == null) {
            return;
        }
        st.bizClaimCondition.add(row(
                "claim_condition_id", "CC-" + st.nextClaimConditionId.incrementAndGet(),
                "transaction_id", st.transactionId,
                "claim_id", st.currentClaimId,
                "segment", "CRC",
                "category", val(e, 1),
                "certification_condition_indicator", val(e, 2),
                "condition_code1", val(e, 3)
        ));
    }

    private void processHi(List<String> e, ExtractionState st) {
        if (st.currentClaimId == null) {
            return;
        }
        for (int i = 1; i < e.size(); i++) {
            String item = val(e, i);
            if (item.isBlank()) {
                continue;
            }
            String[] p = item.split(":", -1);
            st.bizClaimDiagnosis.add(row(
                    "claim_diagnosis_id", "DX-" + st.nextClaimDiagnosisId.incrementAndGet(),
                    "transaction_id", st.transactionId,
                    "claim_id", st.currentClaimId,
                    "sequence", st.nextDiagnosisSeqForClaim(st.currentClaimId),
                    "qualifier", p.length > 0 ? p[0] : null,
                    "code", p.length > 1 ? p[1] : null
            ));
        }
    }

    private void processLx(List<String> e, ExtractionState st) {
        st.currentLineNumber = parseIntSafe(val(e, 1), st.currentLineNumber + 1);
        st.currentServiceLineId = null;
        st.currentServiceLineRow = null;
    }

    private void processSv1(X12Segment s, List<String> e, ExtractionState st) {
        if (st.currentClaimId == null) {
            return;
        }

        if (st.currentLineNumber <= 0) {
            st.currentLineNumber = st.currentLineNumber + 1;
        }

        String sv101 = val(e, 1);
        String[] proc = sv101.split(":", -1);
        String procQualifier = proc.length > 0 ? proc[0] : null;
        String procCode = proc.length > 1 ? proc[1] : null;

        st.currentServiceLineId = "SL-" + st.nextServiceLineId.incrementAndGet();
        Map<String, Object> row = row(
                "service_line_id", st.currentServiceLineId,
                "transaction_id", st.transactionId,
                "claim_id", st.currentClaimId,
                "line_number", st.currentLineNumber,
                "procedure_qualifier", procQualifier,
                "procedure_code", procCode,
                "charge_amount", toDecimal(val(e, 2)),
                "unit_measure", val(e, 3),
                "units", toDecimal(val(e, 4)),
                "date_of_service", null
        );
        st.currentServiceLineRow = row;
        st.bizServiceLineEntity.add(row);

        st.bindingRequests.add(new BindingRequest(
                s.ordinal(),
                s.segmentId(),
                1,
                2,
                null,
                "biz_service_line_entity",
                st.currentServiceLineId,
                "procedure_code"
        ));
    }

    private Map<String, Object> buildTransactionEntityRow(String transactionId, X12Message msg) {
        return row(
                "transaction_id", transactionId,
                "interchange_control_no", firstElement(msg, "ISA", 13),
                "functional_group_control_no", firstElement(msg, "GS", 6),
                "transaction_set_control_no", firstElement(msg, "ST", 2),
                "message_type", "837P",
                "version", firstElement(msg, "ST", 3),
                "source_format", "X12",
                "created_date", firstElement(msg, "GS", 4),
                "created_time", firstElement(msg, "GS", 5)
        );
    }

    private Map<String, Object> buildParserProfileRow(String transactionId) {
        return row(
                "parser_profile_id", "PP-1",
                "transaction_id", transactionId,
                "message_type", "837P",
                "version", "005010X222A1",
                "segment_delimiter", "~",
                "element_delimiter", "*",
                "component_delimiter", ":"
        );
    }

    private List<Map<String, Object>> buildLocatorRows(String transactionId, X12Message msg) {
        AtomicInteger seq = new AtomicInteger(1);
        List<Map<String, Object>> rows = new ArrayList<>();

        for (X12Segment segment : msg.segments()) {
            String qualifier = null;
            if ("REF".equals(segment.segmentId()) && segment.elements().size() > 1) {
                qualifier = segment.elements().get(1);
            }

            for (int i = 1; i < segment.elements().size(); i++) {
                String elementValue = segment.elements().get(i);
                rows.add(row(
                        "locator_id", String.format("LOC-%04d", seq.getAndIncrement()),
                        "transaction_id", transactionId,
                        "loop_id", "UNKNOWN",
                        "segment_ordinal", segment.ordinal(),
                        "segment_id", segment.segmentId(),
                        "element_position", i,
                        "component_position", null,
                        "repetition_ordinal", 1,
                        "qualifier", qualifier,
                        "value", elementValue
                ));

                if (elementValue != null && elementValue.contains(":")) {
                    String[] components = elementValue.split(":", -1);
                    for (int c = 0; c < components.length; c++) {
                        rows.add(row(
                                "locator_id", String.format("LOC-%04d", seq.getAndIncrement()),
                                "transaction_id", transactionId,
                                "loop_id", "UNKNOWN",
                                "segment_ordinal", segment.ordinal(),
                                "segment_id", segment.segmentId(),
                                "element_position", i,
                                "component_position", c + 1,
                                "repetition_ordinal", 1,
                                "qualifier", qualifier,
                                "value", components[c]
                        ));
                    }
                }
            }
        }

        LOG.info(() -> "Built map_x12_locator rows=" + rows.size());
        return rows;
    }

    private List<Map<String, Object>> buildExtractionRules(String transactionId) {
        List<Map<String, Object>> rows = new ArrayList<>();
        addRule(rows, "ER-001", transactionId, "2010BA/NM1/element4", "biz_person_name", "first_name", "claim.subscriber_person_id", "direct");
        addRule(rows, "ER-002", transactionId, "2010BA/NM1/element3", "biz_person_name", "last_name", "claim.subscriber_person_id", "direct");
        addRule(rows, "ER-003", transactionId, "2010BA/NM1/element5", "biz_person_name", "middle_name", "claim.subscriber_person_id", "direct_optional");
        addRule(rows, "ER-004", transactionId, "2010BA/NM1/element9", "biz_person_identifier", "identifier_value", "claim.subscriber_person_id", "direct");
        addRule(rows, "ER-005", transactionId, "2010BA/N3/element1", "biz_person_address", "address_line1", "claim.subscriber_person_id", "direct");
        addRule(rows, "ER-006", transactionId, "2010BA/N4/element1", "biz_person_address", "city", "claim.subscriber_person_id", "direct");
        addRule(rows, "ER-007", transactionId, "2010BA/N4/element2", "biz_person_address", "state", "claim.subscriber_person_id", "direct");
        addRule(rows, "ER-008", transactionId, "2010BA/N4/element3", "biz_person_address", "postal_code", "claim.subscriber_person_id", "direct");
        addRule(rows, "ER-009", transactionId, "2010BA/DMG/element2", "biz_person_demographic", "birth_date", "claim.subscriber_person_id", "direct");
        addRule(rows, "ER-010", transactionId, "2010BA/DMG/element3", "biz_person_demographic", "gender", "claim.subscriber_person_id", "direct");
        addRule(rows, "ER-011", transactionId, "2010CA/NM1/element4", "biz_person_name", "first_name", "claim.patient_person_id", "direct");
        addRule(rows, "ER-012", transactionId, "2010CA/NM1/element3", "biz_person_name", "last_name", "claim.patient_person_id", "direct");
        addRule(rows, "ER-013", transactionId, "2010CA/NM1/element9", "biz_person_identifier", "identifier_value", "claim.patient_person_id", "direct");
        addRule(rows, "ER-014", transactionId, "2300/CLM/element1", "biz_claim_entity", "claim_submitter_id", "claim.claim_id", "direct");
        addRule(rows, "ER-015", transactionId, "2300/CLM/element2", "biz_claim_entity", "total_charge_amount", "claim.claim_id", "direct_numeric");
        addRule(rows, "ER-016", transactionId, "2300/REF[D9]/element2", "biz_claim_identifier", "identifier_value", "claim.claim_id+qualifier", "direct");
        addRule(rows, "ER-017", transactionId, "2300/REF[EA]/element2", "biz_claim_identifier", "identifier_value", "claim.claim_id+qualifier", "direct");
        addRule(rows, "ER-018", transactionId, "2300/REF[G1]/element2", "biz_claim_identifier", "identifier_value", "claim.claim_id+qualifier", "direct");
        addRule(rows, "ER-019", transactionId, "2300/REF[6R]/element2", "biz_claim_identifier", "identifier_value", "claim.claim_id+qualifier", "direct");
        addRule(rows, "ER-020", transactionId, "2300/REF[9F]/element2", "biz_claim_identifier", "identifier_value", "claim.claim_id+qualifier", "direct");
        addRule(rows, "ER-021", transactionId, "2300/REF[F8]/element2", "biz_claim_identifier", "identifier_value", "claim.claim_id+qualifier", "direct");
        addRule(rows, "ER-022", transactionId, "2300/PWK/element6", "biz_claim_attachment", "control_number", "claim.claim_id", "direct");
        addRule(rows, "ER-023", transactionId, "2300/K3/element1", "biz_claim_note", "note_text", "claim.claim_id", "direct");
        addRule(rows, "ER-024", transactionId, "2300/CRC/element4", "biz_claim_condition", "condition_code1", "claim.claim_id", "direct");
        addRule(rows, "ER-025", transactionId, "2400/SV1/element1.component2", "biz_service_line_entity", "procedure_code", "claim.claim_id+line_number", "direct");
        addRule(rows, "ER-026", transactionId, "2400/SV1/element2", "biz_service_line_entity", "charge_amount", "claim.claim_id+line_number", "direct_numeric");
        addRule(rows, "ER-027", transactionId, "2400/DTP[472]/element3", "biz_service_line_entity", "date_of_service", "claim.claim_id+line_number", "direct");
        addRule(rows, "ER-028", transactionId, "2400/REF[6R]/element2", "biz_service_line_identifier", "identifier_value", "service_line.service_line_id+qualifier", "direct");
        return rows;
    }

    private List<Map<String, Object>> buildPhiRules(String transactionId) {
        List<Map<String, Object>> rows = new ArrayList<>();
        addPhi(rows, "PHI-001", transactionId, "2010BA/NM1/element3", "name", "true");
        addPhi(rows, "PHI-002", transactionId, "2010BA/NM1/element4", "name", "true");
        addPhi(rows, "PHI-003", transactionId, "2010BA/NM1/element5", "name", "true");
        addPhi(rows, "PHI-004", transactionId, "2010BA/NM1/element9", "member_id", "true");
        addPhi(rows, "PHI-005", transactionId, "2010BA/N3/element1", "address", "true");
        addPhi(rows, "PHI-006", transactionId, "2010BA/N4/element1", "address", "true");
        addPhi(rows, "PHI-007", transactionId, "2010BA/N4/element3", "address", "true");
        addPhi(rows, "PHI-008", transactionId, "2010BA/DMG/element2", "dob", "true");
        addPhi(rows, "PHI-009", transactionId, "2010CA/NM1/element3", "name", "true");
        addPhi(rows, "PHI-010", transactionId, "2010CA/NM1/element4", "name", "true");
        addPhi(rows, "PHI-011", transactionId, "2010CA/NM1/element9", "member_id", "true");
        addPhi(rows, "PHI-012", transactionId, "2010CA/N3/element1", "address", "true");
        addPhi(rows, "PHI-013", transactionId, "2010CA/N4/element1", "address", "true");
        addPhi(rows, "PHI-014", transactionId, "2010CA/N4/element3", "address", "true");
        addPhi(rows, "PHI-015", transactionId, "2010CA/DMG/element2", "dob", "true");
        addPhi(rows, "PHI-016", transactionId, "2300/REF[D9]/element2", "claim_reference", "conditional");
        addPhi(rows, "PHI-017", transactionId, "2300/REF[EA]/element2", "authorization", "conditional");
        addPhi(rows, "PHI-018", transactionId, "2300/REF[6R]/element2", "patient_account", "conditional");
        addPhi(rows, "PHI-019", transactionId, "2300/REF[9F]/element2", "referral", "conditional");
        addPhi(rows, "PHI-020", transactionId, "2300/REF[F8]/element2", "original_claim_reference", "conditional");
        addPhi(rows, "PHI-021", transactionId, "2300/PWK/element6", "attachment_reference", "conditional");
        addPhi(rows, "PHI-022", transactionId, "2300/K3/element1", "free_text_note", "conditional");
        addPhi(rows, "PHI-023", transactionId, "2400/REF[6R]/element2", "line_reference", "conditional");
        return rows;
    }

    private List<Map<String, Object>> buildReplacementRules(String transactionId) {
        List<Map<String, Object>> rows = new ArrayList<>();
        addRmr(rows, "RMR-001", transactionId, "biz_person_name", "first_name", "2010BA/NM1/element4", "replace_element");
        addRmr(rows, "RMR-002", transactionId, "biz_person_name", "last_name", "2010BA/NM1/element3", "replace_element");
        addRmr(rows, "RMR-003", transactionId, "biz_person_identifier", "identifier_value", "2010BA/NM1/element9", "replace_element");
        addRmr(rows, "RMR-004", transactionId, "biz_person_address", "address_line1", "2010BA/N3/element1", "replace_element");
        addRmr(rows, "RMR-005", transactionId, "biz_person_demographic", "birth_date", "2010BA/DMG/element2", "replace_element");
        addRmr(rows, "RMR-006", transactionId, "biz_person_name", "first_name", "2010CA/NM1/element4", "replace_element");
        addRmr(rows, "RMR-007", transactionId, "biz_person_name", "last_name", "2010CA/NM1/element3", "replace_element");
        addRmr(rows, "RMR-008", transactionId, "biz_person_identifier", "identifier_value", "2010CA/NM1/element9", "replace_element");
        addRmr(rows, "RMR-009", transactionId, "biz_claim_identifier", "identifier_value", "2300/REF/*/element2", "replace_element");
        addRmr(rows, "RMR-010", transactionId, "biz_claim_attachment", "control_number", "2300/PWK/element6", "replace_element");
        addRmr(rows, "RMR-011", transactionId, "biz_claim_note", "note_text", "2300/K3/element1", "replace_element");
        addRmr(rows, "RMR-012", transactionId, "biz_service_line_identifier", "identifier_value", "2400/REF[6R]/element2", "replace_element");
        return rows;
    }

    private void addRule(List<Map<String, Object>> rows, String id, String txn, String src, String tgtTable, String tgtCol, String key, String transform) {
        rows.add(row(
                "extraction_rule_id", id,
                "transaction_id", txn,
                "source_locator_pattern", src,
                "target_table", tgtTable,
                "target_column", tgtCol,
                "target_key_strategy", key,
                "transform_type", transform
        ));
    }

    private void addPhi(List<Map<String, Object>> rows, String id, String txn, String src, String cat, String req) {
        rows.add(row(
                "phi_rule_id", id,
                "transaction_id", txn,
                "source_locator_pattern", src,
                "phi_category", cat,
                "masking_required", req
        ));
    }

    private void addRmr(List<Map<String, Object>> rows, String id, String txn, String t, String c, String p, String m) {
        rows.add(row(
                "replacement_rule_id", id,
                "transaction_id", txn,
                "target_table", t,
                "target_column", c,
                "locator_pattern", p,
                "replacement_mode", m
        ));
    }

    private String refMeaning(String qualifier) {
        return switch (qualifier) {
            case "D9" -> "encounterIdentifier";
            case "EA" -> "priorAuthorization";
            case "6R" -> "patientAccountNumber";
            case "9F" -> "referralNumber";
            case "F8" -> "originalReferenceNumber";
            case "G1" -> "priorAuthorization";
            default -> "reference";
        };
    }

    private Object boolYN(String v) {
        if ("Y".equalsIgnoreCase(v) || "A".equalsIgnoreCase(v)) return 1;
        if ("N".equalsIgnoreCase(v)) return 0;
        return null;
    }

    private Object bool01(boolean b) {
        return b ? 1 : 0;
    }

    private Object toDecimal(String v) {
        if (v == null || v.isBlank()) return null;
        try {
            return Double.parseDouble(v);
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    private int parseIntSafe(String v, int fallback) {
        if (v == null || v.isBlank()) return fallback;
        try {
            return Integer.parseInt(v);
        } catch (NumberFormatException ex) {
            return fallback;
        }
    }

    private String firstElement(X12Message msg, String segmentId, int elementIndex) {
        for (X12Segment s : msg.segments()) {
            if (segmentId.equals(s.segmentId())) {
                return val(s.elements(), elementIndex);
            }
        }
        return null;
    }

    private String val(List<String> e, int idx) {
        if (idx < 0 || idx >= e.size()) return "";
        return e.get(idx) == null ? "" : e.get(idx);
    }

    private Map<String, Object> row(Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(String.valueOf(kv[i]), kv[i + 1]);
        }
        return m;
    }

    private record BindingRequest(
            int segmentOrdinal,
            String segmentId,
            int elementPosition,
            Integer componentPosition,
            String qualifier,
            String targetTable,
            String targetRowId,
            String targetColumn
    ) {}

    private static final class ExtractionState {
        final String transactionId;

        // Canonical tables
        final List<Map<String, Object>> bizTransactionEntity = new ArrayList<>();
        final List<Map<String, Object>> bizSubmitterEntity = new ArrayList<>();
        final List<Map<String, Object>> bizReceiverEntity = new ArrayList<>();
        final List<Map<String, Object>> bizBillingProviderEntity = new ArrayList<>();
        final List<Map<String, Object>> bizPaytoProviderEntity = new ArrayList<>();
        final List<Map<String, Object>> bizPayerEntity = new ArrayList<>();
        final List<Map<String, Object>> bizPersonEntity = new ArrayList<>();
        final List<Map<String, Object>> bizPersonName = new ArrayList<>();
        final List<Map<String, Object>> bizPersonDemographic = new ArrayList<>();
        final List<Map<String, Object>> bizPersonIdentifier = new ArrayList<>();
        final List<Map<String, Object>> bizPersonAddress = new ArrayList<>();
        final List<Map<String, Object>> bizProviderEntity = new ArrayList<>();
        final List<Map<String, Object>> bizServiceFacilityEntity = new ArrayList<>();
        final List<Map<String, Object>> bizClaimEntity = new ArrayList<>();
        final List<Map<String, Object>> bizClaimPolicyContext = new ArrayList<>();
        final List<Map<String, Object>> bizClaimFlags = new ArrayList<>();
        final List<Map<String, Object>> bizClaimDate = new ArrayList<>();
        final List<Map<String, Object>> bizClaimIdentifier = new ArrayList<>();
        final List<Map<String, Object>> bizClaimAttachment = new ArrayList<>();
        final List<Map<String, Object>> bizClaimCondition = new ArrayList<>();
        final List<Map<String, Object>> bizClaimNote = new ArrayList<>();
        final List<Map<String, Object>> bizClaimDiagnosis = new ArrayList<>();
        final List<Map<String, Object>> bizClaimProviderRole = new ArrayList<>();
        final List<Map<String, Object>> bizClaimServiceFacility = new ArrayList<>();
        final List<Map<String, Object>> bizServiceLineEntity = new ArrayList<>();
        final List<Map<String, Object>> bizServiceLineIdentifier = new ArrayList<>();

        // Mapping tables
        final List<Map<String, Object>> mapParserProfile = new ArrayList<>();
        final List<Map<String, Object>> mapX12Locator = new ArrayList<>();
        final List<Map<String, Object>> mapExtractionRule = new ArrayList<>();
        final List<Map<String, Object>> mapPhiClassificationRule = new ArrayList<>();
        final List<Map<String, Object>> mapCanonicalBinding = new ArrayList<>();
        final List<Map<String, Object>> mapReplacementManifestRule = new ArrayList<>();

        // Context
        String currentClaimId;
        String currentClaimPayerId;
        String currentSubscriberPersonId;
        String currentPatientPersonId;
        String currentServiceLineId;
        int currentLineNumber;
        Map<String, Object> currentServiceLineRow;

        String lastPersonId;
        Map<String, Object> lastPersonAddressRow;
        Map<String, Object> lastAddressTarget;
        String lastPerTarget;
        String lastProviderNpi;

        Map<String, Object> pendingPolicy;

        // ID counters
        AtomicInteger nextPayerId = new AtomicInteger(0);
        AtomicInteger nextPersonId = new AtomicInteger(0);
        AtomicInteger nextPersonNameId = new AtomicInteger(0);
        AtomicInteger nextPersonIdentifierId = new AtomicInteger(0);
        AtomicInteger nextPersonAddressId = new AtomicInteger(0);
        AtomicInteger nextProviderId = new AtomicInteger(0);
        AtomicInteger nextServiceFacilityId = new AtomicInteger(0);
        AtomicInteger nextClaimId = new AtomicInteger(0);
        AtomicInteger nextClaimPolicyContextId = new AtomicInteger(0);
        AtomicInteger nextClaimFlagId = new AtomicInteger(0);
        AtomicInteger nextClaimDateId = new AtomicInteger(0);
        AtomicInteger nextClaimIdentifierId = new AtomicInteger(0);
        AtomicInteger nextClaimAttachmentId = new AtomicInteger(0);
        AtomicInteger nextClaimConditionId = new AtomicInteger(0);
        AtomicInteger nextClaimNoteId = new AtomicInteger(0);
        AtomicInteger nextClaimDiagnosisId = new AtomicInteger(0);
        AtomicInteger nextClaimProviderRoleId = new AtomicInteger(0);
        AtomicInteger nextClaimServiceFacilityId = new AtomicInteger(0);
        AtomicInteger nextServiceLineId = new AtomicInteger(0);
        AtomicInteger nextServiceLineIdentifierId = new AtomicInteger(0);
        AtomicInteger nextBindingId = new AtomicInteger(0);

        Map<String, Integer> diagnosisSeqByClaim = new HashMap<>();

        // dedupe maps
        Map<String, String> payerIdByIdentifier = new HashMap<>();
        Set<String> seenPayerIds = new HashSet<>();
        Map<String, String> providerIdByNpi = new HashMap<>();
        Set<String> seenProviderIds = new HashSet<>();
        Map<String, String> serviceFacilityIdByNpi = new HashMap<>();
        Set<String> seenServiceFacilityIds = new HashSet<>();

        // Binding resolution support
        List<BindingRequest> bindingRequests = new ArrayList<>();
        Map<String, List<String>> locatorIndex = new HashMap<>();
        Set<String> locatorClaimed = new HashSet<>();

        ExtractionState(String transactionId) {
            this.transactionId = transactionId;
        }

        int nextDiagnosisSeqForClaim(String claimId) {
            int next = diagnosisSeqByClaim.getOrDefault(claimId, 0) + 1;
            diagnosisSeqByClaim.put(claimId, next);
            return next;
        }

        void indexLocators() {
            for (Map<String, Object> loc : mapX12Locator) {
                String key = key(
                        intVal(loc.get("segment_ordinal")),
                        String.valueOf(loc.get("segment_id")),
                        intVal(loc.get("element_position")),
                        intObj(loc.get("component_position")),
                        strObj(loc.get("qualifier"))
                );
                locatorIndex.computeIfAbsent(key, k -> new ArrayList<>()).add(String.valueOf(loc.get("locator_id")));
            }
        }

        void finalizeBindings() {
            for (BindingRequest req : bindingRequests) {
                String key = key(req.segmentOrdinal, req.segmentId, req.elementPosition, req.componentPosition, req.qualifier);
                List<String> ids = locatorIndex.getOrDefault(key, List.of());
                String locatorId = null;
                for (String id : ids) {
                    if (!locatorClaimed.contains(id)) {
                        locatorId = id;
                        locatorClaimed.add(id);
                        break;
                    }
                }
                if (locatorId == null) {
                    continue;
                }

                mapCanonicalBinding.add(mapRow(
                        "binding_id", String.format("BND-%03d", nextBindingId.incrementAndGet()),
                        "transaction_id", transactionId,
                        "locator_id", locatorId,
                        "target_table", req.targetTable,
                        "target_row_id", req.targetRowId,
                        "target_column", req.targetColumn
                ));
            }
        }

        private String key(int segOrd, String segId, int elemPos, Integer compPos, String qual) {
            return segOrd + "|" + segId + "|" + elemPos + "|" + (compPos == null ? "" : compPos) + "|" + (qual == null ? "" : qual);
        }

        private int intVal(Object o) {
            if (o == null) return 0;
            if (o instanceof Number n) return n.intValue();
            try { return Integer.parseInt(String.valueOf(o)); } catch (Exception e) { return 0; }
        }

        private Integer intObj(Object o) {
            if (o == null) return null;
            if (o instanceof Number n) return n.intValue();
            try { return Integer.parseInt(String.valueOf(o)); } catch (Exception e) { return null; }
        }

        private String strObj(Object o) {
            return o == null ? null : String.valueOf(o);
        }

        private Map<String, Object> mapRow(Object... kv) {
            Map<String, Object> m = new LinkedHashMap<>();
            for (int i = 0; i < kv.length; i += 2) {
                m.put(String.valueOf(kv[i]), kv[i + 1]);
            }
            return m;
        }
    }
}
