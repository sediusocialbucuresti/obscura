use crate::models::{CompanyProfile, ValidationReport};

pub fn validate_profile(
    profile: &CompanyProfile,
    obey_robots: bool,
    compliance_basis: &str,
) -> ValidationReport {
    let mut score = 0u8;
    let mut issues = Vec::new();
    let mut coverage = Vec::new();
    let mut flags = Vec::new();

    if !profile.company_name.trim().is_empty() {
        score += 20;
        coverage.push("company_name".to_string());
    } else {
        issues.push("missing_company_name".to_string());
    }

    if profile.canonical_domain.is_some() {
        score += 10;
        coverage.push("domain".to_string());
    }

    if profile
        .description
        .as_ref()
        .map(|s| s.len() >= 40)
        .unwrap_or(false)
    {
        score += 15;
        coverage.push("description".to_string());
    }

    if !profile.contacts.emails.is_empty() {
        score += 15;
        coverage.push("email".to_string());
    } else {
        issues.push("missing_email".to_string());
    }

    if !profile.contacts.phones.is_empty() {
        score += 10;
        coverage.push("phone".to_string());
    }

    if !profile.addresses.is_empty() || profile.country.is_some() {
        score += 10;
        coverage.push("location".to_string());
    }

    if !profile.products.is_empty() || !profile.services.is_empty() {
        score += 15;
        coverage.push("catalog".to_string());
    }

    if profile.company_type.is_some() || !profile.industries.is_empty() {
        score += 5;
        coverage.push("classification".to_string());
    }

    if profile.contacts.emails.iter().any(|email| email.personal) {
        flags.push("contains_personal_contact_data".to_string());
    }

    if obey_robots {
        flags.push("robots_txt_policy_enabled".to_string());
    } else {
        flags.push("robots_txt_policy_disabled".to_string());
    }

    flags.push(format!("source_basis:{}", compliance_basis));
    flags.push("mautic_default_export_role_based_emails_only".to_string());

    let status = if score >= 75 {
        "ready"
    } else if score >= 45 {
        "review"
    } else {
        "low_confidence"
    };

    if score < 75 {
        issues.push("manual_review_recommended".to_string());
    }

    ValidationReport {
        status: status.to_string(),
        score,
        issues,
        compliance_flags: flags,
        field_coverage: coverage,
    }
}
