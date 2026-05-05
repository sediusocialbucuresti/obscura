# B2B Authorized Source Plan

This plan tracks public or explicitly authorized sources for Europe and MENA
B2B discovery. These sources are used for lawful registry matching, supplier
discovery, and contact enrichment only where the source terms and robots rules
permit it.

## Priority Sources

1. EU business registers search: <https://e-justice.europa.eu/topics/registers-business-insolvency-land/business-registers-search-company-eu_en>
2. OpenCorporates API: <https://api.opencorporates.com/>
3. Companies House API for the United Kingdom: <https://developer.company-information.service.gov.uk/get-started>
4. SIRENE API / INSEE for France: <https://www.insee.fr/fr/information/3591226>
5. Belgium CBE Public Search: <https://www.belgium.be/nl/online_dienst/app_kbo_public_search>
6. Estonia e-Business Register API / open data: <https://www.rik.ee/en/e-business-register/company-registration-api>
7. KVK APIs for the Netherlands: <https://www.kvk.nl/en/ordering-products/get-business-data-with-kvk-apis/>
8. Europages supplier directory: <https://www.europages.de/>
9. Saudi Arabia Commercial Register inquiry: <https://mc.gov.sa/Pages/crq.aspx>
10. UAE license and activity verification hub: <https://u.ae/en/information-and-services/business/important-digital-services/inquire-about-licences-names-and-activities>
11. Qatar Chamber Directory: <https://www.qatarchamber.com/qcci-directory/>
12. Bahrain MOICT directory: <https://moic.gov.bh/en/directory>

## GLEIF Cross-Reference Flow

1. Resolve companies from an authorized source with immutable identifiers where possible.
2. Query GLEIF by LEI, or by legal name plus country when no LEI is available.
3. Attach LEI, legal-name normalization, registration status, registration authority, dates, and parent relationship references.
4. Build the canonical entity key from `name + jurisdiction + registration number`; fall back to `name + address`.
5. Keep provenance per field with source, timestamp, and source id.
6. Flag mismatches such as active registry status with stale or lapsed LEI status.

## Excluded Sources and Methods

- Private or paywalled lists unless the license explicitly permits this exact use.
- CAPTCHA, login, credential, or paywall bypasses.
- Personal contact harvesting without a lawful basis and explicit permission.
- User-generated contact claims that are not cross-checked against official or authorized sources.

## Contact Enrichment Rule

Use official company websites and permitted directories for role-based business
contacts such as `sales@`, `export@`, or published department phone numbers.
Personal-looking emails remain excluded from Mautic exports by default.
