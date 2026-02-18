```
WITH acc AS (
  SELECT
    customr_num,
    customr_bank_num,
    customr_type,
    aplictn_id,
    ifw_effective_date
  FROM ra_fy_2025.cif_accounts_fy25
  WHERE customr_bank_num = 4
    AND aplictn_id IN ('ACS','VSA')
    AND substring(ifw_effective_date, 1, 8) <= '20251031'
),

-- ------------------------
-- PERSONAL (customr_type = 0)
-- ------------------------
pers_active AS (
  SELECT
    a.customr_num,
    a.customr_bank_num,
    a.customr_type,
    a.aplictn_id,
    a.ifw_effective_date
  FROM acc a
  JOIN ra_fy_2025.cif_personal_fy25 p
    ON a.customr_num = p.customr_num
   AND a.customr_bank_num = p.customr_bank_num
   AND a.customr_type = p.customr_type
  WHERE a.customr_type = '0'
    AND p.customr_status = '00'
),

pers_country AS (
  SELECT
    p.*,
    adr.adres_country_c AS country_final,
    'PERSONAL_PRIMARY_ADDRESS' AS country_source
  FROM pers_active p
  LEFT JOIN ra_fy_2025.cif_address_Aug31_2025 adr
    ON p.customr_num = adr.customr_num
   AND p.customr_bank_num = adr.customr_bank_num
   AND p.customr_type = adr.customr_type
   AND adr.adres_segmnt_num = 0   -- primary address
),

-- ------------------------
-- NON-PERSONAL (customr_type = 1)
-- ------------------------
np_active AS (
  SELECT
    a.customr_num,
    a.customr_bank_num,
    a.customr_type,
    a.aplictn_id,
    a.ifw_effective_date
  FROM acc a
  JOIN ra_fy_2025.cif_non_personal_fy25 np
    ON a.customr_num = np.customr_num
   AND a.customr_bank_num = np.customr_bank_num
   AND a.customr_type = np.customr_type
  WHERE a.customr_type = '1'
    AND np.customr_status = '00'
),

np_enriched AS (
  SELECT
    n.*,
    np.customr_industry_desgntn            AS business_type,   -- U/I/P/S
    np.customr_inc_country_c              AS inc_country,
    comp.sed1_s001_busn_subtype           AS company_type,    -- CR/IA/FN/OT/MU/CK...
    comp.sed1_s001_ctry_lgly_frmd         AS legal_form_country
  FROM np_active n
  JOIN ra_fy_2025.cif_non_personal_fy25 np
    ON n.customr_num = np.customr_num
   AND n.customr_bank_num = np.customr_bank_num
   AND n.customr_type = np.customr_type
  LEFT JOIN cif_tables.xcifed1_compl_npers_view comp
    ON n.customr_num = comp.customr_num
   AND n.customr_bank_num = comp.customr_bank_num
   AND n.customr_type = comp.customr_type
),

np_country AS (
  SELECT
    *,
    CASE
      WHEN business_type IN ('S','U') THEN false

      WHEN business_type = 'P' AND company_type IN ('CR','CK') THEN true
      WHEN business_type = 'P' AND company_type = 'MU' THEN false

      WHEN business_type = 'I' AND company_type IN ('CR','IA') THEN true
      WHEN business_type = 'I' AND company_type IN ('FN','OT') THEN false

      ELSE false
    END AS is_incorporated,

    CASE
      WHEN (
        CASE
          WHEN business_type IN ('S','U') THEN false
          WHEN business_type = 'P' AND company_type IN ('CR','CK') THEN true
          WHEN business_type = 'P' AND company_type = 'MU' THEN false
          WHEN business_type = 'I' AND company_type IN ('CR','IA') THEN true
          WHEN business_type = 'I' AND company_type IN ('FN','OT') THEN false
          ELSE false
        END
      ) = true
      THEN COALESCE(NULLIF(TRIM(inc_country), ''), legal_form_country)
      ELSE legal_form_country
    END AS country_final,

    CASE
      WHEN (
        CASE
          WHEN business_type IN ('S','U') THEN false
          WHEN business_type = 'P' AND company_type IN ('CR','CK') THEN true
          WHEN business_type = 'P' AND company_type = 'MU' THEN false
          WHEN business_type = 'I' AND company_type IN ('CR','IA') THEN true
          WHEN business_type = 'I' AND company_type IN ('FN','OT') THEN false
          ELSE false
        END
      ) = true
      AND COALESCE(NULLIF(TRIM(inc_country), ''), '') <> ''
      THEN 'NONPERSONAL_INCORPORATION_COUNTRY'
      ELSE 'NONPERSONAL_LEGAL_FORMATION_COUNTRY'
    END AS country_source
  FROM np_enriched
)

-- ------------------------
-- FINAL UNION
-- ------------------------
SELECT
  customr_num,
  customr_bank_num,
  customr_type,
  aplictn_id,
  ifw_effective_date,
  country_final,
  country_source
FROM pers_country

UNION ALL

SELECT
  customr_num,
  customr_bank_num,
  customr_type,
  aplictn_id,
  ifw_effective_date,
  country_final,
  country_source
FROM np_country
;

```
