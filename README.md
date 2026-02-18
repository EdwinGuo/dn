```
-- Find address country field somewhere
SHOW TABLES IN cif_tables;

-- If you have permission to search in information_schema:
SELECT table_schema, table_name
FROM system.information_schema.columns
WHERE lower(column_name) IN ('adres_country_c', 'ctry_lgly_frmd', 'sed1_s001_ctry_lgly_frmd', 'busn_subtype')
ORDER BY table_schema, table_name;

```

```sql
SELECT TOP 1 *
FROM cif.xcifacc_view;
```

Even better, ask SQL Server directly:

```sql
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'cif'
  AND TABLE_NAME = 'xcifacc_view'
ORDER BY ORDINAL_POSITION;
```

Try:

```sql
SELECT TOP 10 *
FROM cif.xcifacc_view acc
WHERE acc.applctn_id IN ('ACS','VSA');
```

(or whichever candidate column name you find)

---


```sql
-- AND acc.aplictin_id IN ('ACS','VSA')
```


```sql
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA='cif' AND TABLE_NAME IN ('xcifacc_view','xcifbas_personal_view')
ORDER BY TABLE_NAME, ORDINAL_POSITION;
```


```
query = """
SELECT
  acc.customr_num,
  acc.customr_bank_num,
  acc.customr_type,
  acc.aplictin_id,
  acc.ifw_effective_date,
  acc.record_type      AS acc_record_type,
  pers.record_type     AS pers_record_type,
  pers.customr_status
  -- add whatever else you need, but avoid duplicates or alias them
FROM cif.xcifacc_view acc
JOIN cif.xcifbas_personal_view pers
  ON acc.customr_num = pers.customr_num
 AND acc.customr_bank_num = pers.customr_bank_num
 AND acc.customr_type = pers.customr_type
WHERE acc.customr_bank_num = 4
  AND acc.customr_type = 0
  AND acc.aplictin_id IN ('ACS','VSA')
  AND CAST(SUBSTRING(acc.ifw_effective_date, 1, 8) AS date) <= '2024-10-31'
  AND pers.customr_status = '00'
"""

df = spark.read.jdbc(url=srzJdbcURL, table=f"({query}) t", properties=connectionProperties)
display(df)


```

```
WITH base_acc AS (
  SELECT
    acc.customr_num,
    acc.customr_bank_num,
    acc.customr_type,
    acc.aplictn_id,
    acc.ifw_effective_date
  FROM ra_fy_2025.cif_accounts_fy25 acc
  WHERE acc.customr_bank_num = 4
    AND acc.aplictn_id IN ('ACS', 'VSA')
    AND substring(acc.ifw_effective_date, 1, 8) <= '20251031'
),

-- ============= PERSONAL (customer_type = 0) =============
personal_active AS (
  SELECT
    b.customr_num,
    b.customr_bank_num,
    b.customr_type,
    b.aplictn_id,
    b.ifw_effective_date,
    p.customr_status
  FROM base_acc b
  JOIN ra_fy_2025.cif_personal_fy25 p
    ON b.customr_num = p.customr_num
   AND b.customr_bank_num = p.customr_bank_num
   AND b.customr_type = p.customr_type
  WHERE b.customr_type = 0
    AND p.customr_status = '00'
),

-- Primary address country for personal (adres_segment_num = 0)
personal_country AS (
  SELECT
    a.customr_num,
    a.customr_bank_num,
    a.customr_type,
    -- primary country
    adr.adres_country_c AS country_final,
    'PERSONAL_PRIMARY_ADDRESS' AS country_source
  FROM personal_active a
  LEFT JOIN ra_fy_2025.xcifadr adr               -- <-- replace if your address table name differs
    ON a.customr_num = adr.customr_num
   AND a.customr_bank_num = adr.customr_bank_num
   AND a.customr_type = adr.customr_type
   AND adr.adres_segment_num = 0
),

-- ============= NON-PERSONAL (customer_type = 1) =============
nonpersonal_active AS (
  SELECT
    b.customr_num,
    b.customr_bank_num,
    b.customr_type,
    b.aplictn_id,
    b.ifw_effective_date,
    np.customr_status,
    np.customr_industry_desgntn  -- business type indicator: U/I/P/S per requirement
  FROM base_acc b
  JOIN ra_fy_2025.cif_non_personal_fy25 np
    ON b.customr_num = np.customr_num
   AND b.customr_bank_num = np.customr_bank_num
   AND b.customr_type = np.customr_type
  WHERE b.customr_type = 1
    AND np.customr_status = '00'
),

-- Company type + legal formation country from compliance
np_compliance AS (
  SELECT
    c.customr_num,
    c.customr_bank_num,
    c.customr_type,
    c.busn_subtype          AS company_type,          -- e.g. CR, IA, FN, OT, MU, CK...
    c.ctry_lgly_frmd        AS ctry_lgly_frmd          -- or sed1_s001_ctry_lgly_frmd
  FROM ra_fy_2025.cif_customer_compliance c           -- <-- replace with actual table
),

-- Incorporation country from business customer
np_business AS (
  SELECT
    bc.customr_num,
    bc.customr_bank_num,
    bc.customr_type,
    bc.customr_inc_country_c AS inc_country
  FROM ra_fy_2025.cif_business_customer bc            -- <-- replace with actual table
),

-- Apply incorporation vs legal-formation rules
nonpersonal_country AS (
  SELECT
    a.customr_num,
    a.customr_bank_num,
    a.customr_type,

    a.customr_industry_desgntn AS business_type,      -- U/I/P/S
    comp.company_type,
    biz.inc_country,
    comp.ctry_lgly_frmd,

    CASE
      -- Always unincorporated:
      WHEN a.customr_industry_desgntn IN ('S','U') THEN false

      -- Business type P:
      WHEN a.customr_industry_desgntn = 'P' AND comp.company_type IN ('CR','CK') THEN true
      WHEN a.customr_industry_desgntn = 'P' AND comp.company_type = 'MU' THEN false

      -- Business type I:
      WHEN a.customr_industry_desgntn = 'I' AND comp.company_type IN ('CR','IA') THEN true
      WHEN a.customr_industry_desgntn = 'I' AND comp.company_type IN ('FN','OT') THEN false

      -- Fallback (if unknown): treat as unincorporated
      ELSE false
    END AS is_incorporated,

    CASE
      WHEN
        (
          CASE
            WHEN a.customr_industry_desgntn IN ('S','U') THEN false
            WHEN a.customr_industry_desgntn = 'P' AND comp.company_type IN ('CR','CK') THEN true
            WHEN a.customr_industry_desgntn = 'P' AND comp.company_type = 'MU' THEN false
            WHEN a.customr_industry_desgntn = 'I' AND comp.company_type IN ('CR','IA') THEN true
            WHEN a.customr_industry_desgntn = 'I' AND comp.company_type IN ('FN','OT') THEN false
            ELSE false
          END
        ) = true
      THEN
        -- Incorporated: use inc_country unless blank, else legal formation
        COALESCE(NULLIF(TRIM(biz.inc_country), ''), comp.ctry_lgly_frmd)
      ELSE
        -- Unincorporated: use legal formation
        comp.ctry_lgly_frmd
    END AS country_final,

    CASE
      WHEN
        (
          CASE
            WHEN a.customr_industry_desgntn IN ('S','U') THEN false
            WHEN a.customr_industry_desgntn = 'P' AND comp.company_type IN ('CR','CK') THEN true
            WHEN a.customr_industry_desgntn = 'P' AND comp.company_type = 'MU' THEN false
            WHEN a.customr_industry_desgntn = 'I' AND comp.company_type IN ('CR','IA') THEN true
            WHEN a.customr_industry_desgntn = 'I' AND comp.company_type IN ('FN','OT') THEN false
            ELSE false
          END
        ) = true
        AND COALESCE(NULLIF(TRIM(biz.inc_country), ''), '') <> ''
      THEN 'NONPERSONAL_INCORPORATION_COUNTRY'
      ELSE 'NONPERSONAL_LEGAL_FORMATION_COUNTRY'
    END AS country_source

  FROM nonpersonal_active a
  LEFT JOIN np_compliance comp
    ON a.customr_num = comp.customr_num
   AND a.customr_bank_num = comp.customr_bank_num
   AND a.customr_type = comp.customr_type
  LEFT JOIN np_business biz
    ON a.customr_num = biz.customr_num
   AND a.customr_bank_num = biz.customr_bank_num
   AND a.customr_type = biz.customr_type
)

-- ============= FINAL OUTPUT (unified) =============
SELECT
  pa.customr_num,
  pa.customr_bank_num,
  pa.customr_type,
  pa.aplictn_id,
  pa.ifw_effective_date,
  pa.customr_status,
  pc.country_final,
  pc.country_source,
  CAST(NULL AS STRING) AS business_type,
  CAST(NULL AS STRING) AS company_type
FROM personal_active pa
LEFT JOIN personal_country pc
  ON pa.customr_num = pc.customr_num
 AND pa.customr_bank_num = pc.customr_bank_num
 AND pa.customr_type = pc.customr_type

UNION ALL

SELECT
  na.customr_num,
  na.customr_bank_num,
  na.customr_type,
  na.aplictn_id,
  na.ifw_effective_date,
  na.customr_status,
  nc.country_final,
  nc.country_source,
  nc.business_type,
  nc.company_type
FROM nonpersonal_active na
LEFT JOIN nonpersonal_country nc
  ON na.customr_num = nc.customr_num
 AND na.customr_bank_num = nc.customr_bank_num
 AND na.customr_type = nc.customr_type
;

```
