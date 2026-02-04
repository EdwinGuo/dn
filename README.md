# deepnative

## Option 1 — Simple & Practical (most common)



Yep — this is a very **business-logic–heavy query**, so let’s translate it from “SQL brain” into something a **risk / compliance / business stakeholder** would understand.

I’ll break it into **what the query is doing**, **why it’s doing it**, and then give you a **clean business-ready explanation** you can reuse in documentation or audit responses.

---

## 1. What this query is doing (plain English)

At a high level, the query:

> **Identifies active TDAF customers, determines their legal structure based on company type and business type, excludes high-risk entities, and counts how many unique customers remain.**

---

## 2. Step-by-step business logic

### A. Build a base population of customers

The first part creates a **view of customers and their industry / company attributes**.

**Business intent:**

* Focus on **active corporate customers**
* Use **current, effective records**
* Limit scope to **specific applicant types and line of business**

**Key filters explained:**

* `acc.customer_bank_num = 4`
  → Only customers from a specific bank / entity (e.g. TD business unit)
* `acc.customer_type = 1`
  → Corporate / non-individual customers
* `acc.applictn_id IN ('ACS','VSA')`
  → Approved application channels
* `effective_date <= 2025-01-31`
  → Snapshot as of reporting date
* `npers.customer_status = '00'`
  → Active customers only

**Business result:**
A **clean, point-in-time population of active corporate customers**.

---

### B. Determine the customer’s legal structure

The `legal_structures` CTE maps **Company Type + Business Type** into a **standardized Legal Structure Code**.

**Business intent:**

* Normalize messy operational codes into **regulatory-friendly legal structure categories**
* Ensure **consistent classification** across reporting

**Examples of mappings:**

| Company Type   | Business Type | Legal Structure | Meaning                          |
| -------------- | ------------- | --------------- | -------------------------------- |
| S              | (any)         | SVCR            | Sovereign / Government Corp      |
| P              | CR            | PUBC            | Public Corporation               |
| P              | MU            | PUBG            | Public Corp – Government         |
| I              | CR            | PRVR            | Private Corporation              |
| I              | IA            | PRVA            | Private Incorporated Association |
| I              | FN            | PRVF            | Private First Nation             |
| U              | SP            | UNIS            | Unincorporated Sole Proprietor   |
| U              | PA            | UNIC            | Unincorporated Partnership       |
| U              | JV            | UNJV            | Unincorporated Joint Venture     |
| (null/unknown) |               | Default codes   | Business defaults                |

If none of the rules match:

* `OTHR` (Other)

**Business result:**
Every customer gets a **clear legal structure classification** suitable for:

* Risk assessment
* Regulatory reporting
* Portfolio segmentation

---

### C. Exclude high-risk customers

This line is critical from a **risk perspective**:

```sql
where upper(trim(ls.Updated_Risk_Rating)) != 'HIGH'
or ls.Updated_Risk_Rating is null
```

**Business intent:**

* **Exclude customers assessed as HIGH risk**
* Include customers where risk is **not rated or lower than high**

**Why this matters:**

* Supports **risk-adjusted metrics**
* Aligns with AML / KYC / compliance reporting requirements
* Prevents high-risk entities from skewing growth or exposure metrics

---

### D. Count distinct TDAF customers

Finally:

```sql
select count(distinct cust_no)
from TDAF_FULL_GEN
join legal_structures
```

**Business intent:**

* Produce a **unique customer count**
* Only for customers that:

  * Belong to **TDAF**
  * Are **active**
  * Have a **valid legal structure**
  * Are **not high-risk**

---

## 3. Business-ready explanation (copy-paste friendly)

Here’s a **clean explanation you can use in documentation, audit responses, or presentations**:

> This query identifies the active TDAF customer population as of January 31, 2025 and classifies each customer into a standardized legal structure based on company type and business type attributes.
>
> The logic filters for active, corporate customers from approved application channels and applies a point-in-time effective date to ensure reporting accuracy. Legal structure classifications are derived using predefined business rules that distinguish between public corporations, private corporations, unincorporated entities, government entities, and other organizational forms.
>
> Customers assessed as high risk are excluded to align with risk management and compliance requirements. The final output produces a distinct count of eligible TDAF customers, supporting regulatory reporting, risk analysis, and portfolio metrics.

---

## 4. If you want, I can also help you

* Turn this into **a data dictionary explanation**
* Create a **flow diagram** of the logic
* Rewrite it for **SOC / audit language**
* Simplify the CASE logic for maintainability
* Validate whether this logic is appropriate for **customer growth metrics**

Just tell me the audience 👍
