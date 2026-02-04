# deepnative

## Option 1 — Simple & Practical (most common)


Yes — **exactly** 👍
If you count **only records where the derived legal structure = `HIGH`**, then **`OTHR` (unmatched) will be excluded automatically**, *as long as you filter correctly*.

Let me make it very explicit so there’s no ambiguity.

---

## 1. How `OTHR` behaves in your logic

In your CASE statement:

```sql
CASE
  WHEN ... THEN 'SVCR'
  WHEN ... THEN 'PUBC'
  ...
  ELSE 'OTHR'
END AS ls_entity_type_code
```

* `OTHR` is a **catch-all**
* It represents:

  * Unrecognized combinations
  * Missing / unexpected company type + business type
* It is **not HIGH**, unless you explicitly map it to HIGH (you are not)

So:

> **`OTHR` ≠ `HIGH`**

---

## 2. If you filter to `legal_structure = 'HIGH'`

Example:

```sql
where ls_entity_type_code = 'HIGH'
```

Then:

* ✅ Only rows explicitly classified as **HIGH**
* ❌ `OTHR` rows are **excluded**
* ❌ Any other non-HIGH legal structure is excluded

This is **safe and correct**.

---

## 3. Important distinction (very important)

There are **two different “HIGH” concepts** in your query:

### A. Risk rating HIGH (you already excluded)

```sql
where upper(trim(ls.updated_risk_rating)) != 'HIGH'
   or ls.updated_risk_rating is null
```

This:

* Excludes **high-risk customers**
* Has **nothing to do** with legal structure

### B. Legal structure classification = HIGH (your new requirement)

If you want to count **only customers whose legal structure is HIGH**, you must filter on the **derived legal structure field**, not the risk field.

Example:

```sql
where ls_entity_type_code = 'HIGH'
```

(or whatever column name you used for the CASE result)

---

## 4. Recommended safe pattern (very clear & audit-friendly)

I strongly recommend **explicit inclusion**, not relying on exclusion side effects.

### Example

```sql
where ls_entity_type_code = 'HIGH'
```

NOT:

```sql
where ls_entity_type_code != 'OTHR'
```

Why?

* Explicit = clearer to auditors
* Future-proof if new codes are added
* No accidental inclusion

---

## 5. Business explanation you can use

> The metric counts only customers whose legal structure is explicitly classified as HIGH based on predefined business rules. Records classified as OTHR represent unmatched or undefined legal structures and are intentionally excluded from the calculation.

This sentence alone will satisfy:

* Risk
* Compliance
* Audit
* Model validation

---

## 6. Quick validation query (optional but smart)

Run this once to be 100% confident:

```sql
select ls_entity_type_code, count(*)
from legal_structures
group by ls_entity_type_code;
```

You should see:

* `HIGH` → counted
* `OTHR` → present but excluded later

---

If you want, next I can:

* Verify whether **any OTHR should be treated as HIGH**
* Help you justify **why OTHR is excluded** in policy language
* Help you add a **data quality KPI** for % OTHR

Just say the word 👌
