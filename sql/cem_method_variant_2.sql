WITH stroke_events AS (
    SELECT
        d.subject_id,
        d.hadm_id,
        d.icd_code,
        d.icd_version,
        d.seq_num,
        a.admittime,
        a.dischtime,
        ROW_NUMBER() OVER (
            PARTITION BY d.subject_id
            ORDER BY a.admittime ASC, d.seq_num ASC
        ) AS rn
    FROM mimiciv_hosp.diagnoses_icd d
    INNER JOIN mimiciv_hosp.admissions a ON d.hadm_id = a.hadm_id
    WHERE d.icd_code LIKE 'I63%'
      AND d.icd_version = 10
),

index_stroke AS (
    SELECT
        subject_id,
        hadm_id   AS index_hadm_id,
        admittime AS index_admittime
    FROM stroke_events
    WHERE rn = 1
),

cci_icd10_map AS (
    -- Acute myocardial infarction (1)
    SELECT 'Acute myocardial infarction' AS condition, 'Acute myocardial infarction' AS condition_family, 1 AS weight, 'I21' AS icd10_code UNION ALL
    SELECT 'Acute myocardial infarction', 'Acute myocardial infarction', 1, 'I22' UNION ALL
    SELECT 'Acute myocardial infarction', 'Acute myocardial infarction', 1, 'I252' UNION ALL

    -- Congestive heart failure (1)
    SELECT 'Congestive heart failure', 'Congestive heart failure', 1, 'I50' UNION ALL

    -- Peripheral vascular disease (1)
    SELECT 'Peripheral vascular disease', 'Peripheral vascular disease', 1, 'I71' UNION ALL
    SELECT 'Peripheral vascular disease', 'Peripheral vascular disease', 1, 'I790' UNION ALL
    SELECT 'Peripheral vascular disease', 'Peripheral vascular disease', 1, 'I739' UNION ALL
    SELECT 'Peripheral vascular disease', 'Peripheral vascular disease', 1, 'R02' UNION ALL
    SELECT 'Peripheral vascular disease', 'Peripheral vascular disease', 1, 'Z958' UNION ALL
    SELECT 'Peripheral vascular disease', 'Peripheral vascular disease', 1, 'Z959' UNION ALL

    -- Pulmonary disease (1)
    SELECT 'Pulmonary disease', 'Pulmonary disease', 1, 'J40' UNION ALL
    SELECT 'Pulmonary disease', 'Pulmonary disease', 1, 'J41' UNION ALL
    SELECT 'Pulmonary disease', 'Pulmonary disease', 1, 'J42' UNION ALL
    SELECT 'Pulmonary disease', 'Pulmonary disease', 1, 'J44' UNION ALL
    SELECT 'Pulmonary disease', 'Pulmonary disease', 1, 'J43' UNION ALL
    SELECT 'Pulmonary disease', 'Pulmonary disease', 1, 'J45' UNION ALL
    SELECT 'Pulmonary disease', 'Pulmonary disease', 1, 'J46' UNION ALL
    SELECT 'Pulmonary disease', 'Pulmonary disease', 1, 'J47' UNION ALL
    SELECT 'Pulmonary disease', 'Pulmonary disease', 1, 'J67' UNION ALL
    SELECT 'Pulmonary disease', 'Pulmonary disease', 1, 'J60' UNION ALL
    SELECT 'Pulmonary disease', 'Pulmonary disease', 1, 'J61' UNION ALL
    SELECT 'Pulmonary disease', 'Pulmonary disease', 1, 'J62' UNION ALL
    SELECT 'Pulmonary disease', 'Pulmonary disease', 1, 'J63' UNION ALL
    SELECT 'Pulmonary disease', 'Pulmonary disease', 1, 'J66' UNION ALL
    SELECT 'Pulmonary disease', 'Pulmonary disease', 1, 'J64' UNION ALL
    SELECT 'Pulmonary disease', 'Pulmonary disease', 1, 'J65' UNION ALL

    -- Connective-tissue disorder (1)
    SELECT 'Connective-tissue disorder', 'Connective-tissue disorder', 1, 'M32' UNION ALL
    SELECT 'Connective-tissue disorder', 'Connective-tissue disorder', 1, 'M34' UNION ALL
    SELECT 'Connective-tissue disorder', 'Connective-tissue disorder', 1, 'M332' UNION ALL
    SELECT 'Connective-tissue disorder', 'Connective-tissue disorder', 1, 'M053' UNION ALL
    SELECT 'Connective-tissue disorder', 'Connective-tissue disorder', 1, 'M058' UNION ALL
    SELECT 'Connective-tissue disorder', 'Connective-tissue disorder', 1, 'M059' UNION ALL
    SELECT 'Connective-tissue disorder', 'Connective-tissue disorder', 1, 'M060' UNION ALL
    SELECT 'Connective-tissue disorder', 'Connective-tissue disorder', 1, 'M063' UNION ALL
    SELECT 'Connective-tissue disorder', 'Connective-tissue disorder', 1, 'M069' UNION ALL
    SELECT 'Connective-tissue disorder', 'Connective-tissue disorder', 1, 'M050' UNION ALL
    SELECT 'Connective-tissue disorder', 'Connective-tissue disorder', 1, 'M052' UNION ALL
    SELECT 'Connective-tissue disorder', 'Connective-tissue disorder', 1, 'M051' UNION ALL
    SELECT 'Connective-tissue disorder', 'Connective-tissue disorder', 1, 'M353' UNION ALL

    -- Peptic ulcer (1)
    SELECT 'Peptic ulcer', 'Peptic ulcer', 1, 'K25' UNION ALL
    SELECT 'Peptic ulcer', 'Peptic ulcer', 1, 'K26' UNION ALL
    SELECT 'Peptic ulcer', 'Peptic ulcer', 1, 'K27' UNION ALL
    SELECT 'Peptic ulcer', 'Peptic ulcer', 1, 'K28' UNION ALL

    -- Mild liver disease (1)  -> family: Liver disease
    SELECT 'Mild liver disease', 'Liver disease', 1, 'K702' UNION ALL
    SELECT 'Mild liver disease', 'Liver disease', 1, 'K703' UNION ALL
    SELECT 'Mild liver disease', 'Liver disease', 1, 'K73' UNION ALL
    SELECT 'Mild liver disease', 'Liver disease', 1, 'K717' UNION ALL
    SELECT 'Mild liver disease', 'Liver disease', 1, 'K740' UNION ALL
    SELECT 'Mild liver disease', 'Liver disease', 1, 'K742' UNION ALL
    SELECT 'Mild liver disease', 'Liver disease', 1, 'K746' UNION ALL
    SELECT 'Mild liver disease', 'Liver disease', 1, 'K743' UNION ALL
    SELECT 'Mild liver disease', 'Liver disease', 1, 'K744' UNION ALL
    SELECT 'Mild liver disease', 'Liver disease', 1, 'K745' UNION ALL

    -- Diabetes (1) -> family: Diabetes
    SELECT 'Diabetes', 'Diabetes', 1, 'E109' UNION ALL
    SELECT 'Diabetes', 'Diabetes', 1, 'E119' UNION ALL
    SELECT 'Diabetes', 'Diabetes', 1, 'E139' UNION ALL
    SELECT 'Diabetes', 'Diabetes', 1, 'E149' UNION ALL
    SELECT 'Diabetes', 'Diabetes', 1, 'E101' UNION ALL
    SELECT 'Diabetes', 'Diabetes', 1, 'E111' UNION ALL
    SELECT 'Diabetes', 'Diabetes', 1, 'E131' UNION ALL
    SELECT 'Diabetes', 'Diabetes', 1, 'E141' UNION ALL
    SELECT 'Diabetes', 'Diabetes', 1, 'E105' UNION ALL
    SELECT 'Diabetes', 'Diabetes', 1, 'E115' UNION ALL
    SELECT 'Diabetes', 'Diabetes', 1, 'E135' UNION ALL
    SELECT 'Diabetes', 'Diabetes', 1, 'E145' UNION ALL

    -- Diabetes with complications (2) -> family: Diabetes
    SELECT 'Diabetes with complications', 'Diabetes', 2, 'E102' UNION ALL
    SELECT 'Diabetes with complications', 'Diabetes', 2, 'E112' UNION ALL
    SELECT 'Diabetes with complications', 'Diabetes', 2, 'E132' UNION ALL
    SELECT 'Diabetes with complications', 'Diabetes', 2, 'E142' UNION ALL
    SELECT 'Diabetes with complications', 'Diabetes', 2, 'E103' UNION ALL
    SELECT 'Diabetes with complications', 'Diabetes', 2, 'E113' UNION ALL
    SELECT 'Diabetes with complications', 'Diabetes', 2, 'E133' UNION ALL
    SELECT 'Diabetes with complications', 'Diabetes', 2, 'E143' UNION ALL
    SELECT 'Diabetes with complications', 'Diabetes', 2, 'E104' UNION ALL
    SELECT 'Diabetes with complications', 'Diabetes', 2, 'E114' UNION ALL
    SELECT 'Diabetes with complications', 'Diabetes', 2, 'E134' UNION ALL
    SELECT 'Diabetes with complications', 'Diabetes', 2, 'E144' UNION ALL

    -- Paraplegia (2)
    SELECT 'Paraplegia', 'Paraplegia', 2, 'G81' UNION ALL
    SELECT 'Paraplegia', 'Paraplegia', 2, 'G041' UNION ALL
    SELECT 'Paraplegia', 'Paraplegia', 2, 'G820' UNION ALL
    SELECT 'Paraplegia', 'Paraplegia', 2, 'G821' UNION ALL
    SELECT 'Paraplegia', 'Paraplegia', 2, 'G822' UNION ALL

    -- Renal disease (2)
    SELECT 'Renal disease', 'Renal disease', 2, 'N03' UNION ALL
    SELECT 'Renal disease', 'Renal disease', 2, 'N052' UNION ALL
    SELECT 'Renal disease', 'Renal disease', 2, 'N053' UNION ALL
    SELECT 'Renal disease', 'Renal disease', 2, 'N054' UNION ALL
    SELECT 'Renal disease', 'Renal disease', 2, 'N055' UNION ALL
    SELECT 'Renal disease', 'Renal disease', 2, 'N056' UNION ALL
    SELECT 'Renal disease', 'Renal disease', 2, 'N072' UNION ALL
    SELECT 'Renal disease', 'Renal disease', 2, 'N073' UNION ALL
    SELECT 'Renal disease', 'Renal disease', 2, 'N074' UNION ALL
    SELECT 'Renal disease', 'Renal disease', 2, 'N01' UNION ALL
    SELECT 'Renal disease', 'Renal disease', 2, 'N18' UNION ALL
    SELECT 'Renal disease', 'Renal disease', 2, 'N19' UNION ALL
    SELECT 'Renal disease', 'Renal disease', 2, 'N25' UNION ALL

    -- Cancer (2) -> family: Cancer
    SELECT 'Cancer', 'Cancer', 2, 'C0' UNION ALL
    SELECT 'Cancer', 'Cancer', 2, 'C1' UNION ALL
    SELECT 'Cancer', 'Cancer', 2, 'C2' UNION ALL
    SELECT 'Cancer', 'Cancer', 2, 'C3' UNION ALL
    SELECT 'Cancer', 'Cancer', 2, 'C40' UNION ALL
    SELECT 'Cancer', 'Cancer', 2, 'C41' UNION ALL
    SELECT 'Cancer', 'Cancer', 2, 'C43' UNION ALL
    SELECT 'Cancer', 'Cancer', 2, 'C45' UNION ALL
    SELECT 'Cancer', 'Cancer', 2, 'C46' UNION ALL
    SELECT 'Cancer', 'Cancer', 2, 'C47' UNION ALL
    SELECT 'Cancer', 'Cancer', 2, 'C48' UNION ALL
    SELECT 'Cancer', 'Cancer', 2, 'C49' UNION ALL
    SELECT 'Cancer', 'Cancer', 2, 'C5' UNION ALL
    SELECT 'Cancer', 'Cancer', 2, 'C6' UNION ALL
    SELECT 'Cancer', 'Cancer', 2, 'C70' UNION ALL
    SELECT 'Cancer', 'Cancer', 2, 'C71' UNION ALL
    SELECT 'Cancer', 'Cancer', 2, 'C72' UNION ALL
    SELECT 'Cancer', 'Cancer', 2, 'C73' UNION ALL
    SELECT 'Cancer', 'Cancer', 2, 'C74' UNION ALL
    SELECT 'Cancer', 'Cancer', 2, 'C75' UNION ALL
    SELECT 'Cancer', 'Cancer', 2, 'C76' UNION ALL
    SELECT 'Cancer', 'Cancer', 2, 'C80' UNION ALL
    SELECT 'Cancer', 'Cancer', 2, 'C81' UNION ALL
    SELECT 'Cancer', 'Cancer', 2, 'C82' UNION ALL
    SELECT 'Cancer', 'Cancer', 2, 'C83' UNION ALL
    SELECT 'Cancer', 'Cancer', 2, 'C84' UNION ALL
    SELECT 'Cancer', 'Cancer', 2, 'C85' UNION ALL
    SELECT 'Cancer', 'Cancer', 2, 'C883' UNION ALL
    SELECT 'Cancer', 'Cancer', 2, 'C887' UNION ALL
    SELECT 'Cancer', 'Cancer', 2, 'C889' UNION ALL
    SELECT 'Cancer', 'Cancer', 2, 'C900' UNION ALL
    SELECT 'Cancer', 'Cancer', 2, 'C901' UNION ALL
    SELECT 'Cancer', 'Cancer', 2, 'C91' UNION ALL
    SELECT 'Cancer', 'Cancer', 2, 'C92' UNION ALL
    SELECT 'Cancer', 'Cancer', 2, 'C93' UNION ALL
    SELECT 'Cancer', 'Cancer', 2, 'C940' UNION ALL
    SELECT 'Cancer', 'Cancer', 2, 'C941' UNION ALL
    SELECT 'Cancer', 'Cancer', 2, 'C942' UNION ALL
    SELECT 'Cancer', 'Cancer', 2, 'C943' UNION ALL
    SELECT 'Cancer', 'Cancer', 2, 'C945' UNION ALL
    SELECT 'Cancer', 'Cancer', 2, 'C947' UNION ALL
    SELECT 'Cancer', 'Cancer', 2, 'C95' UNION ALL
    SELECT 'Cancer', 'Cancer', 2, 'C96' UNION ALL

    -- Metastatic cancer (3) -> family: Cancer
    SELECT 'Metastatic cancer', 'Cancer', 3, 'C77' UNION ALL
    SELECT 'Metastatic cancer', 'Cancer', 3, 'C78' UNION ALL
    SELECT 'Metastatic cancer', 'Cancer', 3, 'C79' UNION ALL
    SELECT 'Metastatic cancer', 'Cancer', 3, 'C80' UNION ALL

    -- Severe liver disease (3) -> family: Liver disease
    SELECT 'Severe liver disease', 'Liver disease', 3, 'K729' UNION ALL
    SELECT 'Severe liver disease', 'Liver disease', 3, 'K766' UNION ALL
    SELECT 'Severe liver disease', 'Liver disease', 3, 'K767' UNION ALL
    SELECT 'Severe liver disease', 'Liver disease', 3, 'K721' UNION ALL

    -- HIV (6)
    SELECT 'HIV', 'HIV', 6, 'B20' UNION ALL
    SELECT 'HIV', 'HIV', 6, 'B21' UNION ALL
    SELECT 'HIV', 'HIV', 6, 'B22' UNION ALL
    SELECT 'HIV', 'HIV', 6, 'B23' UNION ALL
    SELECT 'HIV', 'HIV', 6, 'B24'
),

cci_raw AS (
    SELECT
        d.subject_id,
        m.condition_family AS condition,
        m.weight
    FROM index_stroke s
    INNER JOIN mimiciv_hosp.admissions a_all
        ON a_all.subject_id = s.subject_id
    INNER JOIN mimiciv_hosp.diagnoses_icd d
        ON d.hadm_id = a_all.hadm_id
       AND d.subject_id = s.subject_id
       AND d.icd_version = 10
    INNER JOIN cci_icd10_map m
        ON d.icd_code LIKE m.icd10_code || '%'
    WHERE a_all.admittime <= s.index_admittime
),

cci_per_condition AS (
    SELECT
        subject_id,
        condition,
        MAX(weight) AS condition_weight
    FROM cci_raw
    GROUP BY subject_id, condition
),

cci_score AS (
    SELECT
        subject_id,
        SUM(condition_weight) AS cci_score
    FROM cci_per_condition
    GROUP BY subject_id
),

cohort AS (
    SELECT
        s.subject_id,
        s.index_hadm_id,
        s.index_admittime,
        p.gender,
        p.anchor_age,
        p.dod,
        (p.dod IS NOT NULL) AS died,
        COALESCE(c.cci_score, 0) AS cci_score
    FROM index_stroke s
    INNER JOIN mimiciv_hosp.patients p
        ON p.subject_id = s.subject_id
    LEFT JOIN cci_score c
        ON c.subject_id = s.subject_id
    WHERE p.anchor_age >= 18
),

/* 1) First lab within first 24h for each patient+lab (one value per lab per patient) */
lab_candidates AS (
    SELECT
        c.subject_id,
        le.itemid,
        le.valuenum,
        ROW_NUMBER() OVER (
            PARTITION BY c.subject_id, le.itemid 
            ORDER BY le.charttime ASC
        ) AS rn
    FROM cohort c
    INNER JOIN mimiciv_hosp.labevents le
        ON le.hadm_id = c.index_hadm_id
       AND le.charttime >= c.index_admittime
       AND le.charttime <  c.index_admittime + INTERVAL '24 hours'
       AND le.valuenum IS NOT NULL
       AND le.itemid IN (50912, 50931, 50983, 51006)  -- creatinine, glucose, sodium, BUN
),

labs_first AS (
    SELECT
        subject_id,
        MAX(CASE WHEN itemid = 50912 THEN valuenum END) AS creatinine_24h_first,
        MAX(CASE WHEN itemid = 50931 THEN valuenum END) AS glucose_24h_first,
        MAX(CASE WHEN itemid = 50983 THEN valuenum END) AS sodium_24h_first,
        MAX(CASE WHEN itemid = 51006 THEN valuenum END) AS bun_24h_first
    FROM lab_candidates
    WHERE rn = 1
    GROUP BY subject_id
),

cohort_with_labs AS (
    SELECT
        c.*,
        l.creatinine_24h_first,
        l.glucose_24h_first,
        l.sodium_24h_first,
        l.bun_24h_first
    FROM cohort c
    LEFT JOIN labs_first l
        ON l.subject_id = c.subject_id
),

lab_cutoffs AS (
  SELECT
    percentile_cont(0.02) WITHIN GROUP (ORDER BY creatinine_24h_first) AS creat_p01,
    percentile_cont(0.98) WITHIN GROUP (ORDER BY creatinine_24h_first) AS creat_p99,

    percentile_cont(0.02) WITHIN GROUP (ORDER BY glucose_24h_first) AS gluc_p01,
    percentile_cont(0.98) WITHIN GROUP (ORDER BY glucose_24h_first) AS gluc_p99,

    percentile_cont(0.02) WITHIN GROUP (ORDER BY sodium_24h_first) AS sod_p01,
    percentile_cont(0.98) WITHIN GROUP (ORDER BY sodium_24h_first) AS sod_p99,

    percentile_cont(0.02) WITHIN GROUP (ORDER BY bun_24h_first) AS bun_p01,
    percentile_cont(0.98) WITHIN GROUP (ORDER BY bun_24h_first) AS bun_p99
  FROM cohort_with_labs
),

cohort_with_labs_winsor AS (
  SELECT
    cwl.subject_id,
    cwl.index_hadm_id,
    cwl.index_admittime,
    cwl.gender,
    cwl.anchor_age,
    cwl.dod,
    cwl.died,
    cwl.cci_score,

    CASE
      WHEN cwl.creatinine_24h_first IS NULL THEN NULL
      WHEN cwl.creatinine_24h_first < lc.creat_p01 THEN lc.creat_p01
      WHEN cwl.creatinine_24h_first > lc.creat_p99 THEN lc.creat_p99
      ELSE cwl.creatinine_24h_first
    END AS creatinine_24h_first,

    CASE
      WHEN cwl.glucose_24h_first IS NULL THEN NULL
      WHEN cwl.glucose_24h_first < lc.gluc_p01 THEN lc.gluc_p01
      WHEN cwl.glucose_24h_first > lc.gluc_p99 THEN lc.gluc_p99
      ELSE cwl.glucose_24h_first
    END AS glucose_24h_first,

    CASE
      WHEN cwl.sodium_24h_first IS NULL THEN NULL
      WHEN cwl.sodium_24h_first < lc.sod_p01 THEN lc.sod_p01
      WHEN cwl.sodium_24h_first > lc.sod_p99 THEN lc.sod_p99
      ELSE cwl.sodium_24h_first
    END AS sodium_24h_first,

    CASE
      WHEN cwl.bun_24h_first IS NULL THEN NULL
      WHEN cwl.bun_24h_first < lc.bun_p01 THEN lc.bun_p01
      WHEN cwl.bun_24h_first > lc.bun_p99 THEN lc.bun_p99
      ELSE cwl.bun_24h_first
    END AS bun_24h_first

  FROM cohort_with_labs cwl
  CROSS JOIN lab_cutoffs lc
),


/* 2) Compute tertiles */
creat_tertiles AS (
    SELECT
        subject_id,
        NTILE(5) OVER (ORDER BY creatinine_24h_first) AS creat_tertile
    FROM cohort_with_labs_winsor
    WHERE creatinine_24h_first IS NOT NULL
),
gluc_tertiles AS (
    SELECT
        subject_id,
        NTILE(3) OVER (ORDER BY glucose_24h_first) AS glucose_tertile
    FROM cohort_with_labs_winsor
    WHERE glucose_24h_first IS NOT NULL
),
sod_tertiles AS (
    SELECT
        subject_id,
        NTILE(2) OVER (ORDER BY sodium_24h_first) AS sodium_tertile
    FROM cohort_with_labs_winsor
    WHERE sodium_24h_first IS NOT NULL
),
bun_tertiles AS (
    SELECT
        subject_id,
        NTILE(10) OVER (ORDER BY bun_24h_first) AS bun_tertile
    FROM cohort_with_labs_winsor
    WHERE bun_24h_first IS NOT NULL
),

cohort_banded AS (
    SELECT
        cwl.*,

		(anchor_age / 5) * 5 AS age_band_start,

        CASE WHEN creatinine_24h_first IS NULL THEN 1 ELSE 0 END AS creat_missing,
        COALESCE(ct.creat_tertile, 0) AS creat_bin, 

        CASE WHEN glucose_24h_first IS NULL THEN 1 ELSE 0 END AS glucose_missing,
        COALESCE(gt.glucose_tertile, 0) AS glucose_bin,

        CASE WHEN sodium_24h_first IS NULL THEN 1 ELSE 0 END AS sodium_missing,
        COALESCE(st.sodium_tertile, 0) AS sodium_bin,

        CASE WHEN bun_24h_first IS NULL THEN 1 ELSE 0 END AS bun_missing,
        COALESCE(bt.bun_tertile, 0) AS bun_bin

    FROM cohort_with_labs_winsor cwl
    LEFT JOIN creat_tertiles ct ON ct.subject_id = cwl.subject_id
    LEFT JOIN gluc_tertiles  gt ON gt.subject_id = cwl.subject_id
    LEFT JOIN sod_tertiles   st ON st.subject_id = cwl.subject_id
    LEFT JOIN bun_tertiles   bt ON bt.subject_id = cwl.subject_id
),


 cem_input AS (
    SELECT
        cb.*,
        CASE WHEN cb.cci_score > 5 THEN 1 ELSE 0 END AS treated,

        CONCAT_WS('|',
            cb.age_band_start::text,
            cb.gender::text,

            cb.creat_missing::text,  cb.creat_bin::text,
            cb.glucose_missing::text, cb.glucose_bin::text,
            cb.sodium_missing::text,  cb.sodium_bin::text,
            cb.bun_missing::text,     cb.bun_bin::text
        ) AS stratum_id
    FROM cohort_banded cb
),

stratum_counts AS (
    SELECT
        stratum_id,
        SUM(treated) AS n_treated,
        SUM(1 - treated) AS n_control
    FROM cem_input
    GROUP BY stratum_id
),

/* CEM pruning step: keep only strata with BOTH treated and control */
cem_matched AS (
    SELECT i.*, s.n_treated, s.n_control
    FROM cem_input i
    JOIN stratum_counts s USING (stratum_id)
    WHERE s.n_treated > 0 AND s.n_control > 0
),

cem_weighted AS (
    SELECT
        m.*,
        CASE
            WHEN m.treated = 1 THEN 1.0
            ELSE (m.n_treated::float / NULLIF(m.n_control, 0))
        END AS cem_w
    FROM cem_matched m
),

/* ---------- BEFORE SMDs------- */
smd_before AS (
  SELECT
    /* age */
    ( AVG(anchor_age) FILTER (WHERE cci_score > 5)
    - AVG(anchor_age) FILTER (WHERE cci_score <= 5)
    )
    / NULLIF(
        SQRT((
          POWER(STDDEV_SAMP(anchor_age) FILTER (WHERE cci_score > 5), 2)
        + POWER(STDDEV_SAMP(anchor_age) FILTER (WHERE cci_score <= 5), 2)
        ) / 2.0),
        0
      ) AS smd_age,

    /* male */
    ( AVG((gender = 'M')::int) FILTER (WHERE cci_score > 5)
    - AVG((gender = 'M')::int) FILTER (WHERE cci_score <= 5)
    )
    / NULLIF(
        SQRT((
          POWER(STDDEV_SAMP((gender = 'M')::int) FILTER (WHERE cci_score > 5), 2)
        + POWER(STDDEV_SAMP((gender = 'M')::int) FILTER (WHERE cci_score <= 5), 2)
        ) / 2.0),
        0
      ) AS smd_male,

    ( AVG(creat_bin) FILTER (WHERE cci_score > 5) - AVG(creat_bin) FILTER (WHERE cci_score <= 5) )
    / NULLIF(SQRT((POWER(STDDEV_SAMP(creat_bin) FILTER (WHERE cci_score > 5),2)
                 + POWER(STDDEV_SAMP(creat_bin) FILTER (WHERE cci_score <= 5),2))/2.0),0) AS smd_creat_bin,

    ( AVG(glucose_bin) FILTER (WHERE cci_score > 5) - AVG(glucose_bin) FILTER (WHERE cci_score <= 5) )
    / NULLIF(SQRT((POWER(STDDEV_SAMP(glucose_bin) FILTER (WHERE cci_score > 5),2)
                 + POWER(STDDEV_SAMP(glucose_bin) FILTER (WHERE cci_score <= 5),2))/2.0),0) AS smd_glucose_bin,

    ( AVG(sodium_bin) FILTER (WHERE cci_score > 5) - AVG(sodium_bin) FILTER (WHERE cci_score <= 5) )
    / NULLIF(SQRT((POWER(STDDEV_SAMP(sodium_bin) FILTER (WHERE cci_score > 5),2)
                 + POWER(STDDEV_SAMP(sodium_bin) FILTER (WHERE cci_score <= 5),2))/2.0),0) AS smd_sodium_bin,

    ( AVG(bun_bin) FILTER (WHERE cci_score > 5) - AVG(bun_bin) FILTER (WHERE cci_score <= 5) )
    / NULLIF(SQRT((POWER(STDDEV_SAMP(bun_bin) FILTER (WHERE cci_score > 5),2)
                 + POWER(STDDEV_SAMP(bun_bin) FILTER (WHERE cci_score <= 5),2))/2.0),0) AS smd_bun_bin,

    ( AVG(creat_missing::int) FILTER (WHERE cci_score > 5) - AVG(creat_missing::int) FILTER (WHERE cci_score <= 5) )
    / NULLIF(SQRT((POWER(STDDEV_SAMP(creat_missing::int) FILTER (WHERE cci_score > 5),2)
                 + POWER(STDDEV_SAMP(creat_missing::int) FILTER (WHERE cci_score <= 5),2))/2.0),0) AS smd_creat_missing,

    ( AVG(glucose_missing::int) FILTER (WHERE cci_score > 5) - AVG(glucose_missing::int) FILTER (WHERE cci_score <= 5) )
    / NULLIF(SQRT((POWER(STDDEV_SAMP(glucose_missing::int) FILTER (WHERE cci_score > 5),2)
                 + POWER(STDDEV_SAMP(glucose_missing::int) FILTER (WHERE cci_score <= 5),2))/2.0),0) AS smd_glucose_missing,

    ( AVG(sodium_missing::int) FILTER (WHERE cci_score > 5) - AVG(sodium_missing::int) FILTER (WHERE cci_score <= 5) )
    / NULLIF(SQRT((POWER(STDDEV_SAMP(sodium_missing::int) FILTER (WHERE cci_score > 5),2)
                 + POWER(STDDEV_SAMP(sodium_missing::int) FILTER (WHERE cci_score <= 5),2))/2.0),0) AS smd_sodium_missing,

    ( AVG(bun_missing::int) FILTER (WHERE cci_score > 5) - AVG(bun_missing::int) FILTER (WHERE cci_score <= 5) )
    / NULLIF(SQRT((POWER(STDDEV_SAMP(bun_missing::int) FILTER (WHERE cci_score > 5),2)
                 + POWER(STDDEV_SAMP(bun_missing::int) FILTER (WHERE cci_score <= 5),2))/2.0),0) AS smd_bun_missing

  FROM cohort_banded
),

smd_before_cont AS (
  SELECT
    ( AVG(creatinine_24h_first) FILTER (WHERE cci_score > 5 AND creatinine_24h_first IS NOT NULL)
    - AVG(creatinine_24h_first) FILTER (WHERE cci_score <= 5 AND creatinine_24h_first IS NOT NULL)
    )
    / NULLIF(
        SQRT((
          POWER(STDDEV_SAMP(creatinine_24h_first) FILTER (WHERE cci_score > 5 AND creatinine_24h_first IS NOT NULL), 2)
        + POWER(STDDEV_SAMP(creatinine_24h_first) FILTER (WHERE cci_score <= 5 AND creatinine_24h_first IS NOT NULL), 2)
        ) / 2.0),
      0
    ) AS smd_creatinine_cont,

    ( AVG(glucose_24h_first) FILTER (WHERE cci_score > 5 AND glucose_24h_first IS NOT NULL)
    - AVG(glucose_24h_first) FILTER (WHERE cci_score <= 5 AND glucose_24h_first IS NOT NULL)
    )
    / NULLIF(
        SQRT((
          POWER(STDDEV_SAMP(glucose_24h_first) FILTER (WHERE cci_score > 5 AND glucose_24h_first IS NOT NULL), 2)
        + POWER(STDDEV_SAMP(glucose_24h_first) FILTER (WHERE cci_score <= 5 AND glucose_24h_first IS NOT NULL), 2)
        ) / 2.0),
      0
    ) AS smd_glucose_cont,

    ( AVG(sodium_24h_first) FILTER (WHERE cci_score > 5 AND sodium_24h_first IS NOT NULL)
    - AVG(sodium_24h_first) FILTER (WHERE cci_score <= 5 AND sodium_24h_first IS NOT NULL)
    )
    / NULLIF(
        SQRT((
          POWER(STDDEV_SAMP(sodium_24h_first) FILTER (WHERE cci_score > 5 AND sodium_24h_first IS NOT NULL), 2)
        + POWER(STDDEV_SAMP(sodium_24h_first) FILTER (WHERE cci_score <= 5 AND sodium_24h_first IS NOT NULL), 2)
        ) / 2.0),
      0
    ) AS smd_sodium_cont,

    ( AVG(bun_24h_first) FILTER (WHERE cci_score > 5 AND bun_24h_first IS NOT NULL)
    - AVG(bun_24h_first) FILTER (WHERE cci_score <= 5 AND bun_24h_first IS NOT NULL)
    )
    / NULLIF(
        SQRT((
          POWER(STDDEV_SAMP(bun_24h_first) FILTER (WHERE cci_score > 5 AND bun_24h_first IS NOT NULL), 2)
        + POWER(STDDEV_SAMP(bun_24h_first) FILTER (WHERE cci_score <= 5 AND bun_24h_first IS NOT NULL), 2)
        ) / 2.0),
      0
    ) AS smd_bun_cont
  FROM cohort_banded
),


/* ---------- AFTER SMDs ---------- */
cem_moments AS (
  SELECT
    treated,

    SUM(cem_w) AS sw,

    SUM(cem_w * anchor_age) AS sw_age,
    SUM(cem_w * anchor_age * anchor_age) AS sw_age2,

    SUM(cem_w * ((gender='M')::int)) AS sw_male,
    SUM(cem_w * ((gender='M')::int) * ((gender='M')::int)) AS sw_male2,

    SUM(cem_w * creat_bin) AS sw_creat_bin,
    SUM(cem_w * creat_bin * creat_bin) AS sw_creat_bin2,

    SUM(cem_w * glucose_bin) AS sw_glucose_bin,
    SUM(cem_w * glucose_bin * glucose_bin) AS sw_glucose_bin2,

    SUM(cem_w * sodium_bin) AS sw_sodium_bin,
    SUM(cem_w * sodium_bin * sodium_bin) AS sw_sodium_bin2,

    SUM(cem_w * bun_bin) AS sw_bun_bin,
    SUM(cem_w * bun_bin * bun_bin) AS sw_bun_bin2,

    SUM(cem_w * (creat_missing::int)) AS sw_creat_miss,
    SUM(cem_w * (creat_missing::int) * (creat_missing::int)) AS sw_creat_miss2,

    SUM(cem_w * (glucose_missing::int)) AS sw_gluc_miss,
    SUM(cem_w * (glucose_missing::int) * (glucose_missing::int)) AS sw_gluc_miss2,

    SUM(cem_w * (sodium_missing::int)) AS sw_sod_miss,
    SUM(cem_w * (sodium_missing::int) * (sodium_missing::int)) AS sw_sod_miss2,

    SUM(cem_w * (bun_missing::int)) AS sw_bun_miss,
    SUM(cem_w * (bun_missing::int) * (bun_missing::int)) AS sw_bun_miss2

  FROM cem_weighted
  GROUP BY treated
),

cem_stats AS (
  SELECT
    treated,
    sw,

    (sw_age / NULLIF(sw,0)) AS mean_age,
    (sw_age2 / NULLIF(sw,0)) - POWER((sw_age / NULLIF(sw,0)), 2) AS var_age,

    (sw_male / NULLIF(sw,0)) AS mean_male,
    (sw_male2 / NULLIF(sw,0)) - POWER((sw_male / NULLIF(sw,0)), 2) AS var_male,

    (sw_creat_bin / NULLIF(sw,0)) AS mean_creat_bin,
    (sw_creat_bin2 / NULLIF(sw,0)) - POWER((sw_creat_bin / NULLIF(sw,0)), 2) AS var_creat_bin,

    (sw_glucose_bin / NULLIF(sw,0)) AS mean_glucose_bin,
    (sw_glucose_bin2 / NULLIF(sw,0)) - POWER((sw_glucose_bin / NULLIF(sw,0)), 2) AS var_glucose_bin,

    (sw_sodium_bin / NULLIF(sw,0)) AS mean_sodium_bin,
    (sw_sodium_bin2 / NULLIF(sw,0)) - POWER((sw_sodium_bin / NULLIF(sw,0)), 2) AS var_sodium_bin,

    (sw_bun_bin / NULLIF(sw,0)) AS mean_bun_bin,
    (sw_bun_bin2 / NULLIF(sw,0)) - POWER((sw_bun_bin / NULLIF(sw,0)), 2) AS var_bun_bin,

    (sw_creat_miss / NULLIF(sw,0)) AS mean_creat_missing,
    (sw_creat_miss2 / NULLIF(sw,0)) - POWER((sw_creat_miss / NULLIF(sw,0)), 2) AS var_creat_missing,

    (sw_gluc_miss / NULLIF(sw,0)) AS mean_glucose_missing,
    (sw_gluc_miss2 / NULLIF(sw,0)) - POWER((sw_gluc_miss / NULLIF(sw,0)), 2) AS var_glucose_missing,

    (sw_sod_miss / NULLIF(sw,0)) AS mean_sodium_missing,
    (sw_sod_miss2 / NULLIF(sw,0)) - POWER((sw_sod_miss / NULLIF(sw,0)), 2) AS var_sodium_missing,

    (sw_bun_miss / NULLIF(sw,0)) AS mean_bun_missing,
    (sw_bun_miss2 / NULLIF(sw,0)) - POWER((sw_bun_miss / NULLIF(sw,0)), 2) AS var_bun_missing

  FROM cem_moments
),

smd_after AS (
  SELECT
    /* join treated=1 and treated=0 stats */
    (t.mean_age - c.mean_age)
      / NULLIF(SQRT((t.var_age + c.var_age) / 2.0), 0) AS smd_age,

    (t.mean_male - c.mean_male)
      / NULLIF(SQRT((t.var_male + c.var_male) / 2.0), 0) AS smd_male,

    (t.mean_creat_bin - c.mean_creat_bin)
      / NULLIF(SQRT((t.var_creat_bin + c.var_creat_bin) / 2.0), 0) AS smd_creat_bin,

    (t.mean_glucose_bin - c.mean_glucose_bin)
      / NULLIF(SQRT((t.var_glucose_bin + c.var_glucose_bin) / 2.0), 0) AS smd_glucose_bin,

    (t.mean_sodium_bin - c.mean_sodium_bin)
      / NULLIF(SQRT((t.var_sodium_bin + c.var_sodium_bin) / 2.0), 0) AS smd_sodium_bin,

    (t.mean_bun_bin - c.mean_bun_bin)
      / NULLIF(SQRT((t.var_bun_bin + c.var_bun_bin) / 2.0), 0) AS smd_bun_bin,

    (t.mean_creat_missing - c.mean_creat_missing)
      / NULLIF(SQRT((t.var_creat_missing + c.var_creat_missing) / 2.0), 0) AS smd_creat_missing,

    (t.mean_glucose_missing - c.mean_glucose_missing)
      / NULLIF(SQRT((t.var_glucose_missing + c.var_glucose_missing) / 2.0), 0) AS smd_glucose_missing,

    (t.mean_sodium_missing - c.mean_sodium_missing)
      / NULLIF(SQRT((t.var_sodium_missing + c.var_sodium_missing) / 2.0), 0) AS smd_sodium_missing,

    (t.mean_bun_missing - c.mean_bun_missing)
      / NULLIF(SQRT((t.var_bun_missing + c.var_bun_missing) / 2.0), 0) AS smd_bun_missing

  FROM cem_stats t
  JOIN cem_stats c ON c.treated = 0
  WHERE t.treated = 1
),

cont_moments AS (
  SELECT
    treated,

    SUM(cem_w) FILTER (WHERE creatinine_24h_first IS NOT NULL) AS sw_creat,
    SUM(cem_w * creatinine_24h_first) FILTER (WHERE creatinine_24h_first IS NOT NULL) AS swx_creat,
    SUM(cem_w * creatinine_24h_first * creatinine_24h_first) FILTER (WHERE creatinine_24h_first IS NOT NULL) AS swx2_creat,

    SUM(cem_w) FILTER (WHERE glucose_24h_first IS NOT NULL) AS sw_gluc,
    SUM(cem_w * glucose_24h_first) FILTER (WHERE glucose_24h_first IS NOT NULL) AS swx_gluc,
    SUM(cem_w * glucose_24h_first * glucose_24h_first) FILTER (WHERE glucose_24h_first IS NOT NULL) AS swx2_gluc,

    SUM(cem_w) FILTER (WHERE sodium_24h_first IS NOT NULL) AS sw_sod,
    SUM(cem_w * sodium_24h_first) FILTER (WHERE sodium_24h_first IS NOT NULL) AS swx_sod,
    SUM(cem_w * sodium_24h_first * sodium_24h_first) FILTER (WHERE sodium_24h_first IS NOT NULL) AS swx2_sod,

    SUM(cem_w) FILTER (WHERE bun_24h_first IS NOT NULL) AS sw_bun,
    SUM(cem_w * bun_24h_first) FILTER (WHERE bun_24h_first IS NOT NULL) AS swx_bun,
    SUM(cem_w * bun_24h_first * bun_24h_first) FILTER (WHERE bun_24h_first IS NOT NULL) AS swx2_bun

  FROM cem_weighted
  GROUP BY treated
),
cont_stats AS (
  SELECT
    treated,
    (swx_creat/NULLIF(sw_creat,0)) AS mean_creat,
    (swx2_creat/NULLIF(sw_creat,0)) - POWER(swx_creat/NULLIF(sw_creat,0),2) AS var_creat,

    (swx_gluc/NULLIF(sw_gluc,0)) AS mean_gluc,
    (swx2_gluc/NULLIF(sw_gluc,0)) - POWER(swx_gluc/NULLIF(sw_gluc,0),2) AS var_gluc,

    (swx_sod/NULLIF(sw_sod,0)) AS mean_sod,
    (swx2_sod/NULLIF(sw_sod,0)) - POWER(swx_sod/NULLIF(sw_sod,0),2) AS var_sod,

    (swx_bun/NULLIF(sw_bun,0)) AS mean_bun,
    (swx2_bun/NULLIF(sw_bun,0)) - POWER(swx_bun/NULLIF(sw_bun,0),2) AS var_bun
  FROM cont_moments
),
smd_after_cont AS (
  SELECT
    (t.mean_creat - c.mean_creat) / NULLIF(SQRT((t.var_creat + c.var_creat)/2.0),0) AS smd_creatinine_cont,
    (t.mean_gluc  - c.mean_gluc ) / NULLIF(SQRT((t.var_gluc  + c.var_gluc )/2.0),0) AS smd_glucose_cont,
    (t.mean_sod   - c.mean_sod  ) / NULLIF(SQRT((t.var_sod   + c.var_sod  )/2.0),0) AS smd_sodium_cont,
    (t.mean_bun   - c.mean_bun  ) / NULLIF(SQRT((t.var_bun   + c.var_bun  )/2.0),0) AS smd_bun_cont
  FROM cont_stats t
  JOIN cont_stats c ON c.treated = 0
  WHERE t.treated = 1
)

/*final select*/
SELECT
    'before' AS dataset,
    CASE WHEN cci_score > 5 THEN 'CCI > 5' ELSE 'CCI <= 5' END AS cci_group,

    COUNT(*) AS total_patients,
    COUNT(*)::float AS sum_weights,

    /* RAW headcount event counts */
    COUNT(*) FILTER (WHERE dod IS NOT NULL AND dod <= index_admittime + INTERVAL '30 days')  AS died_30d_n_raw,
    COUNT(*) FILTER (WHERE dod IS NOT NULL AND dod <= index_admittime + INTERVAL '90 days')  AS died_90d_n_raw,
    COUNT(*) FILTER (WHERE dod IS NOT NULL AND dod <= index_admittime + INTERVAL '365 days') AS died_1y_n_raw,
    COUNT(*) FILTER (WHERE died) AS total_died_n_raw,

    (COUNT(*) FILTER (WHERE dod IS NOT NULL AND dod <= index_admittime + INTERVAL '30 days'))::float  AS died_30d_events_w,
    (COUNT(*) FILTER (WHERE dod IS NOT NULL AND dod <= index_admittime + INTERVAL '90 days'))::float  AS died_90d_events_w,
    (COUNT(*) FILTER (WHERE dod IS NOT NULL AND dod <= index_admittime + INTERVAL '365 days'))::float AS died_1y_events_w,
    (COUNT(*) FILTER (WHERE died))::float AS total_died_events_w,

    /* RAW risks */
    (COUNT(*) FILTER (WHERE dod IS NOT NULL AND dod <= index_admittime + INTERVAL '30 days'))::float
      / NULLIF(COUNT(*),0) AS died_30d_risk_raw,
    (COUNT(*) FILTER (WHERE dod IS NOT NULL AND dod <= index_admittime + INTERVAL '90 days'))::float
      / NULLIF(COUNT(*),0) AS died_90d_risk_raw,
    (COUNT(*) FILTER (WHERE dod IS NOT NULL AND dod <= index_admittime + INTERVAL '365 days'))::float
      / NULLIF(COUNT(*),0) AS died_1y_risk_raw,
    (COUNT(*) FILTER (WHERE died))::float
      / NULLIF(COUNT(*),0) AS total_died_risk_raw,

    (COUNT(*) FILTER (WHERE dod IS NOT NULL AND dod <= index_admittime + INTERVAL '30 days'))::float
      / NULLIF(COUNT(*)::float,0) AS died_30d_risk_w,
    (COUNT(*) FILTER (WHERE dod IS NOT NULL AND dod <= index_admittime + INTERVAL '90 days'))::float
      / NULLIF(COUNT(*)::float,0) AS died_90d_risk_w,
    (COUNT(*) FILTER (WHERE dod IS NOT NULL AND dod <= index_admittime + INTERVAL '365 days'))::float
      / NULLIF(COUNT(*)::float,0) AS died_1y_risk_w,
    (COUNT(*) FILTER (WHERE died))::float
      / NULLIF(COUNT(*)::float,0) AS total_died_risk_w,

    /* medelvärde */
    AVG(anchor_age) AS mean_age,
    AVG(creatinine_24h_first) FILTER (WHERE creatinine_24h_first IS NOT NULL) AS mean_creatinine,
    AVG(glucose_24h_first)    FILTER (WHERE glucose_24h_first IS NOT NULL)    AS mean_glucose,
    AVG(sodium_24h_first)     FILTER (WHERE sodium_24h_first IS NOT NULL)     AS mean_sodium,
    AVG(bun_24h_first)        FILTER (WHERE bun_24h_first IS NOT NULL)        AS mean_bun,

    /* andel */
    AVG((gender = 'M')::int) AS prop_male,
    AVG(creat_missing::int)  AS prop_creat_missing,
    AVG(glucose_missing::int) AS prop_glucose_missing,
    AVG(sodium_missing::int)  AS prop_sodium_missing,
    AVG(bun_missing::int)     AS prop_bun_missing,

    /* SMDs before */
    ABS(b.smd_age) AS smd_age,
    ABS(b.smd_male) AS smd_male,
    ABS(b.smd_creat_bin) AS smd_creat_bin,
    ABS(b.smd_glucose_bin) AS smd_glucose_bin,
    ABS(b.smd_sodium_bin) AS smd_sodium_bin,
    ABS(b.smd_bun_bin) AS smd_bun_bin,
    ABS(b.smd_creat_missing) AS smd_creat_missing,
    ABS(b.smd_glucose_missing) AS smd_glucose_missing,
    ABS(b.smd_sodium_missing) AS smd_sodium_missing,
    ABS(b.smd_bun_missing) AS smd_bun_missing,
    ABS(bc.smd_creatinine_cont) AS smd_creatinine_cont,
    ABS(bc.smd_glucose_cont)    AS smd_glucose_cont,
    ABS(bc.smd_sodium_cont)     AS smd_sodium_cont,
    ABS(bc.smd_bun_cont)        AS smd_bun_cont

FROM cohort_banded
CROSS JOIN smd_before b
CROSS JOIN smd_before_cont bc
GROUP BY
    1,2,
    b.smd_age, b.smd_male, b.smd_creat_bin, b.smd_glucose_bin, b.smd_sodium_bin, b.smd_bun_bin,
    b.smd_creat_missing, b.smd_glucose_missing, b.smd_sodium_missing, b.smd_bun_missing,
    bc.smd_creatinine_cont, bc.smd_glucose_cont, bc.smd_sodium_cont, bc.smd_bun_cont

UNION ALL

SELECT
    'after_cem_att_age_gender_labs' AS dataset,
    CASE WHEN treated = 1 THEN 'CCI > 5' ELSE 'CCI <= 5' END AS cci_group,

    COUNT(*) AS total_patients,
    SUM(cem_w) AS sum_weights,

    COUNT(*) FILTER (WHERE dod IS NOT NULL AND dod <= index_admittime + INTERVAL '30 days')  AS died_30d_n_raw,
    COUNT(*) FILTER (WHERE dod IS NOT NULL AND dod <= index_admittime + INTERVAL '90 days')  AS died_90d_n_raw,
    COUNT(*) FILTER (WHERE dod IS NOT NULL AND dod <= index_admittime + INTERVAL '365 days') AS died_1y_n_raw,
    COUNT(*) FILTER (WHERE died) AS total_died_n_raw,

    SUM(cem_w * (dod IS NOT NULL AND dod <= index_admittime + INTERVAL '30 days')::int)  AS died_30d_events_w,
    SUM(cem_w * (dod IS NOT NULL AND dod <= index_admittime + INTERVAL '90 days')::int)  AS died_90d_events_w,
    SUM(cem_w * (dod IS NOT NULL AND dod <= index_admittime + INTERVAL '365 days')::int) AS died_1y_events_w,
    SUM(cem_w * (died::int)) AS total_died_events_w,

    /* RAW risks (descriptive) */
    (COUNT(*) FILTER (WHERE dod IS NOT NULL AND dod <= index_admittime + INTERVAL '30 days'))::float
      / NULLIF(COUNT(*),0) AS died_30d_risk_raw,
    (COUNT(*) FILTER (WHERE dod IS NOT NULL AND dod <= index_admittime + INTERVAL '90 days'))::float
      / NULLIF(COUNT(*),0) AS died_90d_risk_raw,
    (COUNT(*) FILTER (WHERE dod IS NOT NULL AND dod <= index_admittime + INTERVAL '365 days'))::float
      / NULLIF(COUNT(*),0) AS died_1y_risk_raw,
    (COUNT(*) FILTER (WHERE died))::float
      / NULLIF(COUNT(*),0) AS total_died_risk_raw,

    /* WEIGHTED risks (TRUE CEM ATT) */
    SUM(cem_w * (dod IS NOT NULL AND dod <= index_admittime + INTERVAL '30 days')::int)
      / NULLIF(SUM(cem_w), 0) AS died_30d_risk_w,
    SUM(cem_w * (dod IS NOT NULL AND dod <= index_admittime + INTERVAL '90 days')::int)
      / NULLIF(SUM(cem_w), 0) AS died_90d_risk_w,
    SUM(cem_w * (dod IS NOT NULL AND dod <= index_admittime + INTERVAL '365 days')::int)
      / NULLIF(SUM(cem_w), 0) AS died_1y_risk_w,
    SUM(cem_w * (died::int))
      / NULLIF(SUM(cem_w), 0) AS total_died_risk_w,

    /* means (WEIGHTED) */
    SUM(cem_w * anchor_age) / NULLIF(SUM(cem_w),0) AS mean_age,
    SUM(cem_w * creatinine_24h_first) FILTER (WHERE creatinine_24h_first IS NOT NULL)
      / NULLIF(SUM(cem_w) FILTER (WHERE creatinine_24h_first IS NOT NULL),0) AS mean_creatinine,
    SUM(cem_w * glucose_24h_first) FILTER (WHERE glucose_24h_first IS NOT NULL)
      / NULLIF(SUM(cem_w) FILTER (WHERE glucose_24h_first IS NOT NULL),0) AS mean_glucose,
    SUM(cem_w * sodium_24h_first) FILTER (WHERE sodium_24h_first IS NOT NULL)
      / NULLIF(SUM(cem_w) FILTER (WHERE sodium_24h_first IS NOT NULL),0) AS mean_sodium,
    SUM(cem_w * bun_24h_first) FILTER (WHERE bun_24h_first IS NOT NULL)
      / NULLIF(SUM(cem_w) FILTER (WHERE bun_24h_first IS NOT NULL),0) AS mean_bun,

    /* proportions (WEIGHTED) */
    SUM(cem_w * ((gender='M')::int)) / NULLIF(SUM(cem_w),0) AS prop_male,
    SUM(cem_w * (creat_missing::int)) / NULLIF(SUM(cem_w),0) AS prop_creat_missing,
    SUM(cem_w * (glucose_missing::int)) / NULLIF(SUM(cem_w),0) AS prop_glucose_missing,
    SUM(cem_w * (sodium_missing::int)) / NULLIF(SUM(cem_w),0) AS prop_sodium_missing,
    SUM(cem_w * (bun_missing::int)) / NULLIF(SUM(cem_w),0) AS prop_bun_missing,

    /* SMDs after (weighted) */
    ABS(a.smd_age) AS smd_age,
    ABS(a.smd_male) AS smd_male,
    ABS(a.smd_creat_bin) AS smd_creat_bin,
    ABS(a.smd_glucose_bin) AS smd_glucose_bin,
    ABS(a.smd_sodium_bin) AS smd_sodium_bin,
    ABS(a.smd_bun_bin) AS smd_bun_bin,
    ABS(a.smd_creat_missing) AS smd_creat_missing,
    ABS(a.smd_glucose_missing) AS smd_glucose_missing,
    ABS(a.smd_sodium_missing) AS smd_sodium_missing,
    ABS(a.smd_bun_missing) AS smd_bun_missing,
    ABS(ac.smd_creatinine_cont) AS smd_creatinine_cont,
    ABS(ac.smd_glucose_cont)    AS smd_glucose_cont,
    ABS(ac.smd_sodium_cont)     AS smd_sodium_cont,
    ABS(ac.smd_bun_cont)        AS smd_bun_cont

FROM cem_weighted
CROSS JOIN smd_after a
CROSS JOIN smd_after_cont ac
GROUP BY
    1,2,
    a.smd_age, a.smd_male, a.smd_creat_bin, a.smd_glucose_bin, a.smd_sodium_bin, a.smd_bun_bin,
    a.smd_creat_missing, a.smd_glucose_missing, a.smd_sodium_missing, a.smd_bun_missing,
    ac.smd_creatinine_cont, ac.smd_glucose_cont, ac.smd_sodium_cont, ac.smd_bun_cont;





