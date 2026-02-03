WITH stroke_events AS (
    SELECT
        d.subject_id,
        d.hadm_id,
        a.admittime,
        ROW_NUMBER() OVER (
            PARTITION BY d.subject_id
            ORDER BY a.admittime ASC, d.seq_num ASC
        ) AS rn
    FROM mimiciv_hosp.diagnoses_icd d
    INNER JOIN mimiciv_hosp.admissions a
        ON d.hadm_id = a.hadm_id
    WHERE d.icd_code LIKE 'I63%' AND d.icd_version = 10
),
index_stroke AS (
    SELECT subject_id, hadm_id, admittime
    FROM stroke_events
    WHERE rn = 1
),

-- Demografi: ålder och kön (MIMIC-IV: gender i patients, anchor_age i admissions)
demo AS (
    SELECT
        s.subject_id,
        s.hadm_id,
        p.gender,
        p.anchor_age
    FROM index_stroke s
    INNER JOIN mimiciv_hosp.patients p
        ON p.subject_id = s.subject_id
),

labs_raw AS (
    SELECT
        s.subject_id,
        s.hadm_id,
        lab.itemid,
        lab.charttime,
        lab.valuenum
    FROM index_stroke s
    INNER JOIN mimiciv_hosp.labevents lab
        ON lab.hadm_id = s.hadm_id
       AND lab.charttime >= s.admittime
       AND lab.charttime <  s.admittime + INTERVAL '24 hour'
       AND lab.valuenum IS NOT NULL
       AND lab.itemid IN (50912, 50931, 50983, 51006)  -- creatinine, glucose, sodium, BUN
),

labs_first AS (
    SELECT
        subject_id,
        hadm_id,

        (ARRAY_AGG(valuenum ORDER BY charttime) FILTER (WHERE itemid = 50912))[1] AS creatinine_first,
        (ARRAY_AGG(valuenum ORDER BY charttime) FILTER (WHERE itemid = 50931))[1] AS glucose_first,
        (ARRAY_AGG(valuenum ORDER BY charttime) FILTER (WHERE itemid = 50983))[1] AS sodium_first,
        (ARRAY_AGG(valuenum ORDER BY charttime) FILTER (WHERE itemid = 51006))[1] AS bun_first,

        MAX(CASE WHEN itemid = 50912 THEN 1 ELSE 0 END) AS has_creatinine,
        MAX(CASE WHEN itemid = 50931 THEN 1 ELSE 0 END) AS has_glucose,
        MAX(CASE WHEN itemid = 50983 THEN 1 ELSE 0 END) AS has_sodium,
        MAX(CASE WHEN itemid = 51006 THEN 1 ELSE 0 END) AS has_bun

    FROM labs_raw
    GROUP BY subject_id, hadm_id
)

SELECT
    s.subject_id,
    s.hadm_id,
    s.admittime,

    d.anchor_age,
    d.gender,

    lf.creatinine_first,
    lf.glucose_first,
    lf.sodium_first,
    lf.bun_first,

    (lf.bun_first / NULLIF(lf.creatinine_first, 0)) AS bun_cr_ratio_first,

    lf.has_creatinine,
    lf.has_glucose,
    lf.has_sodium,
    lf.has_bun

FROM index_stroke s
INNER JOIN demo d
  ON d.subject_id = s.subject_id
 AND d.hadm_id    = s.hadm_id
LEFT JOIN labs_first lf
  ON lf.subject_id = s.subject_id
 AND lf.hadm_id    = s.hadm_id;
