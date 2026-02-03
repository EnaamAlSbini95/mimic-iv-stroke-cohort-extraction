WITH stroke_events AS (
    SELECT
        d.subject_id,
        d.hadm_id,
        d.seq_num,
        a.admittime,
        ROW_NUMBER() OVER (
            PARTITION BY d.subject_id
            ORDER BY a.admittime ASC, d.seq_num ASC
        ) AS rn
    FROM mimiciv_hosp.diagnoses_icd d
    JOIN mimiciv_hosp.admissions a
      ON d.hadm_id = a.hadm_id
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

/* FULL CCI MAP (unchanged logic) */
cci_icd10_map (
	condition,
    condition_family,
    weight,
    icd10_code
	) as (
SELECT 'Acute myocardial infarction','Acute myocardial infarction',1,'I21' UNION ALL
    SELECT 'Acute myocardial infarction','Acute myocardial infarction',1,'I22' UNION ALL
    SELECT 'Acute myocardial infarction','Acute myocardial infarction',1,'I252' UNION ALL
    SELECT 'Congestive heart failure','Congestive heart failure',1,'I50' UNION ALL
    SELECT 'Peripheral vascular disease','Peripheral vascular disease',1,'I71' UNION ALL
    SELECT 'Peripheral vascular disease','Peripheral vascular disease',1,'I790' UNION ALL
    SELECT 'Peripheral vascular disease','Peripheral vascular disease',1,'I739' UNION ALL
    SELECT 'Peripheral vascular disease','Peripheral vascular disease',1,'R02' UNION ALL
    SELECT 'Peripheral vascular disease','Peripheral vascular disease',1,'Z958' UNION ALL
    SELECT 'Peripheral vascular disease','Peripheral vascular disease',1,'Z959' UNION ALL
    SELECT 'Pulmonary disease','Pulmonary disease',1,'J40' UNION ALL
    SELECT 'Pulmonary disease','Pulmonary disease',1,'J41' UNION ALL
    SELECT 'Pulmonary disease','Pulmonary disease',1,'J42' UNION ALL
    SELECT 'Pulmonary disease','Pulmonary disease',1,'J43' UNION ALL
    SELECT 'Pulmonary disease','Pulmonary disease',1,'J44' UNION ALL
    SELECT 'Pulmonary disease','Pulmonary disease',1,'J45' UNION ALL
    SELECT 'Pulmonary disease','Pulmonary disease',1,'J46' UNION ALL
    SELECT 'Pulmonary disease','Pulmonary disease',1,'J47' UNION ALL
    SELECT 'Pulmonary disease','Pulmonary disease',1,'J60' UNION ALL
    SELECT 'Pulmonary disease','Pulmonary disease',1,'J61' UNION ALL
    SELECT 'Pulmonary disease','Pulmonary disease',1,'J62' UNION ALL
    SELECT 'Pulmonary disease','Pulmonary disease',1,'J63' UNION ALL
    SELECT 'Pulmonary disease','Pulmonary disease',1,'J64' UNION ALL
    SELECT 'Pulmonary disease','Pulmonary disease',1,'J65' UNION ALL
    SELECT 'Pulmonary disease','Pulmonary disease',1,'J66' UNION ALL
    SELECT 'Pulmonary disease','Pulmonary disease',1,'J67' UNION ALL
    SELECT 'Connective-tissue disorder','Connective-tissue disorder',1,'M32' UNION ALL
    SELECT 'Connective-tissue disorder','Connective-tissue disorder',1,'M34' UNION ALL
    SELECT 'Connective-tissue disorder','Connective-tissue disorder',1,'M050' UNION ALL
    SELECT 'Connective-tissue disorder','Connective-tissue disorder',1,'M051' UNION ALL
    SELECT 'Connective-tissue disorder','Connective-tissue disorder',1,'M052' UNION ALL
    SELECT 'Connective-tissue disorder','Connective-tissue disorder',1,'M053' UNION ALL
    SELECT 'Connective-tissue disorder','Connective-tissue disorder',1,'M058' UNION ALL
    SELECT 'Connective-tissue disorder','Connective-tissue disorder',1,'M059' UNION ALL
    SELECT 'Connective-tissue disorder','Connective-tissue disorder',1,'M060' UNION ALL
    SELECT 'Connective-tissue disorder','Connective-tissue disorder',1,'M063' UNION ALL
    SELECT 'Connective-tissue disorder','Connective-tissue disorder',1,'M069' UNION ALL
    SELECT 'Connective-tissue disorder','Connective-tissue disorder',1,'M332' UNION ALL
    SELECT 'Connective-tissue disorder','Connective-tissue disorder',1,'M353' UNION ALL
    SELECT 'Peptic ulcer','Peptic ulcer',1,'K25' UNION ALL
    SELECT 'Peptic ulcer','Peptic ulcer',1,'K26' UNION ALL
    SELECT 'Peptic ulcer','Peptic ulcer',1,'K27' UNION ALL
    SELECT 'Peptic ulcer','Peptic ulcer',1,'K28' UNION ALL
    SELECT 'Mild liver disease','Liver disease',1,'K702' UNION ALL
    SELECT 'Mild liver disease','Liver disease',1,'K703' UNION ALL
    SELECT 'Mild liver disease','Liver disease',1,'K73' UNION ALL
    SELECT 'Mild liver disease','Liver disease',1,'K717' UNION ALL
    SELECT 'Mild liver disease','Liver disease',1,'K740' UNION ALL
    SELECT 'Mild liver disease','Liver disease',1,'K742' UNION ALL
    SELECT 'Mild liver disease','Liver disease',1,'K743' UNION ALL
    SELECT 'Mild liver disease','Liver disease',1,'K744' UNION ALL
    SELECT 'Mild liver disease','Liver disease',1,'K745' UNION ALL
    SELECT 'Mild liver disease','Liver disease',1,'K746' UNION ALL
    SELECT 'Diabetes','Diabetes',1,'E109' UNION ALL
    SELECT 'Diabetes','Diabetes',1,'E119' UNION ALL
    SELECT 'Diabetes','Diabetes',1,'E139' UNION ALL
    SELECT 'Diabetes','Diabetes',1,'E149' UNION ALL
    SELECT 'Diabetes','Diabetes',1,'E101' UNION ALL
    SELECT 'Diabetes','Diabetes',1,'E111' UNION ALL
    SELECT 'Diabetes','Diabetes',1,'E131' UNION ALL
    SELECT 'Diabetes','Diabetes',1,'E141' UNION ALL
    SELECT 'Diabetes','Diabetes',1,'E105' UNION ALL
    SELECT 'Diabetes','Diabetes',1,'E115' UNION ALL
    SELECT 'Diabetes','Diabetes',1,'E135' UNION ALL
    SELECT 'Diabetes','Diabetes',1,'E145' UNION ALL
    SELECT 'Diabetes with complications','Diabetes',2,'E102' UNION ALL
    SELECT 'Diabetes with complications','Diabetes',2,'E112' UNION ALL
    SELECT 'Diabetes with complications','Diabetes',2,'E132' UNION ALL
    SELECT 'Diabetes with complications','Diabetes',2,'E142' UNION ALL
    SELECT 'Diabetes with complications','Diabetes',2,'E103' UNION ALL
    SELECT 'Diabetes with complications','Diabetes',2,'E113' UNION ALL
    SELECT 'Diabetes with complications','Diabetes',2,'E133' UNION ALL
    SELECT 'Diabetes with complications','Diabetes',2,'E143' UNION ALL
    SELECT 'Diabetes with complications','Diabetes',2,'E104' UNION ALL
    SELECT 'Diabetes with complications','Diabetes',2,'E114' UNION ALL
    SELECT 'Diabetes with complications','Diabetes',2,'E134' UNION ALL
    SELECT 'Diabetes with complications','Diabetes',2,'E144' UNION ALL
    SELECT 'Paraplegia','Paraplegia',2,'G81' UNION ALL
    SELECT 'Paraplegia','Paraplegia',2,'G041' UNION ALL
    SELECT 'Paraplegia','Paraplegia',2,'G820' UNION ALL
    SELECT 'Paraplegia','Paraplegia',2,'G821' UNION ALL
    SELECT 'Paraplegia','Paraplegia',2,'G822' UNION ALL
    SELECT 'Renal disease','Renal disease',2,'N03' UNION ALL
    SELECT 'Renal disease','Renal disease',2,'N052' UNION ALL
    SELECT 'Renal disease','Renal disease',2,'N053' UNION ALL
    SELECT 'Renal disease','Renal disease',2,'N054' UNION ALL
    SELECT 'Renal disease','Renal disease',2,'N055' UNION ALL
    SELECT 'Renal disease','Renal disease',2,'N056' UNION ALL
    SELECT 'Renal disease','Renal disease',2,'N072' UNION ALL
    SELECT 'Renal disease','Renal disease',2,'N073' UNION ALL
    SELECT 'Renal disease','Renal disease',2,'N074' UNION ALL
    SELECT 'Renal disease','Renal disease',2,'N18' UNION ALL
    SELECT 'Renal disease','Renal disease',2,'N19' UNION ALL
    SELECT 'Renal disease','Renal disease',2,'N25' UNION ALL
    SELECT 'Cancer','Cancer',2,'C0' UNION ALL
    SELECT 'Cancer','Cancer',2,'C1' UNION ALL
    SELECT 'Cancer','Cancer',2,'C2' UNION ALL
    SELECT 'Cancer','Cancer',2,'C3' UNION ALL
    SELECT 'Cancer','Cancer',2,'C4' UNION ALL
    SELECT 'Cancer','Cancer',2,'C5' UNION ALL
    SELECT 'Cancer','Cancer',2,'C6' UNION ALL
    SELECT 'Cancer','Cancer',2,'C7' UNION ALL
    SELECT 'Cancer','Cancer',2,'C8' UNION ALL
    SELECT 'Cancer','Cancer',2,'C9' UNION ALL
    SELECT 'Metastatic cancer','Cancer',3,'C77' UNION ALL
    SELECT 'Metastatic cancer','Cancer',3,'C78' UNION ALL
    SELECT 'Metastatic cancer','Cancer',3,'C79' UNION ALL
    SELECT 'Severe liver disease','Liver disease',3,'K721' UNION ALL
    SELECT 'Severe liver disease','Liver disease',3,'K729' UNION ALL
    SELECT 'Severe liver disease','Liver disease',3,'K766' UNION ALL
    SELECT 'Severe liver disease','Liver disease',3,'K767' UNION ALL
    SELECT 'HIV','HIV',6,'B20' UNION ALL
    SELECT 'HIV','HIV',6,'B21' UNION ALL
    SELECT 'HIV','HIV',6,'B22' UNION ALL
    SELECT 'HIV','HIV',6,'B23' UNION ALL
    SELECT 'HIV','HIV',6,'B24'
    
),

cci_raw AS (
    SELECT
        s.subject_id,
        m.condition_family,
        m.weight
    FROM index_stroke s
    JOIN mimiciv_hosp.admissions a
      ON a.subject_id = s.subject_id
    JOIN mimiciv_hosp.diagnoses_icd d
      ON d.hadm_id = a.hadm_id
     AND d.subject_id = s.subject_id   -- FIX: enforce subject consistency
     AND d.icd_version = 10
    JOIN cci_icd10_map m
      ON d.icd_code LIKE m.icd10_code || '%'
    WHERE a.admittime <= s.index_admittime
),


cci_per_family AS (
    SELECT
        subject_id,
        MAX(weight) AS family_weight
    FROM cci_raw
    GROUP BY subject_id, condition_family
),

cci_score AS (
    SELECT
        subject_id,
        SUM(family_weight) AS cci_score
    FROM cci_per_family
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

        CASE
            WHEN p.dod IS NOT NULL
             AND p.dod <= s.index_admittime + INTERVAL '30 days'
            THEN 1 ELSE 0
        END AS died_30d,

        CASE
            WHEN p.dod IS NOT NULL
             AND p.dod <= s.index_admittime + INTERVAL '90 days'
            THEN 1 ELSE 0
        END AS died_90d,

        CASE
            WHEN p.dod IS NOT NULL
             AND p.dod <= s.index_admittime + INTERVAL '365 days'
            THEN 1 ELSE 0
        END AS died_1y,

        COALESCE(c.cci_score, 0) AS cci_score,

        CASE
            WHEN COALESCE(c.cci_score, 0) > 5 THEN 1
            ELSE 0
        END AS high_cci

    FROM index_stroke s
    INNER JOIN mimiciv_hosp.patients p
        ON p.subject_id = s.subject_id
    LEFT JOIN cci_score c
        ON c.subject_id = s.subject_id
    WHERE p.anchor_age >= 18
)


SELECT
    subject_id,
    index_hadm_id AS hadm_id,
    index_admittime AS admittime,
    gender,
    anchor_age,
    dod,
    died,
    died_30d,
    died_90d,
    died_1y,
    cci_score,
    high_cci
FROM cohort
ORDER BY index_admittime;

