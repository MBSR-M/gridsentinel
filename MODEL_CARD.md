# Recovery probability models

GridSentinel implements an explainable, standard-library recovery model. Its
target is **observed communication recovery within 6, 12 or 24 hours** after a
point-in-time snapshot. Operator intervention can contribute to observed
recovery. The model never describes that prediction as spontaneous recovery.

The repository includes seeded synthetic data for verifying algorithms and
workflows. Scores measured on those generated episodes do not establish real
AMI accuracy or operational usefulness. Customer episodes and approved outcome
collection are required before a pilot can evaluate dispatch decisions.

## Training data and feature definitions

Training accepts a bounded JSON dataset through an authorized model-training job.
Each episode requires a unique episode and meter ID, prediction time, feature
observation time, follow-up time, recovery time when observed, communication
stratum, intervention status when known, and exactly these seven numeric features:

| Feature | Meaning |
| --- | --- |
| outage_duration_hours | Duration of the operational outage at prediction time |
| failures_last_24h | Observed failed communications in the preceding day |
| recoveries_last_30d | Observed transitions from failure to success in retained history |
| gateway_failure_ratio | Failed meter fraction in the historical correlation window |
| shared_outage_size | Failed meter count in that window |
| power_alarm_present | Explicit current power alarm or restoration observation |
| hour_of_day | UTC prediction hour, zero through 23 |

The schema is `recovery-1.0`. Feature observation timestamps cannot exceed the
prediction time. Ongoing episodes without follow-up through the prediction
horizon are right-censored and excluded from binary negative labels. A recovery
observed within the horizon can be positively labelled before that horizon ends.
Repeated episode IDs are rejected. Label quality and intervention ascertainment
remain responsibilities of the approved source dataset.

Episodes are split by timestamp cohorts, approximately 60% training, 20%
validation and 20% test. Samples whose outcomes were not yet knowable at the next
cohort boundary are purged. Scaling is fitted only on training data. Evaluation
includes per-communication-stratum holdouts. A vendor/region holdout requires
those cohort definitions in a customer evaluation dataset; the MVP does not
claim that evaluation from generated data.

## Algorithms and evaluation

The first baseline is a Laplace-smoothed historical recovery rate by communication
stratum, with an observed global rate for unseen strata. The candidate is a
seven-dimensional logistic regression fitted with deterministic mini-batch
gradient descent, L2 regularization and bounded standardized features. Both
classes must appear in the training cohort.

Stored validation and test metrics include precision, recall, F1, confusion
counts, Brier score, ten probability-calibration bins and false positives per
1,000 **observations**. That last denominator must not be presented as a unique
meter rate. Detection delay, dispatch avoidance, investigation time and confirmed
top-three RCA accuracy require operator outcome studies and are not supplied as
invented metrics. A logistic candidate worse than its baseline on validation
Brier score uses the historical baseline during inference.

## Integrity and deployment

Artifacts contain JSON coefficients, training-only scaling, feature version,
historical baseline, provenance, metrics and bounded drift-reference samples.
No pickle or executable object deserialization is used. Native MySQL JSON can
normalize numerical representation: creation validates the authoritative stored
JSON, computes its canonical SHA-256 digest once, and records that digest in the
same transaction. Later loads must match the recorded digest and supported
schema and must contain finite, correctly shaped parameters.

Models enter the registry in `PENDING` status. Approval requires
`models.approve`, a rationale and a durable audit entry. Activation requires
`models.activate`, approved status and a valid artifact. A tenant deployment row
and tenant lock serialize activation and preserve the previous model for
rollback. Rabbit job redelivery uses the training-run UUID to avoid publishing a
second model from the same successful job.

`infer_current` builds features from facts known at the scoring time. It declines
to produce a probability when required gateway population or power evidence is
unknown. No approved active model or a corrupt artifact returns the deterministic
operations fallback with an explicit reason and no invented probability. Current
inference is rate-limited per meter by the configured prediction interval; this
is a bounded pilot workflow, not a benchmarked 10M-meter inference schedule.

Feature monitoring persists fixed-reference population stability indices (PSI).
PSI is a distribution-change indicator, not proof of predictive degradation.
Calibration drift and outcome monitoring require complete real follow-up data.

## Verified scope and limitations

Unit tests verify censoring, future feature rejection, chronological splits,
deterministic fitting, baseline fallback, calibration, checksum corruption and
PSI. Live MySQL tests verify training-run redelivery, pending-model activation
denial, audited approval/activation, persistent predictions and corrupt-artifact
fallback. These checks establish implementation behavior on synthetic data.

Kaplan–Meier survival estimation, supervised multiclass RCA, shadow/canary traffic
routing, automatic customer dataset extraction and performance at fleet scale
are not implemented in this pilot. RCA is explicit evidence scoring with human
confirmation. Customer-specific SLA calculations require an approved contract;
model outputs do not replace that contract.
