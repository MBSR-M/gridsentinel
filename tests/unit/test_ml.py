"""Temporal labelling, deterministic models and untrusted artifact boundaries."""
from datetime import datetime,timedelta,timezone
import hashlib
from pathlib import Path
import sys
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/"scripts"))
from generate_meter_data import iter_episodes
from gridsentinel.core.exceptions import ValidationError
from gridsentinel.domain.events import canonical_json
from gridsentinel.ml.datasets import FEATURES,RecoveryEpisode,build_labels,temporal_split
from gridsentinel.ml.algorithms import LogisticModel,HistoricalBaseline,classification_metrics,load_artifact,population_stability


class MLTests(unittest.TestCase):
    def episodes(self) -> list:
        return [RecoveryEpisode.from_dict(row) for row in iter_episodes(100)]

    def test_censoring_not_negative(self) -> None:
        labelled,censored=build_labels(self.episodes(),12)
        self.assertGreater(censored,0);self.assertEqual(len(labelled)+censored,100)
        self.assertNotIn("SYN-EP-42-0",[row.episode_id for row,_ in labelled])

    def test_future_features_rejected(self) -> None:
        row=next(iter_episodes());row["feature_observed_at"]="2030-01-01T00:00:00Z"
        with self.assertRaises(ValidationError):RecoveryEpisode.from_dict(row)

    def test_temporal_cohorts_and_duplicate_episodes(self) -> None:
        rows,_=build_labels(self.episodes(),12);train,validation,test=temporal_split(rows)
        self.assertLess(max(row.as_of for row,_ in train),min(row.as_of for row,_ in validation))
        self.assertLess(max(row.as_of for row,_ in validation),min(row.as_of for row,_ in test))
        with self.assertRaises(ValidationError):build_labels([self.episodes()[0]]*2,12)

    def test_logistic_fit_deterministic_and_predictive(self) -> None:
        rows,_=build_labels(self.episodes(),12);train,validation,test=temporal_split(rows)
        model=LogisticModel.fit(train,epochs=50);other=LogisticModel.fit(train,epochs=50)
        self.assertEqual(model,other)
        metrics=classification_metrics([label for _,label in test],[model.predict(row.features) for row,_ in test])
        self.assertLess(metrics["brier_score"],.25)
        self.assertEqual(sum(metrics["confusion_matrix"].values()),len(test))

    def test_baseline_unseen_stratum_and_calibration(self) -> None:
        rows,_=build_labels(self.episodes(),12);baseline=HistoricalBaseline.fit(rows)
        self.assertEqual(baseline.predict("UNSEEN"),baseline.global_rate)
        metrics=classification_metrics([0,1],[.1,.9]);self.assertEqual(metrics["f1"],1)
        self.assertEqual(len(metrics["calibration"]),2)

    def test_artifact_checksum_schema_and_finite_parameters(self) -> None:
        rows,_=build_labels(self.episodes(),12);model=LogisticModel.fit(rows,epochs=5)
        data={"artifact_format_version":"1.0","feature_schema_version":"recovery-1.0","features":list(FEATURES),"parameters":model.artifact(),"baseline":HistoricalBaseline.fit(rows).artifact()}
        checksum=hashlib.sha256(canonical_json(data).encode()).hexdigest();self.assertEqual(load_artifact(data,checksum),data)
        data["parameters"]["intercept"]+=1
        with self.assertRaises(ValidationError):load_artifact(data,checksum)

    def test_population_drift(self) -> None:
        reference=[i/100 for i in range(100)]
        self.assertAlmostEqual(population_stability(reference,reference),0)
        self.assertGreater(population_stability(reference,[10]*100),.2)
