"""Bounded logistic gradient descent and stratified historical baselines."""
from __future__ import annotations
from dataclasses import dataclass
import hashlib
import json
import math
import random
from typing import Any
from gridsentinel.core.exceptions import ValidationError
from gridsentinel.domain.events import canonical_json
from gridsentinel.ml.datasets import FEATURES, RecoveryEpisode

def sigmoid(value: float) -> float:
    """Stable logistic transform without exponential overflow."""
    if value >= 0:
        return 1 / (1 + math.exp(-value))
    z = math.exp(value)
    return z / (1 + z)

@dataclass(frozen=True)
class LogisticModel:
    """Compact explainable numerical model with training-only scaling."""
    coefficients: tuple[float, ...]
    intercept: float
    means: tuple[float, ...]
    scales: tuple[float, ...]

    @classmethod
    def fit(cls, rows: list[tuple[RecoveryEpisode, int]], epochs: int=150, learning_rate: float=0.05, l2: float=0.001, seed: int=42) -> LogisticModel:
        """Fit deterministic mini-batch SGD only when both labels are observed."""
        if {label for _, label in rows} != {0, 1} or not 1 <= epochs <= 2000 or (not 0 < learning_rate <= 1) or (not 0 <= l2 <= 1):
            raise ValidationError('Logistic training requires both labels and valid hyperparameters')
        count = len(rows)
        dimension = len(FEATURES)
        means = tuple((sum((row.features[i] for row, _ in rows)) / count for i in range(dimension)))
        scales = tuple((max(math.sqrt(sum(((row.features[i] - means[i]) ** 2 for row, _ in rows)) / count), 1e-06) for i in range(dimension)))
        vectors = [(tuple((max(-20, min(20, (row.features[i] - means[i]) / scales[i])) for i in range(dimension))), label) for row, label in rows]
        weights = [0.0] * dimension
        intercept = 0.0
        rng = random.Random(seed)
        for _ in range(epochs):
            rng.shuffle(vectors)
            for start in range(0, count, 64):
                batch = vectors[start:start + 64]
                grad = [0.0] * dimension
                bias = 0.0
                for features, label in batch:
                    error = sigmoid(intercept + sum((weight * value for weight, value in zip(weights, features)))) - label
                    bias += error
                    for i, value in enumerate(features):
                        grad[i] += error * value
                for i in range(dimension):
                    weights[i] -= learning_rate * (grad[i] / len(batch) + l2 * weights[i])
                intercept -= learning_rate * bias / len(batch)
        return cls(tuple(weights), intercept, means, scales)

    def predict(self, features: tuple[float, ...]) -> float:
        """Score finite, fixed-shape features with bounded scaled contributions."""
        if len(features) != len(FEATURES) or not all((math.isfinite(v) for v in features)):
            raise ValidationError('Inference feature schema mismatch')
        return sigmoid(self.intercept + sum((w * max(-20, min(20, (v - m) / s)) for w, v, m, s in zip(self.coefficients, features, self.means, self.scales))))

    def artifact(self) -> dict[str, Any]:
        """Return safe numerical JSON, never pickle."""
        return {'coefficients': list(self.coefficients), 'intercept': self.intercept, 'means': list(self.means), 'scales': list(self.scales)}

@dataclass(frozen=True)
class HistoricalBaseline:
    """Laplace-smoothed observed recovery rates by communication stratum."""
    rates: dict[str, float]
    global_rate: float
    counts: dict[str, int]

    @classmethod
    def fit(cls, rows: list[tuple[RecoveryEpisode, int]]) -> HistoricalBaseline:
        """Fit solely on historical training cohorts."""
        counts = {}
        successes = {}
        for row, label in rows:
            counts[row.stratum] = counts.get(row.stratum, 0) + 1
            successes[row.stratum] = successes.get(row.stratum, 0) + label
        return cls({key: (successes[key] + 1) / (count + 2) for key, count in counts.items()}, (sum((label for _, label in rows)) + 1) / (len(rows) + 2), counts)

    def predict(self, stratum: str) -> float:
        """Use the global observed baseline for an unseen stratum."""
        return self.rates.get(stratum, self.global_rate)

    def artifact(self) -> dict[str, Any]:
        """Serialize learned counts and rates."""
        return {'rates': self.rates, 'global_rate': self.global_rate, 'counts': self.counts}

def classification_metrics(labels: list[int], probabilities: list[float]) -> dict[str, Any]:
    """Evaluate confusion counts, Brier score and empirical calibration bins."""
    if not labels or len(labels) != len(probabilities) or any((not math.isfinite(p) or not 0 <= p <= 1 for p in probabilities)):
        raise ValidationError('Evaluation requires paired labels and bounded probabilities')
    tp = fp = tn = fn = 0
    for label, p in zip(labels, probabilities):
        if p >= 0.5:
            tp += label == 1
            fp += label == 0
        else:
            tn += label == 0
            fn += label == 1
    precision = tp / (tp + fp) if tp + fp else 0
    recall = tp / (tp + fn) if tp + fn else 0
    bins = []
    for i in range(10):
        pairs = [(y, p) for y, p in zip(labels, probabilities) if min(9, int(p * 10)) == i]
        if pairs:
            bins.append({'lower': i / 10, 'count': len(pairs), 'mean_probability': sum((p for _, p in pairs)) / len(pairs), 'observed_rate': sum((y for y, _ in pairs)) / len(pairs)})
    return {'sample_count': len(labels), 'precision': precision, 'recall': recall, 'f1': 2 * precision * recall / (precision + recall) if precision + recall else 0, 'confusion_matrix': {'tp': tp, 'fp': fp, 'tn': tn, 'fn': fn}, 'brier_score': sum(((p - y) ** 2 for y, p in zip(labels, probabilities))) / len(labels), 'calibration': bins, 'false_alerts_per_1000_observations': fp / len(labels) * 1000}

def load_artifact(serialized: str | dict[str, Any], checksum: str) -> dict[str, Any]:
    """Verify checksum, version, shape and finite coefficients before inference."""
    try:
        data = json.loads(serialized) if isinstance(serialized, str) else serialized
        if hashlib.sha256(canonical_json(data).encode()).hexdigest() != checksum:
            raise ValidationError('Model artifact checksum mismatch')
        if data['artifact_format_version'] != '1.0' or data['feature_schema_version'] != 'recovery-1.0' or data['features'] != list(FEATURES):
            raise ValidationError('Unsupported model artifact schema')
        parameters = data['parameters']
        for name in ('coefficients', 'means', 'scales'):
            values = parameters[name]
            if len(values) != len(FEATURES) or any((type(v) not in (int, float) or not math.isfinite(v) for v in values)):
                raise ValidationError('Invalid model numerical parameters')
        if any((v <= 0 for v in parameters['scales'])) or not math.isfinite(parameters['intercept']):
            raise ValidationError('Invalid model scaling or intercept')
        baseline = data['baseline']
        if not 0 <= baseline['global_rate'] <= 1 or not all((0 <= v <= 1 for v in baseline['rates'].values())):
            raise ValidationError('Invalid historical baseline')
        return data
    except (KeyError, TypeError, ValueError, OverflowError) as error:
        raise ValidationError('Model artifact schema validation failed') from error

def population_stability(reference: list[float], observed: list[float], bins: int=10) -> float:
    """Compute fixed-boundary PSI as a drift indicator, not a performance proof."""
    if not reference or not observed or (not all((math.isfinite(v) for v in reference + observed))):
        raise ValidationError('Drift requires finite paired observation populations')
    low, high = (min(reference), max(reference))
    width = (high - low) / bins if high > low else 1

    def distribution(values: list[float]) -> list[float]:
        counts = [0] * bins
        for value in values:
            counts[max(0, min(bins - 1, int((value - low) / width)))] += 1
        return [max(count / len(values), 1e-06) for count in counts]
    expected, actual = (distribution(reference), distribution(observed))
    return sum(((a - e) * math.log(a / e) for e, a in zip(expected, actual)))
