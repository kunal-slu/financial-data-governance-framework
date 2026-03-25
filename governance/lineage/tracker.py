"""
governance/lineage/tracker.py

End-to-end data lineage tracking for U.S. regulatory reporting pipelines.
OpenLineage-style. Aligned with common lineage and traceability control themes
used in BCBS 239-style and machine-readable reporting workflows.

Every dataset transformation, join, aggregation, and output handoff is recorded
with generated fingerprints and run artifacts, replacing manual documentation
with continuous evidence generation.

Author: Kunal Kumar Singh
License: Apache 2.0
"""

from __future__ import annotations

import hashlib
import json
import logging
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data Classes
# ---------------------------------------------------------------------------

@dataclass
class DatasetFacet:
    """Describes a single dataset in the lineage graph."""
    name:          str
    namespace:     str            # e.g. "s3://fdgf-prod/basel3/"
    source_system: str            # e.g. "CORE_BANKING", "GL", "RISK_ENGINE"
    schema_version: str = "1.0"
    record_count:  int  = 0
    byte_size:     int  = 0
    dataset_fingerprint: str = ""  # Content hash for local files; deterministic fingerprint otherwise


@dataclass
class TransformationFacet:
    """Describes a data transformation step."""
    transform_id:   str
    transform_name: str
    transform_type: str           # e.g. "JOIN", "AGGREGATE", "FILTER", "PIVOT"
    sql_or_code:    str = ""      # Captured code / SQL for reproducibility
    spark_plan:     str = ""      # Spark logical plan (optional)


@dataclass
class LineageEvent:
    """
    A single lineage event in an OpenLineage-style structure.
    """
    event_id:        str
    event_type:      str          # "START", "COMPLETE", "FAIL", "ABORT"
    job_name:        str
    job_namespace:   str
    run_id:          str
    inputs:          list[DatasetFacet]
    outputs:         list[DatasetFacet]
    transformations: list[TransformationFacet]
    regulatory_scope: str         # e.g. "Basel III RWA", "CCAR FR Y-14A"
    producer:        str = "fdgf/lineage-tracker/1.0.0"
    schema_url:      str = "https://openlineage.io/spec/1-0-5/OpenLineage.json"
    event_time:      str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    additional_facets: dict[str, Any] = field(default_factory=dict)

    def to_openlineage_dict(self) -> dict:
        """Serialize to an OpenLineage-style JSON structure."""
        return {
            "eventType":  self.event_type,
            "eventTime":  self.event_time,
            "run": {
                "runId": self.run_id,
                "facets": {
                    "regulatoryScope": {"scope": self.regulatory_scope},
                    **self.additional_facets,
                },
            },
            "job": {
                "namespace": self.job_namespace,
                "name":      self.job_name,
                "facets":    {"producer": self.producer},
            },
            "inputs":  [asdict(i) for i in self.inputs],
            "outputs": [asdict(o) for o in self.outputs],
            "transformations": [asdict(t) for t in self.transformations],
            "producer":   self.producer,
            "schemaURL":  self.schema_url,
        }


# ---------------------------------------------------------------------------
# Lineage Tracker
# ---------------------------------------------------------------------------

class LineageTracker:
    """
    Records end-to-end data lineage for regulatory reporting pipelines.

    Captures every transformation from raw source data through to the final
    reporting output, producing a reproducible artifact trail that can support
    traceability and review workflows.

    Usage
    -----
    >>> tracker = LineageTracker(
    ...     job_name="basel3_rwa_pipeline",
    ...     job_namespace="fdgf.regulatory",
    ...     regulatory_scope="Basel III RWA",
    ...     output_dir="audit_output/lineage/",
    ... )
    >>> tracker.start_run()
    >>> tracker.record_input(name="core_banking_exposures", namespace="s3://...", source_system="CORE_BANKING", record_count=500000)
    >>> tracker.record_transformation(name="rwa_calculation", transform_type="AGGREGATE", sql_or_code="SELECT counterparty_id, SUM(exposure * risk_weight) AS rwa_amount FROM ...")
    >>> tracker.record_output(name="basel3_rwa_report", namespace="s3://...", source_system="FDGF_PIPELINE", record_count=500000)
    >>> tracker.complete_run()
    """

    def __init__(
        self,
        job_name:        str,
        job_namespace:   str,
        regulatory_scope: str,
        output_dir:      str | Path = "audit_output/lineage/",
    ) -> None:
        self.job_name         = job_name
        self.job_namespace    = job_namespace
        self.regulatory_scope = regulatory_scope
        self.output_dir       = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.run_id:     str = ""
        self._inputs:    list[DatasetFacet]          = []
        self._outputs:   list[DatasetFacet]          = []
        self._transforms: list[TransformationFacet]  = []
        self._events:    list[LineageEvent]          = []
        self._active:    bool                        = False
        self._last_bundle_path: str | None           = None

    # ------------------------------------------------------------------
    # Run Lifecycle
    # ------------------------------------------------------------------

    def start_run(self) -> str:
        """Begin a new lineage run. Returns the run_id."""
        self.run_id     = str(uuid.uuid4())
        self._active    = True
        self._inputs    = []
        self._outputs   = []
        self._transforms = []
        self._events    = []
        self._last_bundle_path = None

        event = self._create_event("START")
        self._events.append(event)
        self._persist_event(event)
        logger.info("Lineage run STARTED | run_id=%s | job=%s", self.run_id, self.job_name)
        return self.run_id

    def complete_run(self) -> str:
        """Mark the run as successfully completed. Returns path to audit bundle."""
        if not self._active:
            if self._last_bundle_path is not None:
                return self._last_bundle_path
            raise RuntimeError("No active lineage run to complete.")
        self._active = False
        event = self._create_event("COMPLETE")
        self._events.append(event)
        self._persist_event(event)
        bundle_path = self._write_audit_bundle()
        self._last_bundle_path = bundle_path
        logger.info(
            "Lineage run COMPLETE | run_id=%s | audit_bundle=%s",
            self.run_id, bundle_path
        )
        return bundle_path

    def fail_run(self, error: str = "") -> None:
        """Mark the run as failed."""
        if not self._active:
            return
        self._active = False
        event = self._create_event("FAIL", additional_facets={"error": error})
        self._events.append(event)
        self._persist_event(event)
        logger.error("Lineage run FAILED | run_id=%s | error=%s", self.run_id, error)

    # ------------------------------------------------------------------
    # Recording API
    # ------------------------------------------------------------------

    def record_input(
        self,
        name:          str,
        namespace:     str,
        source_system: str,
        record_count:  int = 0,
        byte_size:     int = 0,
        schema_version: str = "1.0",
    ) -> None:
        """Record a source dataset consumed by this pipeline run."""
        self._ensure_active()
        facet = DatasetFacet(
            name=name,
            namespace=namespace,
            source_system=source_system,
            schema_version=schema_version,
            record_count=record_count,
            byte_size=byte_size or self._local_file_size(name, namespace),
            dataset_fingerprint=self._dataset_fingerprint(name, namespace, record_count, byte_size, schema_version),
        )
        self._inputs.append(facet)
        logger.debug("Input recorded: %s from %s (%d records)", name, source_system, record_count)

    def record_output(
        self,
        name:          str,
        namespace:     str,
        source_system: str,
        record_count:  int = 0,
        byte_size:     int = 0,
        schema_version: str = "1.0",
    ) -> None:
        """Record a dataset produced by this pipeline run."""
        self._ensure_active()
        facet = DatasetFacet(
            name=name,
            namespace=namespace,
            source_system=source_system,
            schema_version=schema_version,
            record_count=record_count,
            byte_size=byte_size or self._local_file_size(name, namespace),
            dataset_fingerprint=self._dataset_fingerprint(name, namespace, record_count, byte_size, schema_version),
        )
        self._outputs.append(facet)
        logger.debug("Output recorded: %s (%d records)", name, record_count)

    def record_transformation(
        self,
        name:           str,
        transform_type: str,
        sql_or_code:    str = "",
        spark_plan:     str = "",
    ) -> str:
        """Record a transformation step. Returns the transform_id."""
        self._ensure_active()
        transform_id = str(uuid.uuid4())[:8].upper()
        facet = TransformationFacet(
            transform_id=transform_id,
            transform_name=name,
            transform_type=transform_type,
            sql_or_code=sql_or_code,
            spark_plan=spark_plan,
        )
        self._transforms.append(facet)
        logger.debug("Transformation recorded: %s [%s] id=%s", name, transform_type, transform_id)
        return transform_id

    # ------------------------------------------------------------------
    # Audit Bundle
    # ------------------------------------------------------------------

    def _write_audit_bundle(self) -> str:
        """
        Write a complete, machine-readable audit bundle covering the full run.
        The bundle documents every input, transformation, and output with
        lightweight fingerprints and run evidence.
        """
        bundle = {
            "bundle_type":       "LINEAGE_AUDIT",
            "run_id":            self.run_id,
            "job_name":          self.job_name,
            "job_namespace":     self.job_namespace,
            "regulatory_scope":  self.regulatory_scope,
            "generated_at":      datetime.now(timezone.utc).isoformat(),
            "framework_version": "1.0.0",
            "total_events":      len(self._events),
            "inputs":            [asdict(i) for i in self._inputs],
            "outputs":           [asdict(o) for o in self._outputs],
            "transformations":   [asdict(t) for t in self._transforms],
            "events":            [e.to_openlineage_dict() for e in self._events],
            "lineage_hash":      self._hash_lineage_graph(),
        }

        ts  = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        out = self.output_dir / f"lineage_{self.job_name}_{ts}.json"
        with open(out, "w") as fh:
            json.dump(bundle, fh, indent=2)

        return str(out)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _create_event(
        self,
        event_type: str,
        additional_facets: dict | None = None,
    ) -> LineageEvent:
        return LineageEvent(
            event_id         = str(uuid.uuid4()),
            event_type       = event_type,
            job_name         = self.job_name,
            job_namespace    = self.job_namespace,
            run_id           = self.run_id,
            inputs           = list(self._inputs),
            outputs          = list(self._outputs),
            transformations  = list(self._transforms),
            regulatory_scope = self.regulatory_scope,
            additional_facets = additional_facets or {},
        )

    def _persist_event(self, event: LineageEvent) -> None:
        ts  = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
        out = self.output_dir / f"event_{event.event_type}_{ts}.json"
        with open(out, "w") as fh:
            json.dump(event.to_openlineage_dict(), fh, indent=2)

    def _ensure_active(self) -> None:
        if not self._active:
            raise RuntimeError("Lineage run is not active. Call start_run() before recording artifacts.")

    def _hash_lineage_graph(self) -> str:
        raw = json.dumps(
            {
                "inputs":  [asdict(i) for i in self._inputs],
                "outputs": [asdict(o) for o in self._outputs],
                "transforms": [asdict(t) for t in self._transforms],
            },
            sort_keys=True,
        )
        return hashlib.sha256(raw.encode()).hexdigest()

    @staticmethod
    def _dataset_fingerprint(
        name: str,
        namespace: str,
        record_count: int,
        byte_size: int,
        schema_version: str,
    ) -> str:
        local_path = LineageTracker._resolve_local_path(name, namespace)
        if local_path is not None and local_path.is_file():
            return LineageTracker._hash_file_contents(local_path)
        raw = f"{name}|{namespace}|{record_count}|{byte_size}|{schema_version}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    @staticmethod
    def _resolve_local_path(name: str, namespace: str) -> Path | None:
        if namespace.startswith("local://"):
            base = Path(namespace.removeprefix("local://"))
            candidate = base / name
            if candidate.exists():
                return candidate
            if base.is_file():
                return base
            return None

        base = Path(namespace)
        if base.is_file():
            return base
        candidate = base / name
        if candidate.exists():
            return candidate
        return None

    @staticmethod
    def _hash_file_contents(path: Path) -> str:
        hasher = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                hasher.update(chunk)
        return hasher.hexdigest()

    @staticmethod
    def _local_file_size(name: str, namespace: str) -> int:
        local_path = LineageTracker._resolve_local_path(name, namespace)
        if local_path is not None and local_path.is_file():
            return local_path.stat().st_size
        return 0
