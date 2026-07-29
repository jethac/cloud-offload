"""Queryable SQLite projection of durable prepared-state manifests."""

from __future__ import annotations

import json
import sqlite3
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from cloud_offload.prepared_state import INDEX_SCHEMA, OBSERVATION_SCHEMA


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class CacheVolume:
    id: str
    provider: str
    provider_volume_id: str
    datacenter_id: str
    ownership: str
    status: str
    capacity_bytes: int
    inventory_generation: str | None
    last_verified_at: str | None
    policy: dict[str, Any]
    s3_compatible: bool = False


class CacheRegistry:
    """Local projection used for scheduling; signed manifests remain truth."""

    VOLUME_STATUSES = {"creating", "ready", "degraded", "deleting", "failed"}

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    def _init_db(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS cache_volumes (
                    id TEXT PRIMARY KEY,
                    provider TEXT NOT NULL,
                    provider_volume_id TEXT NOT NULL,
                    datacenter_id TEXT NOT NULL,
                    ownership TEXT NOT NULL CHECK (ownership IN ('managed','adopted')),
                    status TEXT NOT NULL,
                    capacity_bytes INTEGER NOT NULL,
                    inventory_generation TEXT,
                    last_verified_at TEXT,
                    policy_json TEXT NOT NULL,
                    s3_compatible INTEGER NOT NULL DEFAULT 0,
                    UNIQUE(provider, provider_volume_id)
                );
                CREATE INDEX IF NOT EXISTS idx_cache_volumes_placement
                    ON cache_volumes(provider, datacenter_id, status);
                CREATE TABLE IF NOT EXISTS cache_manifests (
                    volume_id TEXT NOT NULL,
                    manifest_id TEXT NOT NULL,
                    profile_fingerprint TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    manifest_json TEXT NOT NULL,
                    PRIMARY KEY(volume_id, manifest_id),
                    FOREIGN KEY(volume_id) REFERENCES cache_volumes(id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS idx_cache_manifests_profile
                    ON cache_manifests(profile_fingerprint, volume_id);
                CREATE TABLE IF NOT EXISTS cache_artifacts (
                    volume_id TEXT NOT NULL,
                    digest TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    compatibility_key TEXT NOT NULL,
                    manifest_id TEXT NOT NULL,
                    last_verified_at TEXT,
                    last_used_at TEXT,
                    restore_count INTEGER NOT NULL DEFAULT 0,
                    restore_ms REAL NOT NULL DEFAULT 0,
                    saved_ms REAL NOT NULL DEFAULT 0,
                    eligibility TEXT NOT NULL DEFAULT 'eligible',
                    policy_json TEXT NOT NULL,
                    PRIMARY KEY(volume_id, digest),
                    FOREIGN KEY(volume_id) REFERENCES cache_volumes(id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS idx_cache_artifacts_digest
                    ON cache_artifacts(digest, eligibility);
                CREATE TABLE IF NOT EXISTS restore_observations (
                    id TEXT PRIMARY KEY,
                    schema TEXT NOT NULL,
                    volume_id TEXT,
                    manifest_id TEXT,
                    digest TEXT,
                    datacenter_id TEXT NOT NULL,
                    worker_class TEXT NOT NULL,
                    image_digest TEXT,
                    strategy TEXT NOT NULL,
                    result TEXT NOT NULL,
                    bytes INTEGER NOT NULL,
                    file_count INTEGER NOT NULL,
                    lookup_ms REAL NOT NULL,
                    transfer_ms REAL NOT NULL,
                    verification_ms REAL NOT NULL,
                    extraction_ms REAL NOT NULL,
                    import_ms REAL NOT NULL,
                    total_ms REAL NOT NULL,
                    fallback_ms REAL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_restore_observations_scope
                    ON restore_observations(datacenter_id, worker_class, digest, created_at);
                CREATE TABLE IF NOT EXISTS cache_invalidations (
                    volume_id TEXT NOT NULL,
                    digest TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(volume_id, digest),
                    FOREIGN KEY(volume_id) REFERENCES cache_volumes(id) ON DELETE CASCADE
                );
                CREATE TABLE IF NOT EXISTS cache_replications (
                    id TEXT PRIMARY KEY,
                    source_volume_id TEXT NOT NULL,
                    target_volume_id TEXT NOT NULL,
                    digests_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    bytes INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    completed_at TEXT,
                    FOREIGN KEY(source_volume_id) REFERENCES cache_volumes(id),
                    FOREIGN KEY(target_volume_id) REFERENCES cache_volumes(id)
                );
                """
            )

    def upsert_volume(
        self,
        *,
        provider: str,
        provider_volume_id: str,
        datacenter_id: str,
        ownership: str,
        capacity_bytes: int,
        policy: dict[str, Any],
        status: str = "ready",
        s3_compatible: bool = False,
        volume_id: str | None = None,
    ) -> CacheVolume:
        if ownership not in {"managed", "adopted"}:
            raise ValueError("Cache volume ownership must be managed or adopted")
        if status not in self.VOLUME_STATUSES:
            raise ValueError(f"Unknown cache volume status: {status}")
        volume_id = volume_id or str(uuid.uuid4())
        with self._connect() as connection:
            existing = connection.execute(
                "SELECT id FROM cache_volumes WHERE provider=? AND provider_volume_id=?",
                (provider, provider_volume_id),
            ).fetchone()
            if existing:
                volume_id = existing["id"]
            connection.execute(
                """
                INSERT INTO cache_volumes (
                    id, provider, provider_volume_id, datacenter_id, ownership,
                    status, capacity_bytes, inventory_generation,
                    last_verified_at, policy_json, s3_compatible
                ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    provider=excluded.provider,
                    provider_volume_id=excluded.provider_volume_id,
                    datacenter_id=excluded.datacenter_id,
                    ownership=excluded.ownership,
                    status=excluded.status,
                    capacity_bytes=excluded.capacity_bytes,
                    policy_json=excluded.policy_json,
                    s3_compatible=excluded.s3_compatible
                """,
                (
                    volume_id,
                    provider,
                    provider_volume_id,
                    datacenter_id,
                    ownership,
                    status,
                    int(capacity_bytes),
                    json.dumps(policy, sort_keys=True),
                    int(s3_compatible),
                ),
            )
        return self.get_volume(volume_id)  # type: ignore[return-value]

    def get_volume(self, volume_id: str) -> CacheVolume | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM cache_volumes WHERE id=?", (volume_id,)
            ).fetchone()
        return self._volume(row) if row else None

    def get_provider_volume(
        self, provider: str, provider_volume_id: str
    ) -> CacheVolume | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM cache_volumes WHERE provider=? AND provider_volume_id=?",
                (provider, provider_volume_id),
            ).fetchone()
        return self._volume(row) if row else None

    def list_volumes(self, *, status: str | None = None) -> list[CacheVolume]:
        query = "SELECT * FROM cache_volumes"
        values: tuple[Any, ...] = ()
        if status:
            query += " WHERE status=?"
            values = (status,)
        query += " ORDER BY provider, datacenter_id, id"
        with self._connect() as connection:
            rows = connection.execute(query, values).fetchall()
        return [self._volume(row) for row in rows]

    @staticmethod
    def _volume(row: sqlite3.Row) -> CacheVolume:
        return CacheVolume(
            id=row["id"],
            provider=row["provider"],
            provider_volume_id=row["provider_volume_id"],
            datacenter_id=row["datacenter_id"],
            ownership=row["ownership"],
            status=row["status"],
            capacity_bytes=int(row["capacity_bytes"]),
            inventory_generation=row["inventory_generation"],
            last_verified_at=row["last_verified_at"],
            policy=json.loads(row["policy_json"]),
            s3_compatible=bool(row["s3_compatible"]),
        )

    def mark_volume(self, volume_id: str, status: str) -> None:
        if status not in self.VOLUME_STATUSES:
            raise ValueError(f"Unknown cache volume status: {status}")
        with self._connect() as connection:
            cursor = connection.execute(
                "UPDATE cache_volumes SET status=? WHERE id=?", (status, volume_id)
            )
            if not cursor.rowcount:
                raise KeyError(volume_id)

    def delete_metadata(self, volume_id: str) -> bool:
        with self._connect() as connection:
            cursor = connection.execute(
                "DELETE FROM cache_volumes WHERE id=?", (volume_id,)
            )
            return bool(cursor.rowcount)

    def reconcile_index(
        self,
        volume_id: str,
        index: dict[str, Any],
        *,
        manifest_documents: dict[str, dict[str, Any]] | None = None,
    ) -> dict[str, int]:
        if index.get("schema") != INDEX_SCHEMA:
            raise ValueError("Unsupported compact inventory schema")
        generation = str(index.get("generation") or "")
        if not generation:
            raise ValueError("Inventory generation is missing")
        seen: set[str] = set()
        inserted = 0
        with self._connect() as connection:
            if not connection.execute(
                "SELECT 1 FROM cache_volumes WHERE id=?", (volume_id,)
            ).fetchone():
                raise KeyError(volume_id)
            for manifest in index.get("manifests") or []:
                manifest_id = str(manifest["manifest_id"])
                document = (manifest_documents or {}).get(manifest_id, manifest)
                connection.execute(
                    """
                    INSERT INTO cache_manifests (
                        manifest_id, volume_id, profile_fingerprint, created_at, manifest_json
                    ) VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(volume_id, manifest_id) DO UPDATE SET
                        profile_fingerprint=excluded.profile_fingerprint,
                        created_at=excluded.created_at,
                        manifest_json=excluded.manifest_json
                    """,
                    (
                        manifest_id,
                        volume_id,
                        manifest["profile_fingerprint"],
                        manifest["created_at"],
                        json.dumps(document, sort_keys=True),
                    ),
                )
                for artifact in manifest.get("artifacts") or []:
                    digest = str(artifact["digest"])
                    seen.add(digest)
                    compatibility_key = json.dumps(
                        {
                            "portability": artifact.get("portability"),
                            "requirements": artifact.get("requirements") or {},
                        },
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                    invalidated = connection.execute(
                        "SELECT 1 FROM cache_invalidations WHERE volume_id=? AND digest=?",
                        (volume_id, digest),
                    ).fetchone()
                    connection.execute(
                        """
                        INSERT INTO cache_artifacts (
                            volume_id, digest, kind, size_bytes, compatibility_key,
                            manifest_id, last_verified_at, policy_json, eligibility
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(volume_id, digest) DO UPDATE SET
                            kind=excluded.kind, size_bytes=excluded.size_bytes,
                            compatibility_key=excluded.compatibility_key,
                            manifest_id=excluded.manifest_id,
                            last_verified_at=excluded.last_verified_at,
                            policy_json=excluded.policy_json,
                            eligibility=excluded.eligibility
                        """,
                        (
                            volume_id,
                            digest,
                            str(artifact.get("kind") or "unknown"),
                            int(artifact.get("size") or 0),
                            compatibility_key,
                            manifest_id,
                            _now(),
                            json.dumps(artifact.get("policy") or {}, sort_keys=True),
                            "invalidated" if invalidated else "eligible",
                        ),
                    )
                    inserted += 1
            rows = connection.execute(
                "SELECT digest FROM cache_artifacts WHERE volume_id=?", (volume_id,)
            ).fetchall()
            drifted = {row["digest"] for row in rows} - seen
            for digest in drifted:
                connection.execute(
                    "UPDATE cache_artifacts SET eligibility='unknown' WHERE volume_id=? AND digest=?",
                    (volume_id, digest),
                )
            connection.execute(
                """
                UPDATE cache_volumes SET inventory_generation=?, last_verified_at=?,
                    status=? WHERE id=?
                """,
                (generation, _now(), "degraded" if drifted else "ready", volume_id),
            )
        return {"artifacts": inserted, "drifted": len(drifted)}

    def announce_manifest(
        self, volume_id: str, generation: str, manifest: dict[str, Any]
    ) -> dict[str, int]:
        """Merge one worker-published signed manifest without dropping known inventory."""
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT manifest_json FROM cache_manifests WHERE volume_id=?",
                (volume_id,),
            ).fetchall()
        documents = {
            document["manifest_id"]: document
            for document in (json.loads(row["manifest_json"]) for row in rows)
        }
        documents[manifest["manifest_id"]] = manifest
        index = {
            "schema": INDEX_SCHEMA,
            "generation": str(generation),
            "manifests": [
                {
                    "manifest_id": document["manifest_id"],
                    "profile_fingerprint": document["profile_fingerprint"],
                    "created_at": document["created_at"],
                    "artifacts": document.get("artifacts") or [],
                }
                for document in documents.values()
            ],
        }
        return self.reconcile_index(volume_id, index, manifest_documents=documents)

    def remove_manifest(
        self,
        volume_id: str,
        manifest_id: str,
        *,
        inventory_generation: str | None = None,
    ) -> dict[str, int]:
        """Remove one manifest and rebuild projections for its artifact set.

        Durable indexes are the source of truth. This operation is used after a
        reversible benchmark generation is removed from that index; rebuilding
        every digest mentioned by the removed manifest prevents its temporary
        manifest ID or invalidation from poisoning later placement decisions.
        Restore counters and timing observations on surviving artifacts remain
        intact.
        """

        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT manifest_json FROM cache_manifests "
                "WHERE volume_id=? AND manifest_id=?",
                (volume_id, manifest_id),
            ).fetchone()
            if not row:
                if inventory_generation is not None:
                    connection.execute(
                        "UPDATE cache_volumes SET inventory_generation=? WHERE id=?",
                        (inventory_generation, volume_id),
                    )
                return {"manifests": 0, "artifacts_restored": 0, "artifacts_removed": 0}
            removed = json.loads(row["manifest_json"])
            connection.execute(
                "DELETE FROM cache_manifests WHERE volume_id=? AND manifest_id=?",
                (volume_id, manifest_id),
            )
            remaining_rows = connection.execute(
                "SELECT manifest_id, manifest_json FROM cache_manifests "
                "WHERE volume_id=? ORDER BY created_at DESC, manifest_id DESC",
                (volume_id,),
            ).fetchall()
            latest_by_digest: dict[str, tuple[str, dict[str, Any]]] = {}
            for remaining_row in remaining_rows:
                document = json.loads(remaining_row["manifest_json"])
                for artifact in document.get("artifacts") or []:
                    latest_by_digest.setdefault(
                        str(artifact.get("digest") or ""),
                        (remaining_row["manifest_id"], artifact),
                    )
            restored = 0
            removed_count = 0
            for digest in {
                str(item.get("digest") or "")
                for item in removed.get("artifacts") or []
                if item.get("digest")
            }:
                candidate = latest_by_digest.get(digest)
                if candidate is None:
                    connection.execute(
                        "DELETE FROM cache_artifacts WHERE volume_id=? AND digest=?",
                        (volume_id, digest),
                    )
                    connection.execute(
                        "DELETE FROM cache_invalidations WHERE volume_id=? AND digest=?",
                        (volume_id, digest),
                    )
                    removed_count += 1
                    continue
                surviving_manifest_id, artifact = candidate
                compatibility_key = json.dumps(
                    {
                        "portability": artifact.get("portability"),
                        "requirements": artifact.get("requirements") or {},
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                )
                invalidated = connection.execute(
                    "SELECT 1 FROM cache_invalidations WHERE volume_id=? AND digest=?",
                    (volume_id, digest),
                ).fetchone()
                connection.execute(
                    """
                    UPDATE cache_artifacts SET
                        kind=?, size_bytes=?, compatibility_key=?, manifest_id=?,
                        policy_json=?, eligibility=?
                    WHERE volume_id=? AND digest=?
                    """,
                    (
                        str(artifact.get("kind") or "unknown"),
                        int(artifact.get("size") or 0),
                        compatibility_key,
                        surviving_manifest_id,
                        json.dumps(artifact.get("policy") or {}, sort_keys=True),
                        "invalidated" if invalidated else "eligible",
                        volume_id,
                        digest,
                    ),
                )
                restored += 1
            if inventory_generation is not None:
                connection.execute(
                    "UPDATE cache_volumes SET inventory_generation=? WHERE id=?",
                    (inventory_generation, volume_id),
                )
            return {
                "manifests": 1,
                "artifacts_restored": restored,
                "artifacts_removed": removed_count,
            }

    def query_manifests(
        self,
        *,
        profile_fingerprint: str | None = None,
        datacenter_id: str | None = None,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        values: list[Any] = []
        if profile_fingerprint:
            clauses.append("m.profile_fingerprint=?")
            values.append(profile_fingerprint)
        if datacenter_id:
            clauses.append("v.datacenter_id=?")
            values.append(datacenter_id)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT m.manifest_json, v.id AS volume_id, v.datacenter_id
                FROM cache_manifests m JOIN cache_volumes v ON v.id=m.volume_id
                """
                + where
                + " ORDER BY m.created_at DESC",
                values,
            ).fetchall()
        return [
            {
                **json.loads(row["manifest_json"]),
                "volume_id": row["volume_id"],
                "datacenter_id": row["datacenter_id"],
            }
            for row in rows
        ]

    def volume_coverage(
        self,
        required: dict[str, int],
        *,
        runtime: dict[str, Any],
        tenant: str,
        profile_fingerprint: str | None = None,
        allow_private: bool = False,
        logical_required: list[str] | tuple[str, ...] = (),
    ) -> list[dict[str, Any]]:
        """Return eligible bytes from the exact profile manifest selected per volume.

        An empty ``required`` set is complete only when the volume has a manifest
        for the requested profile.  This matters for profiles whose expensive
        state is weights or runtime bundles rather than job-declared assets.
        """
        from cloud_offload.prepared_state import artifact_compatibility, artifact_policy

        candidates = []
        for volume in self.list_volumes(status="ready"):
            with self._connect() as connection:
                selected = None
                if profile_fingerprint:
                    selected = connection.execute(
                        """
                        SELECT manifest_id, manifest_json
                        FROM cache_manifests
                        WHERE volume_id=? AND profile_fingerprint=?
                        ORDER BY created_at DESC, manifest_id DESC LIMIT 1
                        """,
                        (volume.id, profile_fingerprint),
                    ).fetchone()
                if selected:
                    manifest = json.loads(selected["manifest_json"])
                    rows = [
                        {
                            "digest": artifact["digest"],
                            "kind": artifact.get("kind"),
                            "size_bytes": artifact.get("size") or 0,
                            "compatibility_key": json.dumps(
                                {
                                    "portability": artifact.get("portability"),
                                    "requirements": artifact.get("requirements") or {},
                                }
                            ),
                            "policy_json": json.dumps(artifact.get("policy") or {}),
                            "eligibility": "eligible",
                            "manifest_id": selected["manifest_id"],
                            "source": artifact.get("source") or {},
                            "destination": artifact.get("destination") or {},
                        }
                        for artifact in manifest.get("artifacts") or []
                    ]
                    invalidated = {
                        row["digest"]
                        for row in connection.execute(
                            "SELECT digest FROM cache_invalidations WHERE volume_id=?",
                            (volume.id,),
                        ).fetchall()
                    }
                    for row in rows:
                        if row["digest"] in invalidated:
                            row["eligibility"] = "invalidated"
                elif profile_fingerprint:
                    rows = []
                else:
                    rows = connection.execute(
                        """
                        SELECT digest, size_bytes, compatibility_key, policy_json,
                               eligibility, manifest_id
                        FROM cache_artifacts WHERE volume_id=?
                        """,
                        (volume.id,),
                    ).fetchall()
                if profile_fingerprint and required:
                    present = {row["digest"] for row in rows}
                    missing = sorted(set(required) - present)
                    if missing:
                        placeholders = ",".join("?" for _ in missing)
                        rows.extend(
                            connection.execute(
                                f"""
                                SELECT digest, size_bytes, compatibility_key, policy_json,
                                       eligibility, manifest_id
                                FROM cache_artifacts
                                WHERE volume_id=? AND digest IN ({placeholders})
                                """,
                                (volume.id, *missing),
                            ).fetchall()
                        )
            hits: list[str] = []
            required_hits: set[str] = set()
            logical_hits: set[str] = set()
            counted: set[str] = set()
            covered = 0
            manifest_ids: set[str] = set()
            for row in rows:
                if row["eligibility"] != "eligible":
                    continue
                compatibility = json.loads(row["compatibility_key"])
                artifact = {
                    "portability": compatibility["portability"],
                    "requirements": compatibility["requirements"],
                    "policy": json.loads(row["policy_json"]),
                }
                if not artifact_compatibility(artifact, runtime).accepted:
                    continue
                if not artifact_policy(
                    artifact, tenant=tenant, allow_private=allow_private
                ).accepted:
                    continue
                from cloud_offload.prepared_state import artifact_requirement_key

                logical_key = (
                    artifact_requirement_key(
                        {
                            "kind": row.get("kind"),
                            "source": row.get("source", {}),
                            "destination": row.get("destination", {}),
                        }
                    )
                    if isinstance(row, dict)
                    else None
                )
                if logical_key:
                    logical_hits.add(logical_key)
                if row["digest"] in required:
                    required_hits.add(row["digest"])
                if row["digest"] not in required and not logical_key:
                    continue
                hits.append(row["digest"])
                if row["digest"] not in counted:
                    covered += int(required.get(row["digest"], row["size_bytes"]))
                    counted.add(row["digest"])
                manifest_ids.add(row["manifest_id"])
            candidates.append(
                {
                    "volume": volume,
                    "cached_digests": sorted(hits),
                    "cached_bytes": covered,
                    "required_bytes": sum(required.values()),
                    "complete": bool(selected or not profile_fingerprint)
                    and len(required_hits) == len(required)
                    and set(logical_required).issubset(logical_hits),
                    "logical_hits": sorted(logical_hits),
                    "manifest_ids": ([selected["manifest_id"]] if selected else []),
                    "coverage_manifest_ids": sorted(manifest_ids),
                }
            )
        return candidates

    def invalidate(self, volume_id: str, digest: str, reason: str) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO cache_invalidations (volume_id, digest, reason, created_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(volume_id, digest) DO UPDATE SET
                    reason=excluded.reason, created_at=excluded.created_at
                """,
                (volume_id, digest, str(reason), _now()),
            )
            connection.execute(
                "UPDATE cache_artifacts SET eligibility='invalidated' WHERE volume_id=? AND digest=?",
                (volume_id, digest),
            )

    def record_observation(self, observation: dict[str, Any]) -> str:
        if observation.get("schema") != OBSERVATION_SCHEMA:
            raise ValueError("Unsupported restore observation schema")
        observation_id = str(observation.get("id") or uuid.uuid4())
        fields = (
            "volume_id",
            "manifest_id",
            "digest",
            "datacenter_id",
            "worker_class",
            "image_digest",
            "strategy",
            "result",
            "bytes",
            "file_count",
            "lookup_ms",
            "transfer_ms",
            "verification_ms",
            "extraction_ms",
            "import_ms",
            "total_ms",
            "fallback_ms",
        )
        values = [observation.get(field) for field in fields]
        numeric = {
            "bytes",
            "file_count",
            "lookup_ms",
            "transfer_ms",
            "verification_ms",
            "extraction_ms",
            "import_ms",
            "total_ms",
        }
        for index, field in enumerate(fields):
            if field in numeric and values[index] is None:
                values[index] = 0
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO restore_observations (
                    id, schema, volume_id, manifest_id, digest, datacenter_id,
                    worker_class, image_digest, strategy, result, bytes, file_count,
                    lookup_ms, transfer_ms, verification_ms, extraction_ms, import_ms,
                    total_ms, fallback_ms, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (observation_id, OBSERVATION_SCHEMA, *values, _now()),
            )
            if observation.get("volume_id") and observation.get("digest"):
                connection.execute(
                    """
                    UPDATE cache_artifacts SET
                        last_used_at=?, restore_count=restore_count+1,
                        restore_ms=restore_ms+?, saved_ms=saved_ms+?
                    WHERE volume_id=? AND digest=?
                    """,
                    (
                        _now(),
                        float(observation.get("total_ms") or 0),
                        max(
                            0.0,
                            float(observation.get("fallback_ms") or 0)
                            - float(observation.get("total_ms") or 0),
                        ),
                        observation["volume_id"],
                        observation["digest"],
                    ),
                )
                if observation.get("result") == "corruption":
                    connection.execute(
                        """
                        INSERT INTO cache_invalidations (
                            volume_id, digest, reason, created_at
                        ) VALUES (?, ?, ?, ?)
                        ON CONFLICT(volume_id, digest) DO UPDATE SET
                            reason=excluded.reason, created_at=excluded.created_at
                        """,
                        (
                            observation["volume_id"],
                            observation["digest"],
                            "worker_reported_corruption",
                            _now(),
                        ),
                    )
                    connection.execute(
                        """
                        UPDATE cache_artifacts SET eligibility='invalidated'
                        WHERE volume_id=? AND digest=?
                        """,
                        (observation["volume_id"], observation["digest"]),
                    )
        return observation_id

    def recent_benefit(self) -> dict[str, Any]:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT COUNT(*) AS attempts,
                       COALESCE(SUM(bytes),0) AS bytes,
                       COALESCE(SUM(CASE WHEN fallback_ms > total_ms THEN fallback_ms-total_ms ELSE 0 END),0) AS saved_ms,
                       COALESCE(SUM(CASE WHEN fallback_ms < total_ms THEN total_ms-fallback_ms ELSE 0 END),0) AS lost_ms
                FROM restore_observations
                """
            ).fetchone()
        return dict(row)

    def create_replication(
        self,
        source_volume_id: str,
        target_volume_id: str,
        digests: list[str],
        sizes: dict[str, int],
    ) -> dict[str, Any]:
        source = self.get_volume(source_volume_id)
        target = self.get_volume(target_volume_id)
        if not source or not target:
            raise KeyError("Replication source or target volume is unknown")
        if source.id == target.id:
            raise ValueError("Replication source and target must differ")
        replication_id = str(uuid.uuid4())
        total = sum(int(sizes.get(item, 0)) for item in digests)
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO cache_replications (
                    id, source_volume_id, target_volume_id, digests_json,
                    status, bytes, created_at
                ) VALUES (?, ?, ?, ?, 'planned', ?, ?)
                """,
                (
                    replication_id,
                    source.id,
                    target.id,
                    json.dumps(sorted(set(digests))),
                    total,
                    _now(),
                ),
            )
        return {
            "id": replication_id,
            "source_volume_id": source.id,
            "target_volume_id": target.id,
            "digests": sorted(set(digests)),
            "bytes": total,
            "status": "planned",
        }

    def complete_replication(
        self, replication_id: str, *, failed_reason: str | None = None
    ) -> None:
        status = "failed" if failed_reason else "completed"
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE cache_replications SET status=?, completed_at=? WHERE id=?
                """,
                (
                    status if not failed_reason else f"failed:{failed_reason}"[:500],
                    _now(),
                    replication_id,
                ),
            )
            if not cursor.rowcount:
                raise KeyError(replication_id)

    def status(self, policy: dict[str, Any]) -> dict[str, Any]:
        volumes = [asdict(item) for item in self.list_volumes()]
        return {
            "policy": policy,
            "volumes": volumes,
            "health": "degraded"
            if any(v["status"] == "degraded" for v in volumes)
            else "ready",
            "capacity_bytes": sum(item["capacity_bytes"] for item in volumes),
            "recent_benefit": self.recent_benefit(),
        }
