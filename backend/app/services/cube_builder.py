"""多维立方领域服务：预计算、维度组合生成、构建调度。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store
from app.analytics_engine import (
    build_cube_cell,
    generate_dimension_combinations,
    compute_metric,
    AGGREGATORS,
    shard_coordinator,
)


def _cube_spec(cube: dict[str, Any]) -> tuple[list[str], list[dict[str, str]]]:
    """解析立方的维度列表与预聚合度量口径，供预计算与分片构建共用。"""
    dimensions = [d.strip() for d in str(cube.get("维度列表", "")).split(",") if d.strip()]
    measures: list[dict[str, str]] = []
    for m in str(cube.get("预聚合度量", "")).split(","):
        parts = m.strip().split(":")
        if len(parts) >= 2:
            measures.append({"name": parts[0].strip(), "aggregator": parts[1].strip()})
        elif m.strip():
            measures.append({"name": m.strip(), "aggregator": "SUM"})
    return dimensions, measures


class CubeBuilderService(BaseModuleService):
    MODULE = "cube_builder"
    ENTITY = "OLAP立方"
    OPERATION_TERM = "聚合"
    LIST_FIELDS = ["立方名称", "事实表", "维度列表", "预聚合度量", "分区策略", "刷新频率", "存储引擎"]
    REQUIRED_FIELDS = ["立方名称", "事实表", "维度列表"]
    STATUS_ORDER = ["草稿", "构建中", "已上线", "已下线"]
    ACTION_RULES = {"开始构建": "构建中", "上线立方": "已上线", "下线立方": "已下线"}
    NEGATIVE_ACTIONS = ["下线立方"]
    SHARD_CONSUMER = "cube"

    # ---- 业务方法 ----

    def precompute_cube(self, cube_id: int, sample_data: list[dict[str, Any]]) -> dict[str, Any]:
        """对指定立方执行预计算，按维度组合分组聚合。"""
        cube = store.find(self.MODULE, cube_id)
        if cube is None:
            return {"ok": False, "message": f"立方 {cube_id} 不存在"}
        dimensions, measures = _cube_spec(cube)

        cells = build_cube_cell(sample_data, dimensions, measures)
        dimension_combos = generate_dimension_combinations(dimensions)

        return {
            "ok": True,
            "cube_id": cube_id,
            "cube_name": str(cube.get("立方名称", "")),
            "dimensions": dimensions,
            "dimension_combinations": len(dimension_combos),
            "cells": len(cells),
            "cells_preview": [
                {
                    "dimensions": c.dimensions,
                    "measures": c.measure_values,
                    "row_count": c.row_count,
                }
                for c in cells[:10]
            ],
            "storage_engine": cube.get("存储引擎"),
        }

    def get_cube_dimensions(self, cube_id: int) -> list[str]:
        """获取立方的维度列表（供切片查询消费）。"""
        cube = store.find(self.MODULE, cube_id)
        if cube is None:
            return []
        return [d.strip() for d in str(cube.get("维度列表", "")).split(",") if d.strip()]

    def get_online_cubes(self) -> list[dict[str, Any]]:
        """获取所有已上线立方。"""
        return [
            row for row in store.rows(self.MODULE)
            if row.get("status") == "已上线"
        ]

    # ---- 分片消费方：与下钻共享同批事实 ----

    def start_shard_build(self, cube_id: int, shard_id: str, sample_data: list[dict[str, Any]]) -> dict[str, Any]:
        """立方作为分片消费方开始构建：登记同批事实，计算单元格并暂存阶段产物。"""
        cube = store.find(self.MODULE, cube_id)
        if cube is None:
            return {"ok": False, "message": f"立方 {cube_id} 不存在"}
        dimensions, measures = _cube_spec(cube)
        registered = shard_coordinator.register_shard(shard_id, sample_data)
        if not registered["ok"]:
            return registered
        begun = shard_coordinator.begin(self.SHARD_CONSUMER, shard_id)
        if not begun["ok"] or begun["already_committed"]:
            return begun
        cells = build_cube_cell(sample_data, dimensions, measures)
        shard_coordinator.stage_product(self.SHARD_CONSUMER, shard_id, {
            "cube_id": cube_id,
            "dimensions": dimensions,
            "cells": [
                {
                    "dimensions": c.dimensions,
                    "measures": c.measure_values,
                    "row_count": c.row_count,
                }
                for c in cells
            ],
        })
        return {
            "ok": True,
            "shard_id": shard_id,
            "cube_id": cube_id,
            "state": begun["state"],
            "attempts": begun["attempts"],
            "cells": len(cells),
        }

    def commit_shard(self, shard_id: str) -> dict[str, Any]:
        """提交分片阶段产物；中断重试后同一分片只积进总量一次。"""
        return shard_coordinator.commit(self.SHARD_CONSUMER, shard_id)

    def interrupt_shard(self, shard_id: str) -> dict[str, Any]:
        """中断当前分片写入，未足量阶段产物保留供辨别。"""
        return shard_coordinator.interrupt(self.SHARD_CONSUMER, shard_id)

    def fetch_shard(self, shard_id: str) -> dict[str, Any]:
        """立方来取分片：可看到下钻一侧是否已完成。"""
        return shard_coordinator.read(self.SHARD_CONSUMER, shard_id)

    def shard_progress(self) -> dict[str, Any]:
        """立方一侧的分片总量进度与未足量分片辨别。"""
        return shard_coordinator.progress(self.SHARD_CONSUMER)

    def estimate_storage(self, cube_id: int, sample_row_count: int) -> dict[str, Any]:
        """估算立方存储需求。"""
        cube = store.find(self.MODULE, cube_id)
        if cube is None:
            return {"ok": False, "message": f"立方 {cube_id} 不存在"}
        dimensions_count = len(
            [d for d in str(cube.get("维度列表", "")).split(",") if d.strip()]
        )
        measures_count = len(
            [m for m in str(cube.get("预聚合度量", "")).split(",") if m.strip()]
        )
        combos = 2 ** dimensions_count - 1
        estimated_cells = combos * min(sample_row_count, 1000)
        estimated_bytes = estimated_cells * measures_count * 32
        return {
            "ok": True,
            "dimension_count": dimensions_count,
            "measure_count": measures_count,
            "dimension_combinations": combos,
            "estimated_cells": estimated_cells,
            "estimated_mb": round(estimated_bytes / (1024 * 1024), 2),
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        online_cubes = sum(
            1 for row in store.rows(self.MODULE) if row.get("status") == "已上线"
        )
        base["online_cubes"] = online_cubes
        base["artifact"] = "OLAP立方数据集"
        return base