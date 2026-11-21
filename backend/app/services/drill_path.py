"""下钻路径领域服务：层级分析、度量聚合、看板联动。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store
from app.analytics_engine import compute_drill_levels, shard_coordinator


def _drill_spec(path_config: dict[str, Any]) -> tuple[list[str], list[str]]:
    """解析下钻层级与各层度量，供下钻执行与分片计算共用。"""
    hierarchy = [h.strip() for h in str(path_config.get("下钻层级", "")).split(",") if h.strip()]
    measures = [m.strip() for m in str(path_config.get("各层度量", "")).split(",") if m.strip()]
    return hierarchy, measures


class DrillPathService(BaseModuleService):
    MODULE = "drill_path"
    ENTITY = "下钻配置"
    OPERATION_TERM = "关联"
    LIST_FIELDS = ["路径名称", "起始维度", "下钻层级", "各层度量", "是否启用手动", "关联看板", "路径说明"]
    REQUIRED_FIELDS = ["路径名称", "起始维度", "下钻层级"]
    STATUS_ORDER = ["草稿", "已启用", "已停用"]
    ACTION_RULES = {"启用路径": "已启用", "停用路径": "已停用"}
    NEGATIVE_ACTIONS = ["停用路径"]
    SHARD_CONSUMER = "drill"

    # ---- 业务方法 ----

    def execute_drill(
        self, path_id: int, data: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """执行下钻分析：逐层聚合度量。"""
        path_config = store.find(self.MODULE, path_id)
        if path_config is None:
            return {"ok": False, "message": f"下钻配置 {path_id} 不存在"}

        hierarchy, measures = _drill_spec(path_config)

        levels = compute_drill_levels(data, hierarchy, measures)

        return {
            "ok": True,
            "path_name": str(path_config.get("路径名称", "")),
            "start_dimension": path_config.get("起始维度"),
            "hierarchy": hierarchy,
            "measures": measures,
            "levels": levels,
            "level_count": len(levels),
            "associated_dashboard": path_config.get("关联看板"),
        }

    def get_drill_paths_for_dashboard(self, dashboard_name: str) -> list[dict[str, Any]]:
        """获取某看板关联的下钻路径（数据范围→下钻路径消费）。"""
        return [
            row for row in store.rows(self.MODULE)
            if row.get("关联看板") == dashboard_name and row.get("status") == "已启用"
        ]

    # ---- 分片消费方：与立方共享同批事实 ----

    def start_shard_drill(self, path_id: int, shard_id: str, data: list[dict[str, Any]]) -> dict[str, Any]:
        """下钻作为分片消费方开始计算：登记同批事实，逐层聚合并暂存阶段产物。"""
        path_config = store.find(self.MODULE, path_id)
        if path_config is None:
            return {"ok": False, "message": f"下钻配置 {path_id} 不存在"}
        hierarchy, measures = _drill_spec(path_config)
        registered = shard_coordinator.register_shard(shard_id, data)
        if not registered["ok"]:
            return registered
        begun = shard_coordinator.begin(self.SHARD_CONSUMER, shard_id)
        if not begun["ok"] or begun["already_committed"]:
            return begun
        levels = compute_drill_levels(data, hierarchy, measures)
        shard_coordinator.stage_product(self.SHARD_CONSUMER, shard_id, {
            "path_id": path_id,
            "hierarchy": hierarchy,
            "measures": measures,
            "levels": levels,
        })
        return {
            "ok": True,
            "shard_id": shard_id,
            "path_id": path_id,
            "state": begun["state"],
            "attempts": begun["attempts"],
            "level_count": len(levels),
        }

    def commit_shard(self, shard_id: str) -> dict[str, Any]:
        """提交分片阶段产物；中断重试后同一分片只积进总量一次。"""
        return shard_coordinator.commit(self.SHARD_CONSUMER, shard_id)

    def interrupt_shard(self, shard_id: str) -> dict[str, Any]:
        """中断当前分片计算，未足量阶段产物保留供辨别。"""
        return shard_coordinator.interrupt(self.SHARD_CONSUMER, shard_id)

    def read_shard(self, shard_id: str) -> dict[str, Any]:
        """下钻读取分片：立方尚未提交时也能读到同批事实并辨别对方进度。"""
        return shard_coordinator.read(self.SHARD_CONSUMER, shard_id)

    def shard_progress(self) -> dict[str, Any]:
        """下钻一侧的分片总量进度与未足量分片辨别。"""
        return shard_coordinator.progress(self.SHARD_CONSUMER)

    def get_enabled_paths(self) -> list[dict[str, Any]]:
        """获取所有已启用路径。"""
        return [
            row for row in store.rows(self.MODULE)
            if row.get("status") == "已启用"
        ]

    def compare_drill_results(
        self, path_ids: list[int], data: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """比较多条下钻路径的分析结果。"""
        results: list[dict[str, Any]] = []
        for pid in path_ids:
            r = self.execute_drill(pid, data)
            if r.get("ok"):
                results.append(r)
        return {
            "ok": True,
            "path_count": len(path_ids),
            "results": results,
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["artifact"] = "下钻路径数据集"
        base["enabled_paths"] = sum(1 for row in store.rows(self.MODULE) if row.get("status") == "已启用")
        return base