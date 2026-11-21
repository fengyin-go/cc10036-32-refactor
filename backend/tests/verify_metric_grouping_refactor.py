"""度量分组重构与分片双消费方协调的验证脚本。

对照任务要求逐项验证：
1. 立方与下钻共用「输入整理」后，旧事实的分组行为与重构前完全一致；
2. 同批事实下，两边各自保留自己的缺值/合法零值计入口径，层级缺值与总量对照原样本；
3. 分片级协调：下钻计算中立方来取、后写方中断、重试幂等（一个分片只积进总量一次）；
4. 两处均可辨别未足量阶段产物；中断恢复后两边读到的事实覆盖完整。

运行：cd backend && .venv/bin/python tests/verify_metric_grouping_refactor.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.analytics_engine import (  # noqa: E402
    MISSING_SKIP,
    MISSING_ZERO_FILL,
    SHARD_COMPUTING,
    SHARD_COMMITTED,
    SHARD_INTERRUPTED,
    SHARD_UNSTARTED,
    DimensionCombo,
    ShardCoordinator,
    build_cube_cell,
    compute_drill_levels,
    compute_metric,
    organize_measure_inputs,
)
from app.services.cube_builder import CubeBuilderService  # noqa: E402
from app.services.drill_path import DrillPathService  # noqa: E402

# ---------------------------------------------------------------------------
# 原始样本：同批事实，含缺值（缺失键/显式 None/不可解析）与合法零值
# ---------------------------------------------------------------------------

SAMPLE: list[dict] = [
    {"大区": "华东", "省份": "江苏", "城市": "南京", "门店": "N1", "GMV": 100, "订单数": 10, "客单价": 10.0},
    {"大区": "华东", "省份": "江苏", "城市": "南京", "门店": "N2", "GMV": 0, "订单数": 0, "客单价": 0},  # 合法零值
    {"大区": "华东", "省份": "浙江", "城市": "杭州", "门店": "H1", "GMV": 200, "订单数": 20},              # 缺「客单价」键
    {"大区": "华北", "省份": "北京", "城市": "北京", "门店": "B1", "GMV": None, "订单数": 5, "客单价": None},  # 显式 None
    {"大区": "华北", "省份": "北京", "城市": "北京", "门店": "B2", "GMV": "N/A", "订单数": "7", "客单价": 30},  # 不可解析 + 数字字符串
    {"大区": "华北", "省份": "天津", "城市": "天津", "门店": "T1", "订单数": 15, "客单价": 10.03},          # 缺「GMV」键
    {"省份": "广东", "城市": "广州", "门店": "G1", "GMV": 300, "订单数": 30, "客单价": 10},                  # 缺维度键 → "" 组
]

CUBE_DIMS = ["大区", "城市"]
CUBE_MEASURES = [
    {"name": "GMV", "aggregator": "SUM"},
    {"name": "订单数", "aggregator": "SUM"},
    {"name": "客单价", "aggregator": "AVG"},
]
DRILL_HIERARCHY = ["大区", "省份", "城市", "门店"]
DRILL_MEASURES = ["GMV", "订单数", "客单价"]


# ---------------------------------------------------------------------------
# 重构前的分组实现（逐行保留作对照物，验证旧事实分组行为稳定）
# ---------------------------------------------------------------------------

def legacy_build_cube_cell(raw_data, dimensions, measures):
    group_map = {}
    for row in raw_data:
        dim_key = tuple(str(row.get(d, "")) for d in dimensions)
        if dim_key not in group_map:
            group_map[dim_key] = {m["name"]: [] for m in measures}
        for m in measures:
            val = row.get(m["name"])
            if val is not None:
                try:
                    group_map[dim_key][m["name"]].append(float(val))
                except (ValueError, TypeError):
                    pass
    results = []
    for dim_key, measure_data in group_map.items():
        measure_values = {}
        total_rows = max(len(v) for v in measure_data.values()) if measure_data else 0
        for m in measures:
            values = measure_data.get(m["name"], [])
            measure_values[m["name"]] = compute_metric(values, m["aggregator"])
        results.append(DimensionCombo(
            dimensions=list(dim_key), measure_values=measure_values, row_count=total_rows))
    return results


def legacy_compute_drill_levels(data, drill_hierarchy, measures, aggregator="SUM"):
    results = []
    for level_idx, dim in enumerate(drill_hierarchy):
        group_map = {}
        for row in data:
            key = str(row.get(dim, ""))
            if key not in group_map:
                group_map[key] = {m: [] for m in measures}
            for m in measures:
                try:
                    group_map[key][m].append(float(row.get(m, 0)))
                except (ValueError, TypeError):
                    pass
        level_data = {"level": level_idx, "dimension": dim, "groups": []}
        for key, mdata in group_map.items():
            entry = {"key": key}
            for m in measures:
                values = mdata.get(m, [])
                entry[m] = round(compute_metric(values, aggregator), 2)
            level_data["groups"].append(entry)
        results.append(level_data)
    return results


# ---------------------------------------------------------------------------
# 1. 旧事实的分组行为应稳住
# ---------------------------------------------------------------------------

def test_legacy_grouping_stable() -> None:
    assert build_cube_cell(SAMPLE, CUBE_DIMS, CUBE_MEASURES) == \
        legacy_build_cube_cell(SAMPLE, CUBE_DIMS, CUBE_MEASURES), "立方分组结果与重构前不一致"
    assert compute_drill_levels(SAMPLE, DRILL_HIERARCHY, DRILL_MEASURES) == \
        legacy_compute_drill_levels(SAMPLE, DRILL_HIERARCHY, DRILL_MEASURES), "下钻分层聚合与重构前不一致"
    # 边界：空数据、空层级、空度量
    assert build_cube_cell([], CUBE_DIMS, CUBE_MEASURES) == legacy_build_cube_cell([], CUBE_DIMS, CUBE_MEASURES)
    assert compute_drill_levels([], DRILL_HIERARCHY, DRILL_MEASURES) == \
        legacy_compute_drill_levels([], DRILL_HIERARCHY, DRILL_MEASURES)
    assert compute_drill_levels(SAMPLE, [], DRILL_MEASURES) == legacy_compute_drill_levels(SAMPLE, [], DRILL_MEASURES)
    assert build_cube_cell(SAMPLE, CUBE_DIMS, []) == legacy_build_cube_cell(SAMPLE, CUBE_DIMS, [])
    print("PASS 1 旧事实分组行为稳定：立方与下钻输出和重构前逐组一致（含边界）")


# ---------------------------------------------------------------------------
# 2. 同批事实各自保留缺值/合法零值口径，层级缺值与总量对照原样本
# ---------------------------------------------------------------------------

def test_missing_and_zero_policy() -> None:
    # 共用输入整理：两种缺值策略的分组输入
    zero_fill = organize_measure_inputs(SAMPLE, ["大区"], ["客单价"], MISSING_ZERO_FILL)
    skip = organize_measure_inputs(SAMPLE, ["大区"], ["客单价"], MISSING_SKIP)
    assert zero_fill[("华东",)]["客单价"] == [10.0, 0.0, 0.0]   # 缺失键按合法零值计入
    assert skip[("华东",)]["客单价"] == [10.0, 0.0]             # 缺值不进入聚合输入

    # 下钻层级缺值对照原样本
    levels = compute_drill_levels(SAMPLE, DRILL_HIERARCHY, DRILL_MEASURES)
    region = {g["key"]: g for g in levels[0]["groups"]}
    assert region["华东"]["GMV"] == 300.0        # 100 + 0(合法零值) + 200
    assert region["华东"]["客单价"] == 10.0       # 10.0 + 0 + 缺失键→0.0
    assert region["华北"]["GMV"] == 0.0          # None 跳过、"N/A" 跳过、缺失键→0.0
    assert region["华北"]["订单数"] == 27.0       # 5 + "7" + 15
    assert region[""]["GMV"] == 300.0            # 缺维度键归入 "" 组
    # 总量对照原样本：可解析 GMV 之和 = 100+0+200+300
    assert round(sum(g["GMV"] for g in region.values()), 2) == 600.0

    # 立方口径：缺值不计入，合法零值保留
    cells = {tuple(c.dimensions): c for c in build_cube_cell(SAMPLE, CUBE_DIMS, CUBE_MEASURES)}
    assert cells[("华东", "南京")].measure_values["客单价"] == 5.0   # (10.0 + 0) / 2，合法零值进分母
    assert cells[("华东", "杭州")].measure_values["客单价"] == 0.0   # 缺失键不计入 → 空集 AVG
    assert cells[("华北", "北京")].measure_values["GMV"] == 0.0      # 缺值全部跳过
    assert cells[("华北", "北京")].row_count == 2                    # 订单数 [5, 7] 撑起行数
    assert cells[("华北", "天津")].measure_values["GMV"] == 0.0      # 缺失键不计入
    assert cells[("", "广州")].measure_values["GMV"] == 300.0
    print("PASS 2 缺值/合法零值口径各自保留，层级缺值与总量对照原样本一致")


# ---------------------------------------------------------------------------
# 3+4. 分片双消费方：下钻计算中立方来取、后写方中断、重试幂等、覆盖完整
# ---------------------------------------------------------------------------

def test_shard_interleaving_via_services() -> None:
    cube_svc = CubeBuilderService()
    drill_svc = DrillPathService()
    shard_id = "shard-verify-001"

    cube_entry, missing = cube_svc.create_entry({
        "立方名称": "分片验证立方", "事实表": "dwd_verify_fact",
        "维度列表": "大区, 城市", "预聚合度量": "GMV:SUM, 订单数:SUM, 客单价:AVG",
    })
    assert not missing
    cube_id = int(cube_entry["id"])
    drill_path_id = 1  # 种子「GMV地区下钻」：大区,省份,城市,门店 / GMV,订单数,客单价

    # 1) 下钻正在计算分片（同批事实登记到分片）
    started = drill_svc.start_shard_drill(drill_path_id, shard_id, SAMPLE)
    assert started["ok"] and started["state"] == SHARD_COMPUTING and started["attempts"] == 1

    # 2) 立方来取：下钻还未完成；同批事实可供立方处理
    fetched = cube_svc.fetch_shard(shard_id)
    assert fetched["ok"] and fetched["state"] == SHARD_UNSTARTED
    assert fetched["peers"]["drill"] == {"state": SHARD_COMPUTING, "ready": False}
    assert fetched["facts"] == SAMPLE

    # 3) 立方（后写方）开始构建，随后中断
    begun = cube_svc.start_shard_build(cube_id, shard_id, SAMPLE)
    assert begun["ok"] and begun["state"] == SHARD_COMPUTING
    interrupted = cube_svc.interrupt_shard(shard_id)
    assert interrupted["ok"] and interrupted["state"] == SHARD_INTERRUPTED

    # 4) 立方提交分片前，下钻读取它：能辨别立方的未足量阶段产物
    seen = drill_svc.read_shard(shard_id)
    assert seen["ok"] and seen["peers"]["cube"] == {"state": SHARD_INTERRUPTED, "ready": False}
    assert seen["facts"] == SAMPLE
    # 两处均可辨别未足量：下钻自己的暂存产物未足量；双方进度都挂着该分片
    drill_own = drill_svc.read_shard(shard_id)
    assert drill_own["ready"] is False and drill_own["product"] is not None
    cube_own = cube_svc.fetch_shard(shard_id)
    assert cube_own["ready"] is False and cube_own["product"] is not None
    assert cube_svc.shard_progress()["interrupted_shards"] == [shard_id]
    assert drill_svc.shard_progress()["pending_shards"] == [shard_id]
    assert cube_svc.shard_progress()["committed_facts"] == 0

    # 5) 中断恢复：立方重试并提交；重复提交幂等，一个分片只积进总量一次
    retried = cube_svc.start_shard_build(cube_id, shard_id, SAMPLE)
    assert retried["ok"] and retried["attempts"] == 2
    committed = cube_svc.commit_shard(shard_id)
    assert committed["ok"] and committed["accumulated"] is True
    assert committed["committed_facts"] == len(SAMPLE)
    duplicate = cube_svc.commit_shard(shard_id)
    assert duplicate["ok"] and duplicate["already_committed"] is True
    assert duplicate["accumulated"] is False
    assert duplicate["committed_facts"] == len(SAMPLE), "重试重复积进了总量"

    # 6) 下钻提交自己的阶段产物
    drill_committed = drill_svc.commit_shard(shard_id)
    assert drill_committed["ok"] and drill_committed["accumulated"] is True
    assert drill_committed["committed_facts"] == len(SAMPLE)

    # 7) 恢复后两边进度完整，读到的事实覆盖 == 原样本
    cube_progress = cube_svc.shard_progress()
    drill_progress = drill_svc.shard_progress()
    assert cube_progress["complete"] and cube_progress["committed_facts"] == len(SAMPLE)
    assert drill_progress["complete"] and drill_progress["committed_facts"] == len(SAMPLE)
    assert cube_svc.fetch_shard(shard_id)["facts"] == SAMPLE
    assert drill_svc.read_shard(shard_id)["facts"] == SAMPLE

    # 8) 两边提交的阶段产物与直接对原样本计算一致（层级缺值与总量对照）
    cube_product = cube_svc.fetch_shard(shard_id)["product"]
    drill_product = drill_svc.read_shard(shard_id)["product"]
    expected_cells = build_cube_cell(SAMPLE, CUBE_DIMS, CUBE_MEASURES)
    assert cube_product["cells"] == [
        {"dimensions": c.dimensions, "measures": c.measure_values, "row_count": c.row_count}
        for c in expected_cells
    ]
    assert drill_product["levels"] == compute_drill_levels(SAMPLE, DRILL_HIERARCHY, DRILL_MEASURES)
    print("PASS 3 分片双消费方：下钻计算中立方来取、后写方中断、重试只积进总量一次")
    print("PASS 4 两处均可辨别未足量阶段产物，恢复后两边事实覆盖完整且对照原样本一致")


# ---------------------------------------------------------------------------
# 协调器边界行为
# ---------------------------------------------------------------------------

def test_coordinator_edge_cases() -> None:
    coord = ShardCoordinator()
    # 未登记不能开始/读取
    assert coord.begin("cube", "ghost")["ok"] is False
    assert coord.read("cube", "ghost")["ok"] is False
    # 重复登记同批事实幂等；事实量冲突被拒绝
    assert coord.register_shard("s1", SAMPLE)["ok"] is True
    again = coord.register_shard("s1", SAMPLE)
    assert again["ok"] and again["already_registered"] is True
    assert coord.register_shard("s1", SAMPLE[:3])["ok"] is False
    # 未 begin 不能 commit / stage
    assert coord.commit("cube", "s1")["ok"] is False
    assert coord.stage_product("cube", "s1", {})["ok"] is False
    # 中断后重试只积进一次；已提交后中断不影响足量产物
    coord.begin("drill", "s1")
    coord.stage_product("drill", "s1", {"levels": []})
    coord.interrupt("drill", "s1")
    assert coord.read("drill", "s1")["state"] == SHARD_INTERRUPTED
    coord.begin("drill", "s1")
    coord.stage_product("drill", "s1", {"levels": ["retry"]})
    first = coord.commit("drill", "s1")
    retry = coord.commit("drill", "s1")
    assert first["accumulated"] is True and retry["accumulated"] is False
    assert coord.progress("drill")["committed_facts"] == len(SAMPLE)
    assert coord.interrupt("drill", "s1")["state"] == SHARD_COMMITTED
    # 另一消费方互不影响，且能辨别对方已足量
    assert coord.progress("cube")["committed_facts"] == 0
    assert coord.progress("cube")["pending_shards"] == ["s1"]
    assert coord.read("cube", "s1")["peers"]["drill"] == {"state": SHARD_COMMITTED, "ready": True}
    print("PASS 5 协调器边界：登记幂等/冲突拒绝、未开始不能提交、消费方互不影响")


if __name__ == "__main__":
    test_legacy_grouping_stable()
    test_missing_and_zero_policy()
    test_shard_interleaving_via_services()
    test_coordinator_edge_cases()
    print("\n全部验证通过。")
