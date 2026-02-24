# core/decision.py
# Decision Tree state machine
# - state is ALWAYS a dict (never custom object) to avoid AttributeError
# - step() returns DecisionResult, and when done=True, includes collected answers
#
# You can later replace TREE with loading from system_docs, but keep interface stable.

from __future__ import annotations

import time
import re
from dataclasses import dataclass
from typing import Any, Dict, Optional, List


@dataclass
class DecisionResult:
    state: Dict[str, Any]
    reply: str
    done: bool
    node_id: str
    answers: Dict[str, Any]


def _now() -> float:
    return time.time()


def _norm(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    return s


def _is_yes(s: str) -> bool:
    s = _norm(s).lower()
    return s in ["y", "yes", "是", "需要", "要", "有", "可以", "ok", "好"]


def _is_no(s: str) -> bool:
    s = _norm(s).lower()
    return s in ["n", "no", "否", "不需要", "不要", "沒有", "不", "不用"]


class DecisionEngine:
    """
    Minimal deterministic decision tree for optical measurement product selection.

    Node format:
      id: {
        "question": "...",
        "key": "answers dict key",
        "type": "choice|bool|text",
        "choices": {"label": "next_node_id", ...}  # for choice
        "next": "next_node_id"  # for bool/text
      }

    Terminal is when node_id == "__done__" or engine sets done=True.
    """

    START_NODE = "metric"

    TREE: Dict[str, Dict[str, Any]] = {
        "metric": {
            "question": (
                "我先幫你做選型整理。你主要想量測的指標是什麼？\n"
                "1) 光譜/波長\n"
                "2) 輝度/亮度\n"
                "3) 照度/輻照度\n"
                "4) 光通量/積分球相關\n"
                "5) 反射率/穿透率\n"
                "請回 1-5。"
            ),
            "key": "measurement_metric",
            "type": "choice",
            "choices": {
                "1": "band",
                "2": "target_object",
                "3": "target_object",
                "4": "needs_integrating_sphere",
                "5": "rf_surface",
            },
        },
        "band": {
            "question": (
                "你的量測波段主要是？\n"
                "1) UVC\n"
                "2) UVA/UVB\n"
                "3) 可見光 VIS\n"
                "4) 寬波段/不確定\n"
                "請回 1-4。"
            ),
            "key": "band",
            "type": "choice",
            "choices": {"1": "target_object", "2": "target_object", "3": "target_object", "4": "target_object"},
        },
        "needs_integrating_sphere": {
            "question": "你是否需要積分球（例如量光通量、總光輸出、或搭配積分球量測）？（是/否）",
            "key": "needs_integrating_sphere",
            "type": "bool",
            "next": "target_object",
        },
        "rf_surface": {
            "question": "你要量測的材質/表面比較偏哪一種？\n1) 鏡面/玻璃\n2) 一般材料/霧面\n請回 1-2。",
            "key": "surface_type",
            "type": "choice",
            "choices": {"1": "target_object", "2": "target_object"},
        },
        "target_object": {
            "question": (
                "你的量測對象是？（可直接描述）\n"
                "例如：UVC LED、醫療燈、植物燈、顯示器、背光模組、螢光粉、鏡面玻璃…"
            ),
            "key": "target_object",
            "type": "text",
            "next": "fixture_or_env",
        },
        "fixture_or_env": {
            "question": (
                "量測環境/治具需求是？\n"
                "1) 暗箱/遮光環境\n"
                "2) 一般實驗桌即可\n"
                "3) 不確定\n"
                "請回 1-3。"
            ),
            "key": "environment",
            "type": "choice",
            "choices": {"1": "budget", "2": "budget", "3": "budget"},
        },
        "budget": {
            "question": (
                "你的預算區間大概是？\n"
                "1) 低（入門）\n"
                "2) 中（標準）\n"
                "3) 高（高階/研發）\n"
                "4) 不確定\n"
                "請回 1-4。"
            ),
            "key": "budget",
            "type": "choice",
            "choices": {"1": "__done__", "2": "__done__", "3": "__done__", "4": "__done__"},
        },
    }

    def is_active(self, state: Dict[str, Any]) -> bool:
        return bool(state) and bool(state.get("active"))

    def _init_state(self) -> Dict[str, Any]:
        return {
            "active": True,
            "node_id": self.START_NODE,
            "answers": {},
            "updated_at": _now(),
        }

    def _get_node(self, node_id: str) -> Optional[Dict[str, Any]]:
        return self.TREE.get(node_id)

    def _help_text(self) -> str:
        return (
            "（你可以輸入：restart 重新開始、back 回上一題、done 直接結束並用目前條件推薦）"
        )

    def _set_answer(self, state: Dict[str, Any], key: str, value: Any) -> None:
        if "answers" not in state or not isinstance(state["answers"], dict):
            state["answers"] = {}
        state["answers"][key] = value

    def _go_next(self, state: Dict[str, Any], next_node_id: str) -> None:
        state["node_id"] = next_node_id
        state["updated_at"] = _now()

    def step(self, state: Dict[str, Any], user_input: str) -> DecisionResult:
        """
        If state inactive/empty -> start new tree and ask first question.
        If active -> parse answer and move to next node, or done.
        """
        ui = _norm(user_input)

        # Global commands
        if ui.lower() in ["restart", "重新開始", "/restart"]:
            state = self._init_state()
            node = self._get_node(state["node_id"])
            return DecisionResult(state=state, reply=(node["question"] + "\n" + self._help_text()), done=False,
                                 node_id=state["node_id"], answers=state["answers"])

        if not state or not self.is_active(state):
            state = self._init_state()
            node = self._get_node(state["node_id"])
            return DecisionResult(state=state, reply=(node["question"] + "\n" + self._help_text()), done=False,
                                 node_id=state["node_id"], answers=state["answers"])

        # Allow user to force finish
        if ui.lower() in ["done", "結束", "直接推薦", "/done"]:
            # mark done
            state["active"] = False
            return DecisionResult(state=state, reply="✅ 好的，我用目前條件開始推薦型號。", done=True,
                                 node_id=state.get("node_id", "__done__"), answers=state.get("answers", {}))

        node_id = str(state.get("node_id") or self.START_NODE)
        node = self._get_node(node_id)

        # If somehow invalid, restart safely
        if not node:
            state = self._init_state()
            node = self._get_node(state["node_id"])
            return DecisionResult(state=state, reply=(node["question"] + "\n" + self._help_text()), done=False,
                                 node_id=state["node_id"], answers=state["answers"])

        # Handle back (simple: restart for safety; can be improved with history)
        if ui.lower() in ["back", "上一題", "/back"]:
            # simplest safe behavior: restart
            state = self._init_state()
            node = self._get_node(state["node_id"])
            return DecisionResult(state=state, reply=("我先幫你重新整理一遍。\n" + node["question"] + "\n" + self._help_text()),
                                 done=False, node_id=state["node_id"], answers=state["answers"])

        qtype = node.get("type", "text")
        key = node.get("key", f"q_{node_id}")

        # Parse input -> set answer -> move next
        if qtype == "choice":
            choices: Dict[str, str] = node.get("choices") or {}
            if ui in choices:
                self._set_answer(state, key, ui)
                nxt = choices[ui]
                if nxt == "__done__":
                    state["active"] = False
                    return DecisionResult(state=state, reply="✅ 收到，我開始用你的條件推薦型號。", done=True,
                                         node_id="__done__", answers=state.get("answers", {}))
                self._go_next(state, nxt)
                next_node = self._get_node(state["node_id"])
                return DecisionResult(state=state, reply=(next_node["question"] + "\n" + self._help_text()), done=False,
                                     node_id=state["node_id"], answers=state.get("answers", {}))
            else:
                return DecisionResult(
                    state=state,
                    reply="我沒看懂你的選項，請依題目回覆指定的數字。\n" + node["question"] + "\n" + self._help_text(),
                    done=False,
                    node_id=node_id,
                    answers=state.get("answers", {}),
                )

        if qtype == "bool":
            if _is_yes(ui):
                self._set_answer(state, key, True)
            elif _is_no(ui):
                self._set_answer(state, key, False)
            else:
                return DecisionResult(
                    state=state,
                    reply="請回覆「是/否」。\n" + node["question"] + "\n" + self._help_text(),
                    done=False,
                    node_id=node_id,
                    answers=state.get("answers", {}),
                )
            nxt = node.get("next") or "__done__"
            if nxt == "__done__":
                state["active"] = False
                return DecisionResult(state=state, reply="✅ 收到，我開始用你的條件推薦型號。", done=True,
                                     node_id="__done__", answers=state.get("answers", {}))
            self._go_next(state, nxt)
            next_node = self._get_node(state["node_id"])
            return DecisionResult(state=state, reply=(next_node["question"] + "\n" + self._help_text()), done=False,
                                 node_id=state["node_id"], answers=state.get("answers", {}))

        # text
        if qtype == "text":
            if not ui:
                return DecisionResult(
                    state=state,
                    reply="請補充一句描述即可。\n" + node["question"] + "\n" + self._help_text(),
                    done=False,
                    node_id=node_id,
                    answers=state.get("answers", {}),
                )
            self._set_answer(state, key, ui)
            nxt = node.get("next") or "__done__"
            if nxt == "__done__":
                state["active"] = False
                return DecisionResult(state=state, reply="✅ 收到，我開始用你的條件推薦型號。", done=True,
                                     node_id="__done__", answers=state.get("answers", {}))
            self._go_next(state, nxt)
            next_node = self._get_node(state["node_id"])
            return DecisionResult(state=state, reply=(next_node["question"] + "\n" + self._help_text()), done=False,
                                 node_id=state["node_id"], answers=state.get("answers", {}))

        # fallback
        return DecisionResult(
            state=state,
            reply="系統決策樹節點設定異常，我先用目前條件直接推薦型號。",
            done=True,
            node_id=node_id,
            answers=state.get("answers", {}),
        )

    def build_recommendation_query(self, answers: Dict[str, Any]) -> str:
        """
        Turn collected answers into a product-search query string.
        This is the key that connects Decision Tree -> VectorDB (products only).
        """
        if not answers or not isinstance(answers, dict):
            return ""

        parts: List[str] = []

        metric = answers.get("measurement_metric")
        metric_map = {
            "1": "光譜 波長 光譜儀",
            "2": "輝度 亮度 亮度計 輝度計",
            "3": "照度 輻照度 照度計 輻照度計",
            "4": "光通量 積分球",
            "5": "反射率 穿透率 鏡面 玻璃",
        }
        if metric in metric_map:
            parts.append(metric_map[metric])

        band = answers.get("band")
        band_map = {"1": "UVC", "2": "UVA UVB", "3": "VIS 可見光", "4": "寬波段"}
        if band in band_map:
            parts.append(band_map[band])

        needs_is = answers.get("needs_integrating_sphere")
        if needs_is is True:
            parts.append("需要 積分球")
        elif needs_is is False:
            parts.append("不一定要 積分球")

        surface = answers.get("surface_type")
        if surface == "1":
            parts.append("鏡面 玻璃")
        elif surface == "2":
            parts.append("霧面 一般材料")

        obj = answers.get("target_object")
        if obj:
            parts.append(str(obj))

        env = answers.get("environment")
        env_map = {"1": "暗箱 遮光", "2": "桌上型", "3": "通用"}
        if env in env_map:
            parts.append(env_map[env])

        budget = answers.get("budget")
        budget_map = {"1": "入門", "2": "標準", "3": "高階 研發", "4": "不確定預算"}
        if budget in budget_map:
            parts.append(budget_map[budget])

        # Add stable anchor tokens to bias product docs
        parts.append("產品 型號 規格")

        # Join
        q = " ".join([p for p in parts if p])
        return _norm(q)
