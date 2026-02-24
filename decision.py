# decision.py (3Q Smart Decision Tree + Skip/Unknown Friendly + Hit-rate Booster)
import re
from typing import Any, Dict, List, Tuple


# -----------------------------
# Basic utils
# -----------------------------
def _norm(text: str) -> str:
    return (text or "").strip()


def _lower(text: str) -> str:
    return _norm(text).lower()


def _has_any(text: str, keys: List[str]) -> bool:
    t = _lower(text)
    return any((k or "").lower() in t for k in keys if k)


MODEL_RE = re.compile(r"\b([A-Za-z]{1,}\-?\d{2,}[A-Za-z0-9\-]*)\b")
VS_RE = re.compile(r"\b(vs\.?|versus)\b", re.IGNORECASE)

# numbers / units / wavelength
NM_RE = re.compile(r"(\d{2,4})\s*nm\b", re.IGNORECASE)
UNIT_RE = re.compile(
    r"\b("
    r"lux|lx|"
    r"cd/m2|cd/m\^2|nit|nits|"
    r"w/m2|w/m\^2|mw/cm2|mw/cm\^2|"
    r"uw/cm2|uw/cm\^2|"
    r"μw/cm2|μw/cm\^2|"
    r"umol/m2/s|μmol/m2/s|μmol/m\^2/s"
    r")\b",
    re.IGNORECASE,
)


def _extract_models(text: str) -> List[str]:
    t = _norm(text)
    if not t:
        return []
    found = MODEL_RE.findall(t)
    out: List[str] = []
    seen = set()
    for m in found:
        m = (m or "").strip()
        if not m:
            continue
        k = m.lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(m)
    return out


def _has_model_token(text: str) -> bool:
    # 只要出現一個型號 token 就算
    return bool(_extract_models(text))


def _looks_like_model_compare(text: str) -> bool:
    """
    若是「兩台以上型號」或 A vs B 形式，通常不進 decision，直接回答比較更好
    """
    t = _norm(text)
    if not t:
        return False
    models = _extract_models(t)
    if len(models) >= 2:
        return True
    if VS_RE.search(t) and len(models) >= 1:
        return True
    # 常見中文比較語意
    if _has_any(t, ["比較", "差異", "哪個好", "哪個較好", "差別"]):
        if len(models) >= 1:
            return True
    return False


def _is_control(text: str) -> str:
    """
    回傳控制指令： 'reset' | 'cancel' | ''
    """
    t = _lower(text)
    if not t:
        return ""
    if any(k in t for k in ["重來", "重新", "reset", "restart"]):
        return "reset"
    if any(k in t for k in ["取消", "停止", "cancel", "stop", "結束"]):
        return "cancel"
    return ""


# -----------------------------
# Entry decision
# -----------------------------
def should_enter_decision(user_text: str) -> bool:
    """
    進入決策樹條件：
    - 有推薦/選型/怎麼選 等意圖
    - 但「明確型號比較」通常直接回答比較更好 -> 不進
    - 若使用者已給型號但仍強烈要求選型/推薦 -> 仍可進
    """
    t = _norm(user_text)
    if not t:
        return False

    # 型號比較：不進 decision（直接走一般回答/比較）
    if _looks_like_model_compare(t):
        return False

    # 若使用者已給型號，通常不需要 3Q
    if _has_model_token(t):
        if _has_any(t, ["選型", "推薦", "幫我選", "適合哪個", "挑哪台", "要哪一台"]):
            return True
        return False

    triggers = [
        "選型", "推薦", "怎麼選", "我要買", "適合哪個", "挑哪台", "規劃", "需求",
        "要選", "要買", "建議型號", "要哪一台", "幫我選", "給我型號", "配一台",
    ]
    return _has_any(t, triggers)


# -----------------------------
# State
# -----------------------------
def new_state() -> Dict[str, Any]:
    return {"active": True, "step": 0, "answers": {}, "finished": False, "last_question": ""}


def _is_unknown(text: str, question_key: str = "") -> bool:
    """
    unknown/skip friendly：使用空字串代表未知
    注意：Q3（限制）回答「沒有」其實是有效訊息，不能當 unknown
    """
    t = _lower(text)
    if not t:
        return True

    # 「沒有」在限制題多半表示無限制
    if question_key == "scene_constraints":
        if t in ("沒有", "無", "沒", "no", "none"):
            return False

    unknown_tokens = [
        "不知道", "不確定", "都可以", "隨便", "沒想法", "不清楚",
        "略過", "跳過", "skip", "na", "n/a",
    ]
    # 單獨 "-" 或 "none" 仍視為 unknown
    if t in ("-", "none"):
        return True

    return any(tok in t for tok in unknown_tokens)


def _questions() -> List[Dict[str, Any]]:
    # ✅ 3 題：資訊密度高、但不拖
    return [
        {
            "key": "target",
            "q": "你要量什麼？（例：光譜/UVA-UVB-UVC/VIS、輝度/照度、UVC 輻照度/光強度、PPFD/PPF/PAR、積分球總光通量、反射率/穿透率、螢光粉…）",
        },
        {
            "key": "object_band",
            "q": "量測對象與主要波段？（例：UVC LED、醫療燈、顯示器、玻璃/鏡面；波段 UVC/UVB/UVA/VIS/NIR；或 275nm/365nm；不確定也可）",
        },
        {
            "key": "scene_constraints",
            "q": "使用情境與限制？（研發/產線/品管/客製；是否要速度/自動化/報表；預算或尺寸限制；沒有就回「沒有」）",
        },
    ]


def advance(state: Dict[str, Any], user_text: str) -> Tuple[Dict[str, Any], str]:
    qs = _questions()

    cmd = _is_control(user_text)
    if cmd == "reset":
        state.clear()
        state.update(new_state())
        state["last_question"] = qs[0]["q"]
        return state, "好的，我們重來一次。\n\nQ1️⃣ " + qs[0]["q"]
    if cmd == "cancel":
        state["active"] = False
        state["finished"] = False
        state["last_question"] = ""
        return state, "已取消選型流程。你可以直接問我產品規格/比較或公司資訊。"

    # 初次進入：先問 Q1
    if state.get("step", 0) <= 0 and not state.get("last_question"):
        state["step"] = 0
        state["last_question"] = qs[0]["q"]
        return state, "我用 3 個問題幫你快速選型。\n\nQ1️⃣ " + qs[0]["q"]

    step = int(state.get("step", 0))
    if 0 <= step < len(qs):
        key = qs[step]["key"]
        ans = _norm(user_text)
        state.setdefault("answers", {})[key] = "" if _is_unknown(ans, question_key=key) else ans

    step += 1
    state["step"] = step

    if step >= len(qs):
        state["finished"] = True
        state["active"] = False
        state["last_question"] = ""
        return state, "收到。我整理需求後，直接給你 2~3 個候選型號、差異與建議。"

    nq = qs[step]["q"]
    state["last_question"] = nq
    return state, f"Q{step+1}️⃣ {nq}"


# -----------------------------
# Recommendation query terms
# -----------------------------
def build_recommendation_query_terms(state: Dict[str, Any]) -> List[str]:
    """
    把 3 題答案轉成檢索關鍵字（讓 app.py 用它去做 RAG/DB/BM25 搜尋）
    - 抽取：核心領域詞、波段/數字 nm、單位、場景詞
    - 產生：短詞 + 組合詞 + raw 摘要（保底）
    """
    ans = state.get("answers") or {}
    parts = [str(v).strip() for v in ans.values() if str(v).strip()]
    raw = re.sub(r"\s+", " ", " ".join(parts)).strip()
    if not raw:
        return []

    # ✅ 核心詞彙：補齊你常見的命中低問題
    vocab = [
        # bands
        "UVC", "UVB", "UVA", "VIS", "NIR", "IR", "PAR",
        # plant lighting
        "PPFD", "PPF", "PAR", "光子通量", "光合光子", "植物照明", "植物燈",
        # irradiance / intensity
        "輻照度", "irradiance", "radiometer", "光強度", "強度", "照度", "lux", "illuminance",
        # luminance
        "輝度", "luminance", "亮度", "nit", "cd/m2",
        # spectrum
        "光譜", "spectrum", "spectrometer", "波長",
        # integrating sphere / flux
        "積分球", "總光通量", "光通量", "lumen", "lm",
        # reflect / transmit
        "反射率", "reflectance", "穿透率", "透過率", "transmittance", "鏡面", "玻璃",
        # phosphor
        "螢光粉", "phosphor",
        # objects / industries
        "UVC LED", "LED", "雷射", "laser", "顯示器", "display", "醫療燈",
        # scenario
        "產線", "品管", "研發", "自動化", "報表", "暗箱", "校正", "校準",
    ]

    lower = raw.lower()

    keep: List[str] = []
    for w in vocab:
        if w.lower() in lower:
            keep.append(w)

    # ✅ 抽取 nm
    nms = NM_RE.findall(raw)
    for nm in nms[:6]:
        keep.append(f"{nm}nm")

    # ✅ 抽取單位
    units = UNIT_RE.findall(raw)
    for u in units[:6]:
        keep.append(u)

    # ✅ 組合詞（提高檢索命中：例如 "UVC 輻照度", "PPFD 植物照明"）
    combos: List[str] = []
    if any(x.lower() in lower for x in ["uvc", "uvb", "uva"]):
        if any(x.lower() in lower for x in ["輻照度", "irradiance", "radiometer", "光強度", "強度"]):
            combos.append("UVC 輻照度")
        combos.append("紫外線 光譜")
    if any(x.lower() in lower for x in ["ppfd", "ppf", "par", "植物"]):
        combos.append("PPFD 植物照明")
        combos.append("PAR 光子通量")

    keep.extend(combos)

    # ✅ 保底：完整需求摘要（BM25/向量都吃得到）
    keep.append(raw)

    out: List[str] = []
    seen = set()
    for x in keep:
        x = (x or "").strip()
        if not x:
            continue
        k = x.lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(x)

    return out[:12]
