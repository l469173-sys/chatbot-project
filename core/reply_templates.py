from core.state import DialogueState

def render_clarify(state: DialogueState, question: str) -> str:
    known = []
    if state.measurement_object: known.append(f"對象：{state.measurement_object}")
    if state.measurement_metric: known.append(f"指標：{state.measurement_metric}")
    if state.usage_context: known.append(f"情境：{state.usage_context}")

    prefix = "目前我已收到：" + "；".join(known) + "。\n" if known else ""
    return prefix + question

def render_recommendation(state: DialogueState, result: dict) -> str:
    header = (
        "我已確認你的需求：\n"
        f"- 對象：{state.measurement_object}\n"
        f"- 指標：{state.measurement_metric}\n"
        f"- 情境：{state.usage_context}\n"
    )

    if not result["recommend"]:
        return header + "\n" + result["reason"]

    rec = "\n".join([f"- {x}" for x in result["recommend"]])
    avoid = "\n".join([f"- {x}" for x in result["avoid"]]) if result["avoid"] else "（無）"

    return (
        header
        + "\n建議設備範圍：\n" + rec
        + "\n\n不建議：\n" + avoid
        + "\n\n原因：\n" + result["reason"]
        + "\n\n若要更精準到配置/規格，請補充：距離、波段、被測物尺寸、是否需要自動化或校正級需求。"
    )
