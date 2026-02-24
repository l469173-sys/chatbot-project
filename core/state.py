from dataclasses import dataclass, asdict
import json
import os

STATE_DIR = os.path.join(os.path.dirname(__file__), "..", "data", ".sessions")
os.makedirs(STATE_DIR, exist_ok=True)

@dataclass
class DialogueState:
    measurement_object: str | None = None
    measurement_metric: str | None = None
    usage_context: str | None = None

    # 可選：進階欄位（未來擴充）
    band: str | None = None
    distance: str | None = None
    automation: str | None = None

    def update(self, **kwargs):
        for k, v in kwargs.items():
            if v is not None and hasattr(self, k):
                setattr(self, k, v)

    def to_dict(self):
        return asdict(self)

def _path(session_id: str) -> str:
    safe = "".join([c for c in session_id if c.isalnum() or c in ("-", "_")])[:64] or "default"
    return os.path.join(STATE_DIR, f"{safe}.json")

def load_state(session_id: str) -> DialogueState:
    p = _path(session_id)
    if not os.path.exists(p):
        return DialogueState()
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        return DialogueState(**data)
    except Exception:
        return DialogueState()

def save_state(session_id: str, state: DialogueState) -> None:
    p = _path(session_id)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(state.to_dict(), f, ensure_ascii=False, indent=2)
