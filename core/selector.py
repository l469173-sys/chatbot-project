from core.state import DialogueState

# 回傳格式：建議(建議型號/系列)、不建議、原因
RULES = [
    # 光強度 cd
    ({"measurement_metric": "intensity_cd", "usage_context": "field_quick"},
     {"recommend": ["SRI-2000LID"], "avoid": ["LID-060"], "reason": "現場快測以便攜式為主；暗箱式較適合一致性比較。"}),

    ({"measurement_metric": "intensity_cd", "usage_context": "production_qc"},
     {"recommend": ["LID-060", "LI-100"], "avoid": ["SRI-2000LID"], "reason": "產線/品保重視遮光與重複性，暗箱式更穩定。"}),

    # 光通量 lm
    ({"measurement_metric": "flux_lm", "usage_context": "field_quick"},
     {"recommend": ["SRI-2000LM"], "avoid": ["LM-ISP-XXXX"], "reason": "快速比較可用儀表型；系統型偏校正與可追溯。"}),

    ({"measurement_metric": "flux_lm", "usage_context": "rnd_lab"},
     {"recommend": ["LM-ISP-XXXX"], "avoid": ["SRI-2000LM"], "reason": "研發/校正需求建議系統型積分球方案。"}),

    # 輝度 cd/m2（接觸 vs 非接觸）
    ({"measurement_metric": "luminance_cd_m2", "usage_context": "production_qc"},
     {"recommend": ["SRI-RL-5000"], "avoid": ["SM-NE-2900", "SM-NE-3900"], "reason": "產線排除環境光干擾，接觸式重複性高。"}),

    ({"measurement_metric": "luminance_cd_m2", "usage_context": "rnd_lab"},
     {"recommend": ["SM-NE-2900", "SM-NE-3900"], "avoid": ["SRI-RL-5000"], "reason": "研發常需要非接觸下的亮度分佈/分析能力。"}),

    # UVC
    ({"measurement_object": "uvc_source", "measurement_metric": "radiant_irradiance"},
     {"recommend": ["SRI-4000UVC"], "avoid": ["SRI-2000UV", "HA-4000UVC"], "reason": "UVC 強度需用對應波段輻射量測設備。"}),

    ({"measurement_object": "uvc_source", "measurement_metric": "spectrum_color"},
     {"recommend": ["HA-4000UVC"], "avoid": ["SRI-4000UVC"], "reason": "光譜分析用於波長分佈，不等於強度或劑量。"}),

    # 植物燈
    ({"measurement_object": "plant_light"},
     {"recommend": ["SRI-PL-6000"], "avoid": ["一般照度計(lux)"], "reason": "植物照明以 PPF/PPFD（光子量）為核心。"}),

    # VCSEL
    ({"measurement_object": "vcsel"},
     {"recommend": ["VCSEL_measurement_system"], "avoid": ["一般 LED 照明量測"], "reason": "VCSEL 屬半導體雷射元件量測，需求不同。"}),
]

def select_products(state: DialogueState) -> dict:
    s = state.to_dict()

    for cond, out in RULES:
        ok = True
        for k, v in cond.items():
            if s.get(k) != v:
                ok = False
                break
        if ok:
            return {"recommend": out["recommend"], "avoid": out["avoid"], "reason": out["reason"]}

    # 找不到規則 → 回到追問（別亂推）
    return {
        "recommend": [],
        "avoid": [],
        "reason": "目前條件不足以安全推薦。請補充：量測距離/波段/被測物尺寸/是否需要自動化或校正級需求。"
    }
