# system_docs 機器可讀 Metadata（權重與行為控制）

> 目的：
>
> * 讓 RAG 在「不看語意」的情況下，也能依 **規則優先權** 正確運作
> * 作為 system_docs 的 **機器可讀治理層**（不對外顯示）
> * 搭配 `_INDEX_SYSTEM_DOCS.md` 使用

---

## 一、全域原則（Global Rules）

```yaml
system_docs:
  role: governance_layer
  override_product_docs: true
  allow_generation_when_conflict: false
  conflict_resolution: "higher_priority_wins"
```

說明：

* system_docs 一律高於 product_structured
* 若規則衝突，禁止模型自行綜合生成

---

## 二、文件層級權重定義（Document Priority）

```yaml
doc_priority:
  100:
    - _INDEX_SYSTEM_DOCS.md

  90:
    - 客服機器人_產品選型決策樹_完整版.md
    - 網站客服機器人_功能與服務範圍說明.md

  85:
    - FAQ_光學量測_完整版.md

  70:
    - 光強度_vs_光通量_vs_照度_vs_輝度_差異說明.txt
    - 輝度量測_接觸式_vs_非接觸式_選型說明.txt
    - 光強度量測_便攜式_vs_暗箱式_選型說明.txt
    - 光通量量測_儀表型_vs_系統型_選型說明.txt

  65:
    - 積分球_角色_用途_與限制說明.txt
    - 積分球_內壁材質_波段差異與選型說明.txt
    - 測試光源_vs_量測儀_角色差異說明.txt

  60:
    - 光學材料量測_穿透率_vs_反射率_vs_亮度_差異說明.txt
    - 鏡面反射_vs_反射率_vs_穿透率_選型說明.txt
    - 螢光粉_vs_LED成品量測_差異說明.txt

  55:
    - 醫療燈_照度_vs_輻射照度_vs_輻射功率_差異說明.txt
    - UVC_光譜_vs_強度_vs_劑量_差異說明.txt
```

---

## 三、Answer Mode 行為約束（Answer Mode Gates）

```yaml
answer_mode_rules:
  selection:
    require:
      - measurement_object
      - measurement_metric
      - usage_context
    on_missing:
      mode: ask_clarifying
      allow_product_cards: false

  explanation:
    allow_product_cards: false

  faq_hit:
    priority_over_rag: true
    allow_product_cards: false

  refusal:
    force_reason: true
    allow_product_cards: false
```

說明：

* 條件不足時，**強制反問**
* FAQ 命中時，不顯示型號

---

## 四、產品卡片顯示規則（UI Binding Rules）

```yaml
product_card_rules:
  show_only_when:
    - answer_mode: suggest_range
    - conditions_complete: true

  hide_when:
    - answer_mode: ask_clarifying
    - answer_mode: refusal
    - answer_mode: explanation
```

---

## 五、高風險領域額外限制（High-Risk Guard）

```yaml
high_risk_domains:
  medical:
    allow_recommendation: false
    require_disclaimer: true

  uvc:
    allow_recommendation: false
    require_disclaimer: true
```

---

## 六、使用說明（給工程端）

* 本檔案應於 RAG 初始化時 **第一批載入**
* 權重數值可調整，但階層關係不可顛倒
* 不得與 `_INDEX_SYSTEM_DOCS.md` 分離使用

---

## 七、版本資訊

* 建立日期：2026-02-03
* 角色：system_docs 機器治理層
* 對外顯示：否
