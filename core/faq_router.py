import re

FAQ_RULES = [
    # 報價/庫存/交期 → 直接拒答引導
    (re.compile(r"(報價|價格|多少錢|庫存|交期|多久到|現貨)"), 
     "我可以提供產品與選型方向，但不提供報價、庫存或交期資訊。請聯絡業務窗口協助。"),

    # UVC 三指標混用
    (re.compile(r"(UVC).*(lux|流明|lm|照度)"), 
     "UVC 領域通常需分清「光譜 / 強度 / 劑量」，lux 或流明不適合作為 UVC 殺菌能量評估指標。你要確認的是：光譜、瞬時強度，還是累積劑量？"),

    # 植物燈指標
    (re.compile(r"(植物燈|PPFD|PPF).*(lux|照度)"), 
     "植物照明的核心指標是 PPF/PPFD（光子量），lux（照度）不等於植物可用光。你要量的是 PPF、PPFD，還是光譜分佈？"),
]

def try_faq(user_text: str) -> str | None:
    t = user_text.strip()
    for pattern, reply in FAQ_RULES:
        if pattern.search(t):
            return reply
    return None
