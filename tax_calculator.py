"""
부동산 투자 수익률 및 세금 자동 계산 모듈
2026년 기준 세율 적용

세목:
- 취득세 (지방세법, 지방교육세, 농어촌특별세 포함)
- 양도소득세 (장기보유특별공제, 1세대1주택 비과세 포함)
- 투자 수익률 (레버리지 포함 ROE, 연환산 수익률)
"""


def calc_acquisition_tax(price_만원, num_homes=1, area_m2=85.0, is_first_home=False, is_adjusted_zone=True):
    """
    취득세 계산 (2026년 기준)

    Args:
        price_만원  : 매수가 (만원)
        num_homes   : 매수 후 보유 주택 수 (1=1주택, 2=2주택, 3=3주택 이상)
        area_m2     : 전용면적 (m2)
        is_first_home : 생애최초 여부 (True/False)
        is_adjusted_zone : 조정대상지역 여부 (True/False)

    Returns:
        dict {
            취득세율(%), 취득세(만원),
            지방교육세(만원), 농어촌특별세(만원),
            합계(만원), 취득세율_설명
        }

    2026년 취득세 기준:
    ■ 1주택 (일반):
      - 6억 이하     : 1%
      - 6억 초과~9억 : 1% + (취득가-6억)/3억 * 2%  (구간별 선형 증가, 최대 3%)
      - 9억 초과     : 3%
    ■ 2주택 (조정대상지역): 8%
    ■ 2주택 (비조정)     : 1~3% (1주택과 동일 세율 적용)
    ■ 3주택 이상 (조정)  : 12%
    ■ 3주택 이상 (비조정): 8%
    ■ 법인               : 12%

    ■ 생애최초:
      - 1.5억 이하   : 취득세 면제
      - 1.5억~12억   : 취득세 200만원 한도로 감면 (실질 50% 감면 효과)
      - 12억 초과    : 감면 없음

    ■ 지방교육세: 취득세율 2% 초과분에는 0.3%, 2% 이하분에는 0.2%
       (실무 간소화: 취득세의 10% 적용)
    ■ 농어촌특별세: 전용 85m2 이하 면제, 초과 시 취득세의 10%
    """
    price_억 = price_만원 / 10000  # 만원 → 억원 환산

    # ── 1. 기본 취득세율 결정 ──────────────────────────────────────────────
    if num_homes >= 3:
        # 3주택 이상
        if is_adjusted_zone:
            base_rate = 0.12   # 12%
            rate_desc = "3주택 이상 (조정대상지역) 12%"
        else:
            base_rate = 0.08   # 8%
            rate_desc = "3주택 이상 (비조정지역) 8%"
    elif num_homes == 2:
        if is_adjusted_zone:
            base_rate = 0.08   # 8%
            rate_desc = "2주택 (조정대상지역) 8%"
        else:
            # 비조정 2주택: 1주택과 동일
            if price_억 <= 6:
                base_rate = 0.01
                rate_desc = "2주택 (비조정, 6억 이하) 1%"
            elif price_억 <= 9:
                base_rate = 0.01 + (price_억 - 6) / 3 * 0.02
                rate_desc = f"2주택 (비조정, 6~9억) {base_rate*100:.2f}%"
            else:
                base_rate = 0.03
                rate_desc = "2주택 (비조정, 9억 초과) 3%"
    else:
        # 1주택 (일반 세율)
        if price_억 <= 6:
            base_rate = 0.01
            rate_desc = "1주택 6억 이하 1%"
        elif price_억 <= 9:
            base_rate = 0.01 + (price_억 - 6) / 3 * 0.02
            rate_desc = f"1주택 6~9억 구간 {base_rate*100:.2f}%"
        else:
            base_rate = 0.03
            rate_desc = "1주택 9억 초과 3%"

    # ── 2. 취득세 원금액 계산 ─────────────────────────────────────────────
    acq_tax = price_만원 * base_rate

    # ── 3. 생애최초 감면 (1주택 한정, 12억 이하) ─────────────────────────
    first_home_discount = 0.0
    if is_first_home and num_homes == 1:
        if price_억 <= 1.5:
            # 전액 면제
            first_home_discount = acq_tax
            rate_desc += " (생애최초 면제)"
        elif price_억 <= 12:
            # 200만원 한도 감면
            first_home_discount = min(acq_tax, 200)
            rate_desc += f" (생애최초 {first_home_discount:.0f}만원 감면)"
        # 12억 초과는 감면 없음

    acq_tax_after_discount = max(0, acq_tax - first_home_discount)

    # ── 4. 지방교육세 (취득세의 10% — 실무 간소화) ───────────────────────
    edu_tax = acq_tax_after_discount * 0.10

    # ── 5. 농어촌특별세 (전용 85m2 이하 면제, 초과 시 취득세의 10%) ──────
    if area_m2 <= 85.0:
        rural_tax = 0.0
    else:
        rural_tax = acq_tax_after_discount * 0.10

    total_tax = acq_tax_after_discount + edu_tax + rural_tax
    effective_rate = (total_tax / price_만원 * 100) if price_만원 > 0 else 0

    return {
        "취득세율(%)": round(base_rate * 100, 4),
        "취득세(만원)": round(acq_tax_after_discount, 1),
        "지방교육세(만원)": round(edu_tax, 1),
        "농어촌특별세(만원)": round(rural_tax, 1),
        "합계(만원)": round(total_tax, 1),
        "실효세율(%)": round(effective_rate, 4),
        "취득세율_설명": rate_desc,
    }


def calc_capital_gains_tax(
    buy_price_만원,
    sell_price_만원,
    holding_years,
    residence_years=0,
    is_one_home=True,
    num_homes=1,
    deductible_costs_만원=0,
):
    """
    양도소득세 계산 (2026년 기준)

    Args:
        buy_price_만원     : 매수가 (만원)
        sell_price_만원    : 매도가 (만원)
        holding_years      : 보유 기간 (년, 소수점 가능)
        residence_years    : 거주 기간 (년, 소수점 가능)
        is_one_home        : 1세대 1주택 여부
        num_homes          : 양도 시점 주택 수 (중과세 판단)
        deductible_costs_만원 : 필요경비 (만원, 중개수수료+취득세+인테리어 등)

    Returns:
        dict {
            양도차익(만원), 장기보유특별공제액(만원), 장기보유특별공제율(%),
            과세표준(만원), 세율(%), 양도세(만원), 지방소득세(만원),
            합계(만원), 비과세여부, 비과세사유, 상세내역
        }

    2026년 양도세 기준:
    ■ 1세대1주택 비과세 요건:
      - 보유 2년 이상 + 조정지역은 거주 2년 이상
      - 양도가액 12억 이하 → 전액 비과세
      - 양도가액 12억 초과 → 12억 초과분만 과세

    ■ 장기보유특별공제 (1세대1주택 거주자):
      - 보유 연 2% + 거주 연 4% (최대 80%)
      - 단, 거주 2년 미만 시 일반 공제 적용 (보유 연 2%, 최대 30%)

    ■ 장기보유특별공제 (1세대1주택 외):
      - 보유 3년 이상: 연 2%, 최대 30%

    ■ 기본세율 (양도소득):
      과세표준      세율  누진공제
      ~1,400만      6%    0
      ~5,000만      15%   126만
      ~8,800만      24%   522만
      ~1.5억        35%   1,490만
      ~3억          38%   1,940만
      ~5억          40%   2,540만
      ~10억         42%   3,540만
      10억 초과     45%   6,540만

    ■ 중과세율:
      - 2주택 (조정지역): 기본세율 + 20%p
      - 3주택 이상     : 기본세율 + 30%p
      ※ 2026년 현재 한시적 중과 배제 시행 중 (기본세율 적용)
        → 이 계산기는 보수적으로 중과세 포함 계산
    """

    gain = sell_price_만원 - buy_price_만원 - deductible_costs_만원

    # 양도차익이 없으면 세금 없음
    if gain <= 0:
        return {
            "양도차익(만원)": round(gain, 1),
            "장기보유특별공제액(만원)": 0.0,
            "장기보유특별공제율(%)": 0.0,
            "과세표준(만원)": 0.0,
            "세율(%)": 0.0,
            "양도세(만원)": 0.0,
            "지방소득세(만원)": 0.0,
            "합계(만원)": 0.0,
            "비과세여부": False,
            "비과세사유": "양도차익 없음",
            "상세내역": {},
        }

    # ── 1세대1주택 비과세 판단 ──────────────────────────────────────────────
    tax_exempt = False
    tax_exempt_reason = ""
    taxable_gain = gain  # 실제 과세 대상 차익

    if is_one_home and num_homes == 1:
        holding_ok = holding_years >= 2
        residence_ok = residence_years >= 2  # 조정지역 기준 (보수적 적용)

        if holding_ok and residence_ok:
            sell_억 = sell_price_만원 / 10000
            if sell_억 <= 12:
                # 전액 비과세
                tax_exempt = True
                tax_exempt_reason = "1세대1주택 비과세 (보유 2년+, 거주 2년+, 12억 이하)"
                return {
                    "양도차익(만원)": round(gain, 1),
                    "장기보유특별공제액(만원)": 0.0,
                    "장기보유특별공제율(%)": 0.0,
                    "과세표준(만원)": 0.0,
                    "세율(%)": 0.0,
                    "양도세(만원)": 0.0,
                    "지방소득세(만원)": 0.0,
                    "합계(만원)": 0.0,
                    "비과세여부": True,
                    "비과세사유": tax_exempt_reason,
                    "상세내역": {"비과세한도": "12억 이하 전액 비과세"},
                }
            else:
                # 12억 초과분만 과세
                # 과세양도차익 = 전체 양도차익 × (양도가액 - 12억) / 양도가액
                tax_exempt_reason = "1세대1주택 12억 초과분 부분과세"
                taxable_ratio = (sell_price_만원 - 120000) / sell_price_만원
                taxable_gain = gain * taxable_ratio
        elif holding_ok and not residence_ok:
            tax_exempt_reason = "거주기간 2년 미만 → 비과세 불가"
        elif not holding_ok:
            tax_exempt_reason = "보유기간 2년 미만 → 비과세 불가"

    # ── 장기보유특별공제율 계산 ───────────────────────────────────────────
    holding_yrs_int = int(holding_years)

    if is_one_home and num_homes == 1 and residence_years >= 2:
        # 1세대1주택 + 거주 2년 이상: 보유 연 2% + 거주 연 4%
        holding_deduct = min(holding_yrs_int * 0.02, 0.40)   # 최대 40%
        residence_deduct = min(int(residence_years) * 0.04, 0.40)  # 최대 40%
        ltg_rate = min(holding_deduct + residence_deduct, 0.80)    # 합산 최대 80%
        ltg_desc = f"1세대1주택 장특공 (보유{holding_yrs_int}년 {holding_deduct*100:.0f}% + 거주{int(residence_years)}년 {residence_deduct*100:.0f}%)"
    elif holding_yrs_int >= 3:
        # 일반 장기보유특별공제: 보유 3년 이상, 연 2%, 최대 30%
        ltg_rate = min((holding_yrs_int - 2) * 0.02, 0.30)
        ltg_desc = f"일반 장특공 보유{holding_yrs_int}년 {ltg_rate*100:.0f}%"
    else:
        ltg_rate = 0.0
        ltg_desc = "장기보유특별공제 미적용 (보유 3년 미만)"

    ltg_amount = taxable_gain * ltg_rate
    taxable_after_ltg = max(0, taxable_gain - ltg_amount)

    # ── 기본 공제 250만원 차감 ─────────────────────────────────────────────
    basic_deduction = 250  # 연 250만원 기본공제
    taxable_base = max(0, taxable_after_ltg - basic_deduction)

    # ── 세율 결정 및 양도세 계산 ──────────────────────────────────────────
    # 중과세 여부 (2026년 기준: 조정지역 2주택+20%p, 3주택+30%p)
    # ※ 2024.01.01~2025.05.09 한시 중과 배제가 연장될 수 있으나 보수적으로 적용
    surcharge = 0
    surcharge_desc = ""
    if num_homes == 2:
        surcharge = 0.20
        surcharge_desc = "2주택 중과(+20%p)"
    elif num_homes >= 3:
        surcharge = 0.30
        surcharge_desc = "3주택 이상 중과(+30%p)"

    # 기본세율 계산
    brackets = [
        (1400,    0.06, 0),
        (5000,    0.15, 126),
        (8800,    0.24, 522),
        (15000,   0.35, 1490),
        (30000,   0.38, 1940),
        (50000,   0.40, 2540),
        (100000,  0.42, 3540),
        (float("inf"), 0.45, 6540),
    ]

    base_tax = 0.0
    marginal_rate = 0.0
    for limit, rate, deduction in brackets:
        if taxable_base <= limit:
            base_tax = taxable_base * rate - deduction
            marginal_rate = rate
            break

    # 중과세 추가 (중과세액 = 과세표준 × 중과세율)
    surcharge_tax = taxable_base * surcharge

    capital_gains_tax = max(0, base_tax + surcharge_tax)
    local_income_tax = capital_gains_tax * 0.10  # 지방소득세 10%

    total_tax = capital_gains_tax + local_income_tax
    effective_rate_pct = (total_tax / gain * 100) if gain > 0 else 0

    detail = {
        "양도차익_전체(만원)": round(gain, 1),
        "과세대상_양도차익(만원)": round(taxable_gain, 1),
        "장특공_설명": ltg_desc,
        "장특공_차감액(만원)": round(ltg_amount, 1),
        "기본공제(만원)": basic_deduction,
        "과세표준(만원)": round(taxable_base, 1),
        "적용세율": f"{(marginal_rate + surcharge)*100:.0f}% (기본 {marginal_rate*100:.0f}% + 중과 {surcharge*100:.0f}%)",
        "중과세_설명": surcharge_desc if surcharge_desc else "중과세 없음",
        "실효세율(%)": round(effective_rate_pct, 2),
    }
    if tax_exempt_reason:
        detail["비과세_설명"] = tax_exempt_reason

    return {
        "양도차익(만원)": round(gain, 1),
        "장기보유특별공제액(만원)": round(ltg_amount, 1),
        "장기보유특별공제율(%)": round(ltg_rate * 100, 1),
        "과세표준(만원)": round(taxable_base, 1),
        "세율(%)": round((marginal_rate + surcharge) * 100, 1),
        "양도세(만원)": round(capital_gains_tax, 1),
        "지방소득세(만원)": round(local_income_tax, 1),
        "합계(만원)": round(total_tax, 1),
        "비과세여부": tax_exempt,
        "비과세사유": tax_exempt_reason,
        "상세내역": detail,
    }


def calc_investment_return(
    buy_price_만원,
    sell_price_만원,
    holding_years,
    loan_amount_만원=0,
    loan_rate_pct=3.5,
    monthly_income_만원=0,
    annual_expenses_만원=0,
    acquisition_tax_만원=0,
    capital_gains_tax_만원=0,
):
    """
    투자 수익률 계산 (레버리지 포함)

    Args:
        buy_price_만원       : 매수가 (만원)
        sell_price_만원      : 매도가 (만원)
        holding_years        : 보유 기간 (년)
        loan_amount_만원     : 대출금액 (만원)
        loan_rate_pct        : 대출금리 (%, 예: 3.5)
        monthly_income_만원  : 월 임대소득 (만원, 없으면 0)
        annual_expenses_만원 : 연간 유지비 (만원, 재산세+관리비+수선비 등)
        acquisition_tax_만원 : 취득세 합계 (만원, calc_acquisition_tax 결과)
        capital_gains_tax_만원 : 양도세 합계 (만원, calc_capital_gains_tax 결과)

    Returns:
        dict {
            자기자본(만원), 총수익(만원), 순수익(만원),
            ROE(%), 연환산ROE(%), 총비용(만원),
            이자비용(만원), 임대수입(만원), 손익분기매도가(만원),
            상세내역
        }
    """
    # 자기자본 = 매수가 + 취득세 - 대출금
    equity_만원 = buy_price_만원 + acquisition_tax_만원 - loan_amount_만원
    equity_만원 = max(equity_만원, 1)  # 0 나누기 방지

    # 이자 비용 합계 (단순 이자, 거치식 기준)
    annual_interest = loan_amount_만원 * (loan_rate_pct / 100)
    total_interest = annual_interest * holding_years

    # 임대 수입 합계
    total_rental_income = monthly_income_만원 * 12 * holding_years

    # 유지비 합계
    total_expenses = annual_expenses_만원* holding_years

    # 총 비용
    total_cost = acquisition_tax_만원 + total_interest + total_expenses + capital_gains_tax_만원

    # 총 수익 (가격차익 + 임대수입)
    price_gain = sell_price_만원 - buy_price_만원
    gross_profit = price_gain + total_rental_income

    # 순수익
    net_profit = gross_profit - total_cost

    # ROE (자기자본 대비 순수익)
    roe_pct = (net_profit / equity_만원) * 100

    # 연환산 ROE (CAGR 방식)
    if holding_years > 0 and equity_만원 > 0:
        # 최종가치 = 자기자본 + 순수익
        final_value = equity_만원 + net_profit
        if final_value > 0:
            cagr = ((final_value / equity_만원) ** (1 / holding_years) - 1) * 100
        else:
            cagr = -100.0
    else:
        cagr = 0.0

    # 손익분기 매도가: 모든 비용을 커버하는 최소 매도가
    # 단, 양도세는 매도가에 따라 달라지므로 간략화하여 취득세+이자+유지비만 사용
    breakeven_price = buy_price_만원 + acquisition_tax_만원 + total_interest + total_expenses - total_rental_income

    detail = {
        "매수가(만원)": round(buy_price_만원, 0),
        "매도가(만원)": round(sell_price_만원, 0),
        "보유기간(년)": holding_years,
        "자기자본(만원)": round(equity_만원, 0),
        "대출금(만원)": round(loan_amount_만원, 0),
        "취득세(만원)": round(acquisition_tax_만원, 1),
        "이자비용_합계(만원)": round(total_interest, 1),
        "임대수입_합계(만원)": round(total_rental_income, 1),
        "유지비_합계(만원)": round(total_expenses, 1),
        "양도세(만원)": round(capital_gains_tax_만원, 1),
        "총비용(만원)": round(total_cost, 1),
        "가격차익(만원)": round(price_gain, 1),
        "총수익(만원)": round(gross_profit, 1),
        "순수익(만원)": round(net_profit, 1),
    }

    return {
        "자기자본(만원)": round(equity_만원, 0),
        "총수익(만원)": round(gross_profit, 1),
        "순수익(만원)": round(net_profit, 1),
        "ROE(%)": round(roe_pct, 2),
        "연환산ROE_CAGR(%)": round(cagr, 2),
        "총비용(만원)": round(total_cost, 1),
        "이자비용(만원)": round(total_interest, 1),
        "임대수입(만원)": round(total_rental_income, 1),
        "손익분기매도가(만원)": round(breakeven_price, 0),
        "상세내역": detail,
    }
