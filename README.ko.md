# druck-etf-auto

[![CI](https://github.com/m1nd0322/druck-etf-auto/actions/workflows/ci.yml/badge.svg)](https://github.com/m1nd0322/druck-etf-auto/actions/workflows/ci.yml)

KR/US ETF 유니버스를 대상으로 하는 매크로 레짐 + 모멘텀 기반 자산배분 엔진입니다. 개인 운용 환경에서도 더 안전하게 쓸 수 있도록 강화된 상태를 목표로 합니다.

> English version: see [README.md](README.md)

## 이 프로젝트가 하는 일

`druck-etf-auto`는 아래 흐름으로 동작합니다.
- 시장 데이터 로드
- 시장 레짐 분류 (`RISK_ON`, `NEUTRAL`, `RISK_OFF`)
- ETF 점수 계산 (모멘텀, 추세, 변동성, 낙폭 반영)
- 목표 비중 생성
- 리스크 컷 적용
- 리포트 생성
- 필요 시 주문 계획 생성 또는 실행

이 저장소는 **기본적으로 안전한 설정**을 우선합니다.
즉, 처음부터 실거래를 하도록 설계되지 않았습니다.

## 누구를 위한 README인가

이 README는 **처음 사용하는 사람** 기준으로 썼습니다.
처음에는 아래 순서대로 보시는 것을 권장합니다.

1. 환경 설치
2. dry-run 상태로 리포트 1회 실행
3. 대시보드 열기
4. 백테스트 스냅샷 실행
5. 안전 신호와 로그 확인
6. 그 다음에만 실거래 연동 검토

## 안전 모델

이 시스템은 **시스템 에러**와 **전략 위험 신호**를 분리합니다.

### 시스템 에러
예시:
- 데이터 다운로드 실패
- notifier 실패
- 일시적 런타임 예외
- scheduler job 예외

기대 동작:
- 에러는 보고합니다
- 전체 운영 루프를 불필요하게 죽이지 않습니다

### 전략 위험 신호
예시:
- 매크로 점수가 매우 약함
- 음수 모멘텀 종목이 너무 많음
- 리스크 컷 이후 현금 비중이 과도함
- 성과 열화 신호
- benchmark 대비 상대 약세

기대 동작:
- 시스템 자체는 계속 돌아갈 수 있습니다
- 하지만 **매매는 안전을 위해 중단**될 수 있습니다
- 운영자는 dashboard, runtime event, log를 보고 판단합니다

## 주요 기능

- 검증된 config 로딩 (`druck/config.py`)
- dry-run 우선 워크플로우
- report, order preview, audit, runtime event, halt 상태를 보여주는 대시보드
- trade audit logging
- replan 이벤트용 operator acknowledgement 흐름
- partial fill 이후 bounded replan loop
- 전체 루프를 무너뜨리지 않고 에러를 보고하는 runtime guard
- 위험한 거래 조건을 막는 strategy halt 규칙
- 향후 확장을 위한 backtest scaffold
- 핵심 로직과 운영 흐름을 검증하는 CI 및 테스트

## 저장소 구조

- `druck/engine.py` - 전체 실행 파이프라인
- `druck/trading.py` - 주문 계획 생성, 리뷰, 실행, replan loop
- `druck/runtime.py` - runtime guard 및 runtime event 보고
- `druck/db.py` - fills, trade audit, operator acknowledgement, runtime event 저장
- `druck/web/app.py` - dashboard 및 API
- `druck/backtest.py` - 현재 backtest scaffold
- `run_report.py` - 리포트 1회 실행
- `run_backtest.py` - 백테스트 스냅샷 실행
- `run_auto.py` - scheduler 진입점
- `run_web.py` - 웹 대시보드 실행

## 1. 설치

### 요구사항
- Python 3.11+ 권장
- Linux/macOS: 리포트, 대시보드, 테스트, backtest scaffold 사용 가능
- Windows: Kiwoom 실거래 경로 사용 시 필요

### 설치 절차

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

## 2. 가장 안전한 첫 실행 방법

처음에는 **report only** 로 시작하는 게 가장 안전합니다.

### config 확인
메인 설정 파일:
- `config.yaml`

기본 안전 설정:
- `mode.dry_run: true`
- `mode.enable_kiwoom: false`

즉, 기본 상태에서는 실제 주문이 나가지 않습니다.

### 리포트 1회 실행

```bash
. .venv/bin/activate
python run_report.py
```

생성 결과:
- `output/report_YYYYMMDD_HHMMSS.md`
- `output/selection_YYYYMMDD_HHMMSS.csv`

## 3. 대시보드 열기

### 로컬 Python 실행

```bash
. .venv/bin/activate
python run_web.py
```

### Docker Compose 실행

```bash
docker compose up -d --build web
```

Docker 대시보드는 아래 로컬 마운트를 전제로 합니다.
- `./config.yaml:/app/config.yaml:ro`
- `./config.local.yaml:/app/config.local.yaml:ro`
- `./data:/app/data:ro`
- `./trade_log.db:/app/trade_log.db`
- `./output:/app/output`
- `./.cache:/app/.cache`

이유:
- `config.local.yaml`이 있어야 web 컨테이너가 로컬 전용 Telegram 설정을 읽을 수 있습니다
- `data/market_data/listings/*.parquet`가 있어야 `494310.KS` 같은 티커를 읽기 쉬운 ETF 이름으로 바꿔 표시할 수 있습니다

또한 web 이미지에는 `requirements-docker.txt` 기준으로 아래 의존성이 필요합니다.
- `tabulate`
- `pyarrow`
- `duckdb`

그 다음 브라우저에서 로컬 dashboard를 엽니다.

처음 볼 항목:
- 최신 regime 상태
- risk score
- 선택된 ETF, 가능한 경우 이름 우선 + ticker 보조 표시
- order plan preview
- warnings
- runtime events
- strategy halt 상태
- 최근 order operation, 즉 매수/매도 제출 시도와 예수금/잔고/체결/미체결 조회 같은 관련 작업 이력
- 최근 trade audit event
- 최근 operator acknowledgement

## 4. 백테스트 스냅샷 실행

```bash
. .venv/bin/activate
python run_backtest.py
```

중요한 점:
- 현재는 **scaffold** 입니다
- 완성형 기관급 백테스트 엔진이 아닙니다
- 워크플로우와 출력 구조를 점검하는 용도로 먼저 보셔야 합니다

## 5. 핵심 안전장치 이해하기

### Dry run
- `mode.dry_run: true`
- 처음 쓰는 사람에게 가장 안전한 모드입니다
- 실제 주문 없이 trade intent와 review 정보만 생성합니다

### Live trade review gate
실거래 전 아래를 검사합니다.
- broker 지원 여부
- Kiwoom 활성화 여부
- dry-run 해제 여부
- 계좌 설정 여부
- trade-plan warning 유무

### Partial fill 처리
partial fill이 발생하면:
- 해당 사이클은 replan-required 상태가 됩니다
- 정상 완료로 보지 않습니다
- dashboard에 acknowledgement 필요 상태가 표시됩니다
- 운영자는 note와 함께 acknowledgement를 남길 수 있습니다

### Runtime events
Runtime events는 다음과 같은 운영 이슈를 기록합니다.
- `system_error`
- `strategy_halt`

이 이벤트는 dashboard와 API에서 볼 수 있습니다.
운영자가 note를 남기며 resolve 처리도 할 수 있습니다.

### Strategy halt
전략이 손실 확대 방향으로 보이면 매매가 멈출 수 있습니다.
현재 halt 계열:
- 매크로 위험도 약세
- 음수 모멘텀 종목 과다
- 컷 이후 현금 비중 과도
- risk cut 군집
- 평균 score 열화
- 평균 momentum 열화
- 최근 수익률 proxy 약세
- benchmark 대비 상대 약세

## 6. 초심자용 설정 가이드

중요한 설정 섹션:
- `mode` - dry run / Kiwoom 활성화
- `data` - lookback, provider, cache
- `macro_filter` - 레짐 threshold와 구성요소
- `selection` - ETF 점수화 및 집중도
- `risk_cut` - 방어적 리스크 제어
- `rebalance` - 최소 거래 기준
- `strategy_halt` - 신호 열화 시 거래 중단 규칙
- `schedule` - report / risk-check 시간
- `notifier` - Telegram 알림
- `kiwoom` - broker 실행 설정

### 초심자에게 안전한 예시

```yaml
mode:
  dry_run: true
  enable_kiwoom: false
```

생성되는 리포트와 dashboard 동작을 충분히 이해하기 전까지는 여기 머무는 것을 권장합니다.

## 7. 실거래로 넘어가는 방법

실거래는 단계적으로 가야 합니다.

### 1단계, report only
- `run_report.py` 실행
- 출력 결과 점검

### 2단계, dashboard + backtest scaffold
- dashboard 실행
- backtest snapshot 실행
- runtime event와 halt 상태 확인

### 3단계, Kiwoom dry-run review
다음처럼 설정:

```yaml
mode:
  dry_run: true
  enable_kiwoom: true
kiwoom:
  account_no: "YOUR_ACCOUNT_NO"
```

그 다음 아래를 확인:
- order plan preview
- live review checks
- audit logs
- runtime events

### 4단계, 첫 라이브 롤아웃
이전 단계가 충분히 안정적일 때만:
- dry run 해제
- 작은 노출로 시작
- fill, audit, runtime event, strategy halt 신호를 적극 모니터링

## 8. 실제로 자주 쓸 명령어

### 리포트 1회 실행
```bash
python run_report.py
```

### scheduler 실행
```bash
python run_auto.py
```

### dashboard 실행
```bash
python run_web.py
```

### 테스트 실행
```bash
pytest -q
```

### backtest scaffold 실행
```bash
python run_backtest.py
```

## 9. API와 dashboard 관측 포인트

유용한 API:
- `/api/status`
- `/api/audit`
- `/api/ack`
- `/api/runtime`

운영자 입장에서는 dashboard를 1차 인터페이스로 보는 것을 권장합니다.

## 10. 테스트와 품질

```bash
pytest -q
```

현재 커버 범위:
- config validation
- macro regime logic
- portfolio scoring 및 cut
- notifier / scheduler 동작
- trade review 및 execution safety
- partial fill 및 replan flow
- operator acknowledgement flow
- runtime event 저장 및 resolve
- strategy halt 동작
- dashboard API 동작

## 11. 공유 데이터 연동

이 프로젝트는 [`m1nd0322/data`](https://github.com/m1nd0322/data) 저장소의 공유 시장 데이터를 사용할 수 있습니다.

현재 동작:
- shared parquet 데이터가 있으면 우선 사용
- 없는 티커는 yfinance/FDR fallback
- cache로 반복 다운로드 최소화

## 12. 현재 한계

- 현재 backtest는 아직 최소 scaffold 수준입니다
- strategy halt는 안전 중심 규칙이며, 완전한 전략 리스크 연구 레이어는 아닙니다
- Kiwoom 실거래는 Windows 환경 준비가 필요합니다

## 13. 더 깊게 이해하려면 이 순서 추천

1. `config.yaml`
2. `druck/engine.py`
3. `druck/trading.py`
4. `druck/runtime.py`
5. `druck/web/app.py`
6. 로컬 dashboard 실행 상태

## 면책

이 저장소는 연구 및 엔지니어링 데모 목적입니다.
투자 자문이 아닙니다.
