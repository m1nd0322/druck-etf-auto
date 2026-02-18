from __future__ import annotations
import sys, time, math
from dataclasses import dataclass
from datetime import datetime, time as dtime
from typing import Dict, Optional, Any, Tuple, List

from .broker_base import Broker
from .utils_rate import RateLimiter
from .db import init_db, log_fill

def _now_kst() -> datetime:
    return datetime.now()

def _is_market_open_kst(now: Optional[datetime]=None) -> bool:
    now = now or _now_kst()
    t = now.time()
    return dtime(9,0) <= t <= dtime(15,30)

def _is_near_close_kst(now: Optional[datetime]=None) -> bool:
    now = now or _now_kst()
    return now.time() >= dtime(15,20)

def _normalize_code(ticker: str) -> str:
    if not ticker:
        return ""
    t = ticker.strip()
    if t.startswith("^"):
        return ""
    for suf in [".KS",".KQ",".KR",".KO"]:
        if t.endswith(suf):
            t = t[:-len(suf)]
    if t.startswith("A") and len(t)==7 and t[1:].isdigit():
        t = t[1:]
    return t

@dataclass
class KiwoomTRResponse:
    rqname: str
    trcode: str
    recordname: str
    prev_next: str

class KiwoomBroker(Broker):
    def __init__(
        self,
        account_no: str,
        dry_run: bool=True,
        split_n: int=3,
        tr_max_per_sec: int=5,
        slippage_limit_bps: float=30.0,
        require_market_open: bool=True,
        block_near_close: bool=True,
        db_path: str="trade_log.db",
    ):
        self.account_no = account_no.strip()
        self.dry_run = dry_run
        self.split_n = max(1, int(split_n))
        self.require_market_open = require_market_open
        self.block_near_close = block_near_close
        self.slippage_limit_bps = float(slippage_limit_bps)

        self.ocx=None
        self.app=None
        self.QEventLoop=None

        self._rate = RateLimiter(max_per_sec=tr_max_per_sec)
        self._connected=False

        self._tr_event_loop=None
        self._last_tr: Optional[KiwoomTRResponse]=None

        # chejan tracking
        self._chejan_loop=None
        self._order_done=False
        self._filled_qty=0
        self._avg_fill_price=0.0
        self._last_side=""

        # DB for fills
        self._db = init_db(db_path)

    def connect(self) -> None:
        self._init_ocx()
        self._login_blocking()
        if not self._connected:
            raise RuntimeError("Kiwoom login failed")

    # -------- TR helpers --------
    def _init_ocx(self):
        from PyQt5.QtWidgets import QApplication
        from PyQt5.QAxContainer import QAxWidget
        from PyQt5.QtCore import QEventLoop
        self.QEventLoop = QEventLoop
        self.app = QApplication.instance() or QApplication(sys.argv)
        self.ocx = QAxWidget("KHOPENAPI.KHOpenAPICtrl.1")
        self.ocx.OnEventConnect.connect(self._on_event_connect)
        self.ocx.OnReceiveTrData.connect(self._on_receive_tr_data)
        self.ocx.OnReceiveChejanData.connect(self._on_receive_chejan_data)
        self.ocx.OnReceiveMsg.connect(lambda *args: None)

    def _login_blocking(self):
        self._connected=False
        self.ocx.dynamicCall("CommConnect()")
        loop = self.QEventLoop()
        self._login_loop = loop
        loop.exec_()

    def _on_event_connect(self, err_code: int):
        self._connected = (int(err_code)==0)
        if getattr(self, "_login_loop", None) is not None:
            self._login_loop.exit()

    def _set_input_value(self, key: str, value: str):
        self.ocx.dynamicCall("SetInputValue(QString, QString)", key, value)

    def _comm_rq_data(self, rqname: str, trcode: str, prev_next: str, screen_no: str="1000"):
        self._rate.wait()
        self.ocx.dynamicCall("CommRqData(QString, QString, int, QString)", rqname, trcode, int(prev_next), screen_no)

    def _wait_tr(self) -> KiwoomTRResponse:
        self._tr_event_loop = self.QEventLoop()
        self._tr_event_loop.exec_()
        self._tr_event_loop = None
        if self._last_tr is None:
            raise RuntimeError("Missing TR response")
        return self._last_tr

    def _on_receive_tr_data(self, screen_no, rqname, trcode, recordname, prev_next, *args):
        self._last_tr = KiwoomTRResponse(rqname=rqname, trcode=trcode, recordname=recordname, prev_next=str(prev_next))
        if self._tr_event_loop is not None:
            self._tr_event_loop.exit()

    def _get_comm_data(self, trcode: str, rqname: str, index: int, item: str) -> str:
        return self.ocx.dynamicCall("GetCommData(QString, QString, int, QString)", trcode, rqname, index, item)

    def _get_repeat_cnt(self, trcode: str, recordname: str) -> int:
        return int(self.ocx.dynamicCall("GetRepeatCnt(QString, QString)", trcode, recordname))

    # -------- Chejan --------
    def _on_receive_chejan_data(self, gubun, item_cnt, fid_list):
        if str(gubun) != "0":
            return
        code = self.ocx.dynamicCall("GetChejanData(int)", 9001).strip().replace("A","")
        status = self.ocx.dynamicCall("GetChejanData(int)", 913).strip()   # 주문상태
        filled = self.ocx.dynamicCall("GetChejanData(int)", 911).strip()   # 체결량
        price  = self.ocx.dynamicCall("GetChejanData(int)", 910).strip()   # 체결가
        side_raw = self.ocx.dynamicCall("GetChejanData(int)", 907).strip() # 매도수구분(1매도/2매수) - 환경에 따라 다를 수 있음

        try:
            filled_i = int(filled)
            price_i = abs(int(price))
        except Exception:
            return

        if filled_i > 0:
            prev_qty = self._filled_qty
            self._filled_qty += filled_i
            total_value = self._avg_fill_price * prev_qty + price_i * filled_i
            self._avg_fill_price = total_value / max(1, self._filled_qty)

            side = self._last_side
            log_fill(self._db, code, filled_i, float(price_i), side)

        if status == "체결":
            self._order_done = True
            if self._chejan_loop is not None:
                self._chejan_loop.exit()

    def _wait_for_fill(self, timeout_sec: int=20):
        from PyQt5.QtCore import QTimer
        self._order_done = False
        self._filled_qty = 0
        self._avg_fill_price = 0.0

        loop = self.QEventLoop()
        self._chejan_loop = loop

        timer = QTimer()
        timer.setSingleShot(True)
        timer.timeout.connect(loop.exit)
        timer.start(int(timeout_sec*1000))

        loop.exec_()
        self._chejan_loop = None

    def _check_slippage(self, ref_price: float) -> bool:
        if self._avg_fill_price <= 0 or ref_price <= 0:
            return True
        bps = ((self._avg_fill_price - ref_price) / ref_price) * 10000.0
        if abs(bps) > self.slippage_limit_bps:
            return False
        return True

    # -------- Public API --------
    def get_cash(self) -> float:
        if self.dry_run and not self._connected:
            return 0.0
        self._set_input_value("계좌번호", self.account_no)
        self._set_input_value("비밀번호", "")
        self._set_input_value("비밀번호입력매체구분", "00")
        self._set_input_value("조회구분", "2")
        self._comm_rq_data("opw00001_req", "opw00001", "0")
        resp = self._wait_tr()
        depo = self._get_comm_data(resp.trcode, resp.rqname, 0, "예수금").strip()
        avail = self._get_comm_data(resp.trcode, resp.rqname, 0, "주문가능금액").strip()
        val = 0
        for s in [avail, depo]:
            try:
                v = int(s)
                if v > 0:
                    val = v; break
            except Exception:
                continue
        return float(val)

    def get_positions(self) -> Dict[str,int]:
        if self.dry_run and not self._connected:
            return {}
        positions={}
        prev_next="0"
        while True:
            self._set_input_value("계좌번호", self.account_no)
            self._set_input_value("비밀번호", "")
            self._set_input_value("비밀번호입력매체구분", "00")
            self._set_input_value("조회구분", "2")
            self._comm_rq_data("opw00018_req", "opw00018", prev_next)
            resp = self._wait_tr()
            cnt = self._get_repeat_cnt(resp.trcode, resp.recordname)
            for i in range(cnt):
                code = self._get_comm_data(resp.trcode, resp.rqname, i, "종목번호").strip().replace("A","").strip()
                qty  = self._get_comm_data(resp.trcode, resp.rqname, i, "보유수량").strip()
                code = _normalize_code(code)
                try: q=int(qty)
                except Exception: q=0
                if code and q:
                    positions[code]=positions.get(code,0)+q
            prev_next = resp.prev_next
            if str(prev_next).strip() != "2":
                break
        return positions

    def get_last_price(self, ticker: str) -> float:
        code=_normalize_code(ticker)
        if not code:
            return 0.0
        self._set_input_value("종목코드", code)
        self._comm_rq_data(f"opt10001_{code}", "opt10001", "0")
        resp=self._wait_tr()
        cur=self._get_comm_data(resp.trcode, resp.rqname, 0, "현재가").strip()
        try:
            return float(abs(int(cur)))
        except Exception:
            return 0.0

    # -------- unfilled / cancel / reorder --------
    def get_unfilled_orders(self) -> List[Tuple[str,str,int,str]]:
        self._set_input_value("계좌번호", self.account_no)
        self._set_input_value("체결구분", "1")  # 미체결
        self._set_input_value("매매구분", "0")
        self._comm_rq_data("opt10075_req", "opt10075", "0")
        resp=self._wait_tr()
        cnt=self._get_repeat_cnt(resp.trcode, resp.recordname)
        unfilled=[]
        for i in range(cnt):
            order_no=self._get_comm_data(resp.trcode, resp.rqname, i, "주문번호").strip()
            code=self._get_comm_data(resp.trcode, resp.rqname, i, "종목코드").strip().replace("A","")
            remain=self._get_comm_data(resp.trcode, resp.rqname, i, "미체결수량").strip()
            bs=self._get_comm_data(resp.trcode, resp.rqname, i, "매매구분").strip()  # 환경에 따라 다를 수 있음
            try: r=int(remain)
            except Exception: r=0
            if order_no and code and r>0:
                unfilled.append((order_no, code, r, bs))
        return unfilled

    def cancel_order(self, order_no: str, code: str):
        self._rate.wait()
        # order type 3 = cancel
        ret=self.ocx.dynamicCall(
            "SendOrder(QString, QString, QString, int, QString, int, int, QString, QString)",
            "cancel",
            "1000",
            self.account_no,
            3,
            code,
            0,
            0,
            "00",
            order_no,
        )
        if int(ret)!=0:
            raise RuntimeError(f"Cancel failed ret={ret} order_no={order_no}")

    def cancel_and_reorder(self, order_no: str, code: str, remain: int, side: str):
        if self.dry_run:
            print(f"[DRY_RUN][CANCEL+REORDER] {order_no} {code} remain={remain} side={side}")
            return
        self.cancel_order(order_no, code)
        time.sleep(1.0)
        self.place_order(code, remain, side, "MKT")

    # -------- ordering --------
    def place_order(self, ticker: str, qty: int, side: str, order_type: str="MKT") -> None:
        code=_normalize_code(ticker)
        qty=int(qty)
        side=side.upper().strip()
        if not code or qty<=0:
            return
        if self.require_market_open and not _is_market_open_kst():
            print("[KIWOOM] market closed; skip")
            return
        if self.block_near_close and _is_near_close_kst():
            print("[KIWOOM] near close; block new order")
            return
        if order_type!="MKT":
            raise ValueError("Only MKT supported in this repo")

        ref_price = self.get_last_price(code)
        parts = self._split_qty(qty, self.split_n)

        for idx, q in enumerate(parts, start=1):
            if q<=0: 
                continue
            self._last_side = side
            if self.dry_run:
                print(f"[DRY_RUN][ORDER] {side} {code} x {q} (part {idx}/{len(parts)})")
                continue

            self._send_order_market(code, q, side)
            self._wait_for_fill(timeout_sec=25)

            if self._filled_qty == 0:
                # 미체결/미체결잔량 처리: opt10075에서 해당 종목 미체결 조회 후 cancel+reorder
                unfilled = [u for u in self.get_unfilled_orders() if u[1].replace('A','')==code]
                for order_no, c, remain, bs in unfilled:
                    # remain 전량 시장가 재주문
                    self.cancel_and_reorder(order_no, code, remain, side)
                break

            if not self._check_slippage(ref_price):
                print("[KIWOOM] slippage too large; abort remaining slices")
                break

            time.sleep(0.3)

    def _send_order_market(self, code: str, qty: int, side: str):
        self._rate.wait()
        order_type = 1 if side=="BUY" else 2
        rqname = f"order_{side}_{code}_{int(time.time())}"
        ret=self.ocx.dynamicCall(
            "SendOrder(QString, QString, QString, int, QString, int, int, QString, QString)",
            rqname,
            "1000",
            self.account_no,
            order_type,
            code,
            int(qty),
            0,
            "03",  # 시장가
            "",
        )
        if int(ret)!=0:
            raise RuntimeError(f"SendOrder failed ret={ret} {side} {code} x {qty}")

    @staticmethod
    def _split_qty(qty: int, n: int) -> Tuple[int,...]:
        base = qty//n
        rem = qty%n
        parts=[]
        for i in range(n):
            parts.append(base + (1 if i<rem else 0))
        return tuple(parts)

    # -------- portfolio value --------
    def get_portfolio_value(self) -> float:
        cash=self.get_cash()
        pos=self.get_positions()
        total=cash
        for code,qty in pos.items():
            px=self.get_last_price(code)
            total += float(qty)*float(px)
        return float(total)
