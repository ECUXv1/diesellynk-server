"""
DieselLynk Nexiq Agent
======================
Runs on the Windows tablet alongside Nexiq Mini Blue 2.
Reads adapter status, fault codes, live J1939 data.
Posts everything to the DieselLynk server every few seconds.

Requirements:
    pip install requests pywin32

Usage:
    python nexiq_agent.py --session DL-XXXX --token DRIVER_TOKEN --server http://localhost:8080
"""

import ctypes
import ctypes.util
import struct
import time
import json
import requests
import argparse
import sys
import os
from typing import Optional, List, Dict

# ── RP1210 CONSTANTS ──────────────────────────────────────────────────────────

RP1210_ERR_SUCCESS = 0

# J1939 PGNs we care about
PGN_ENGINE_SPEED = 0xF004        # SPN 190 — RPM
PGN_VEHICLE_SPEED = 0xFEF1       # SPN 84 — km/h
PGN_COOLANT_TEMP = 0xFEEE        # SPN 110 — °C
PGN_OIL_PRESSURE = 0xF003        # SPN 100 — kPa
PGN_BOOST_PRESSURE = 0xFEF6      # SPN 102 — kPa
PGN_BATTERY_VOLTAGE = 0xFEF3     # SPN 158 — V

# DM1 — Active Diagnostic Trouble Codes
PGN_DM1_ACTIVE_FAULTS = 0xFECA

# J1939 source addresses (common)
SA_ENGINE = 0x00
SA_TRANSMISSION = 0x03
SA_BRAKE = 0x0B
SA_INSTRUMENT = 0x17
SA_BODY_CONTROLLER = 0x21

SA_NAMES = {
    0x00: "Engine",
    0x03: "Transmission",
    0x0B: "Brakes",
    0x17: "Instruments",
    0x21: "Body Controller",
    0x27: "Cab Controller",
    0x28: "Auxiliary Heater",
    0x3D: "Turbocharger",
    0xFE: "Unknown",
}

# FMI descriptions
FMI_DESCRIPTIONS = {
    0:  "Above normal operating range",
    1:  "Below normal operating range",
    2:  "Data erratic, intermittent or incorrect",
    3:  "Voltage above normal or shorted high",
    4:  "Voltage below normal or shorted low",
    5:  "Current below normal or open circuit",
    6:  "Current above normal or grounded circuit",
    7:  "Mechanical system not responding properly",
    8:  "Abnormal frequency, pulse width or period",
    9:  "Abnormal update rate",
    10: "Abnormal rate of change",
    11: "Root cause not known",
    12: "Bad intelligent device or component",
    13: "Out of calibration",
    14: "Special instructions",
    15: "Data valid but above normal operational range",
    16: "Data valid but below normal operational range",
    17: "Data valid but below normal operational range (moderately severe)",
    18: "Data valid but above normal operational range (moderately severe)",
    19: "Received network data in error",
    31: "Condition exists",
}

# ── RP1210 WRAPPER ────────────────────────────────────────────────────────────

class RP1210:
    """
    Minimal RP1210 API wrapper.
    Auto-detects connected Nexiq adapter by trying known DLLs in order:
      1. NULN2032  — Nexiq USB Link 2
      2. NULN2R32  — Nexiq Mini Blue 2
      3. NEXIQ32   — Nexiq legacy adapters
    Falls back to demo mode if none found.
    """

    # All known Nexiq DLL names in priority order
    KNOWN_DLLS = [
        ("NULN2032",  "Nexiq USB Link 2"),
        ("NULN2R32",  "Nexiq Mini Blue 2"),
        ("NEXIQ32",   "Nexiq Legacy"),
        ("DGDPA5MA",  "DG Tech DPA 5 Multi-Application"),
    ]

    def __init__(self, dll_name: str = None):
        """
        dll_name: force a specific DLL (optional)
        If None, auto-detects by trying all known DLLs.
        """
        self.dll = None
        self.client_id = -1
        self.dll_name = dll_name
        self.adapter_name = "Unknown"

        if dll_name:
            # Force specific DLL
            self._load_dll(dll_name)
        else:
            # Auto-detect
            self._auto_detect()

    def _load_dll(self, dll_name: str) -> bool:
        try:
            self.dll = ctypes.windll.LoadLibrary(dll_name)
            self.dll_name = dll_name
            print(f"[RP1210] Loaded {dll_name}.dll")
            return True
        except Exception as e:
            print(f"[RP1210] Could not load {dll_name}: {e}")
            self.dll = None
            return False

    def _auto_detect(self):
        """Try each known DLL until one loads successfully"""
        print("[RP1210] Auto-detecting Nexiq adapter...")
        for dll_name, friendly_name in self.KNOWN_DLLS:
            if self._load_dll(dll_name):
                self.adapter_name = friendly_name
                print(f"[RP1210] Detected: {friendly_name} ({dll_name})")
                return
        print("[RP1210] No Nexiq adapter found — will run in demo mode")
        self.adapter_name = "Demo Mode"

    @property
    def available(self) -> bool:
        return self.dll is not None

    def connect(self, device_id: int = 1, protocol: str = "J1939") -> bool:
        if not self.dll:
            return False
        try:
            # RP1210_ClientConnect(HWND, short nDeviceID, char* fpchProtocol,
            #                      long lSendBuffer, long lRcvBuffer, short nIsAppPacketizingIncomingMsgs)
            fpch = ctypes.create_string_buffer(protocol.encode())
            self.client_id = self.dll.RP1210_ClientConnect(
                0, device_id, fpch, 0, 0, 0
            )
            if self.client_id >= 0:
                print(f"[RP1210] Connected — client_id={self.client_id}")
                # Enable all PGNs
                self._set_all_filters_pass()
                return True
            else:
                print(f"[RP1210] Connect failed, code={self.client_id}")
                return False
        except Exception as e:
            print(f"[RP1210] Connect exception: {e}")
            return False

    def disconnect(self):
        if self.dll and self.client_id >= 0:
            self.dll.RP1210_ClientDisconnect(self.client_id)
            self.client_id = -1

    def _set_all_filters_pass(self):
        """Allow all J1939 messages through"""
        if not self.dll or self.client_id < 0:
            return
        buf = ctypes.create_string_buffer(4)
        struct.pack_into("<I", buf, 0, 0)
        self.dll.RP1210_SendCommand(
            3,  # RP1210_SET_ALL_FILTERS_STATES_TO_PASS
            self.client_id,
            buf, 4
        )

    def read_message(self, timeout_ms: int = 100) -> Optional[bytes]:
        """Read one message from the adapter buffer"""
        if not self.dll or self.client_id < 0:
            return None
        buf = ctypes.create_string_buffer(2048)
        result = self.dll.RP1210_ReadMessage(
            self.client_id, buf, ctypes.sizeof(buf), timeout_ms
        )
        if result > 0:
            return bytes(buf.raw[:result])
        return None


# ── J1939 PARSER ─────────────────────────────────────────────────────────────

def parse_j1939_header(data: bytes) -> Dict:
    """Parse RP1210 J1939 message header"""
    if len(data) < 6:
        return {}
    # RP1210 J1939 format:
    # [0] = echo flag
    # [1-4] = timestamp (ms)
    # [5] = destination addr
    # [6] = source addr
    # [7] = priority+pgn (3 bytes)
    # [10+] = data
    try:
        src_addr = data[6] if len(data) > 6 else 0
        pgn = struct.unpack_from(">I", b'\x00' + data[7:10])[0] if len(data) > 10 else 0
        payload = data[10:] if len(data) > 10 else b""
        return {"src": src_addr, "pgn": pgn, "data": payload}
    except Exception:
        return {}


def parse_dm1(data: bytes) -> List[Dict]:
    """
    Parse DM1 Active Fault Codes (PGN 0xFECA)
    Each fault code is 4 bytes: SPN(19bits) + FMI(5bits) + CM(1) + OC(7bits)
    """
    faults = []
    if len(data) < 2:
        return faults

    # Byte 0: lamp status, byte 1: lamp status
    # Bytes 2+: fault code entries (4 bytes each)
    i = 2
    while i + 3 < len(data):
        b0, b1, b2, b3 = data[i], data[i+1], data[i+2], data[i+3]

        # SPN: bits 7-0 of byte0, bits 7-0 of byte1, bits 7-5 of byte2
        spn = b0 | (b1 << 8) | ((b2 & 0xE0) >> 5 << 16)
        fmi = b2 & 0x1F
        oc  = b3 & 0x7F  # occurrence count

        if spn != 0:
            faults.append({
                "spn": spn,
                "fmi": fmi,
                "count": oc,
                "description": FMI_DESCRIPTIONS.get(fmi, "Unknown"),
                "active": True,
            })

        i += 4

    return faults


def decode_engine_speed(data: bytes) -> Optional[float]:
    """SPN 190 — Engine speed in RPM (resolution 0.125 RPM/bit)"""
    if len(data) >= 4:
        raw = struct.unpack_from("<H", data, 2)[0]
        if raw != 0xFFFF:
            return raw * 0.125
    return None


def decode_vehicle_speed(data: bytes) -> Optional[float]:
    """SPN 84 — Wheel-based vehicle speed km/h (resolution 0.00390625 km/h/bit)"""
    if len(data) >= 2:
        raw = struct.unpack_from("<H", data, 0)[0]
        if raw != 0xFFFF:
            return raw * 0.00390625
    return None


def decode_coolant_temp(data: bytes) -> Optional[float]:
    """SPN 110 — Engine coolant temperature °C (offset -40)"""
    if len(data) >= 1 and data[0] != 0xFF:
        return data[0] - 40.0
    return None


def decode_oil_pressure(data: bytes) -> Optional[float]:
    """SPN 100 — Engine oil pressure kPa (resolution 4 kPa/bit)"""
    if len(data) >= 4 and data[3] != 0xFF:
        return data[3] * 4.0
    return None


def decode_boost_pressure(data: bytes) -> Optional[float]:
    """SPN 102 — Boost pressure kPa (resolution 2 kPa/bit)"""
    if len(data) >= 2 and data[1] != 0xFF:
        return data[1] * 2.0
    return None


def decode_battery_voltage(data: bytes) -> Optional[float]:
    """SPN 158 — Battery voltage (resolution 0.05 V/bit)"""
    if len(data) >= 2:
        raw = struct.unpack_from("<H", data, 0)[0]
        if raw != 0xFFFF:
            return raw * 0.05
    return None


# ── TRUCK INFO READER ─────────────────────────────────────────────────────────

def request_vin_from_adapter() -> str:
    """
    In real implementation this sends a J1939 PGN request for VIN (PGN 0xFEEC)
    and reads the response. For now returns placeholder.
    TODO: implement full PGN request/response cycle
    """
    return "PENDING"


# ── DIESELLYNK AGENT ──────────────────────────────────────────────────────────

class DieselLynkAgent:
    def __init__(self, session_id: str, driver_token: str, server_url: str, dll_name: str = None):
        self.session_id   = session_id
        self.driver_token = driver_token
        self.server_url   = server_url.rstrip("/")
        self.rp1210       = RP1210(dll_name=dll_name)  # None = auto-detect
        self.connected    = False
        self.fault_codes  = []
        self.live_data    = {}
        self.truck_info   = None
        self.last_post_time  = 0
        self.post_interval   = 2.0

    def connect_adapter(self) -> bool:
        """Try to connect to whichever Nexiq adapter was detected"""
        if not self.rp1210.available:
            print("[Agent] No RP1210 adapter available — running in demo mode")
            return False

        success = self.rp1210.connect(device_id=1, protocol="J1939")
        if success:
            self.connected = True
            print(f"[Agent] {self.rp1210.adapter_name} connected successfully")
        else:
            print(f"[Agent] {self.rp1210.adapter_name} found but connect failed — check cable/truck power")
        return success

    def read_messages(self, duration_ms: int = 500):
        """Read all available messages for duration_ms milliseconds"""
        if not self.connected:
            return

        start = time.time()
        new_faults = []
        live = {}

        while (time.time() - start) < (duration_ms / 1000.0):
            raw = self.rp1210.read_message(timeout_ms=50)
            if not raw:
                continue

            msg = parse_j1939_header(raw)
            if not msg:
                continue

            pgn = msg.get("pgn", 0)
            data = msg.get("data", b"")
            src = msg.get("src", 0)

            # Route by PGN
            if pgn == PGN_DM1_ACTIVE_FAULTS:
                faults = parse_dm1(data)
                source_name = SA_NAMES.get(src, f"0x{src:02X}")
                for f in faults:
                    f["source"] = source_name
                    new_faults.extend(faults)

            elif pgn == PGN_ENGINE_SPEED:
                rpm = decode_engine_speed(data)
                if rpm is not None:
                    live["engine_rpm"] = round(rpm, 1)

            elif pgn == PGN_VEHICLE_SPEED:
                spd = decode_vehicle_speed(data)
                if spd is not None:
                    live["vehicle_speed"] = round(spd, 1)

            elif pgn == PGN_COOLANT_TEMP:
                temp = decode_coolant_temp(data)
                if temp is not None:
                    live["coolant_temp"] = round(temp, 1)

            elif pgn == PGN_OIL_PRESSURE:
                psi = decode_oil_pressure(data)
                if psi is not None:
                    live["oil_pressure"] = round(psi, 1)

            elif pgn == PGN_BOOST_PRESSURE:
                boost = decode_boost_pressure(data)
                if boost is not None:
                    live["boost_pressure"] = round(boost, 1)

        # Update stored state
        if new_faults:
            self.fault_codes = new_faults
        if live:
            self.live_data = live

    def post_status(self):
        """POST current status to DieselLynk server"""
        now = time.time()
        if now - self.last_post_time < self.post_interval:
            return
        self.last_post_time = now

        payload = {
            "driver_token": self.driver_token,
            "connected": self.connected,
            "adapter_name": self.rp1210.adapter_name if self.connected else "",
            "fault_codes": self.fault_codes,
            "live_data": self.live_data if self.live_data else None,
            "truck_info": self.truck_info,
        }

        try:
            resp = requests.post(
                f"{self.server_url}/api/nexiq-status/{self.session_id}",
                json=payload,
                timeout=3
            )
            if resp.status_code == 200:
                pass  # silent success
            else:
                print(f"[Agent] Server returned {resp.status_code}")
        except requests.exceptions.ConnectionError:
            print("[Agent] Cannot reach server — will retry")
        except Exception as e:
            print(f"[Agent] Post error: {e}")

    def run_demo_mode(self):
        """
        Demo mode when no Nexiq adapter available.
        Simulates fault codes and live data for development.
        """
        import random
        import math

        print("[Agent] Running in DEMO MODE — simulating Nexiq data")
        t = 0

        while True:
            t += 1

            # Simulate live engine data
            self.live_data = {
                "engine_rpm": round(700 + 300 * abs(math.sin(t * 0.1)), 1),
                "vehicle_speed": 0.0,
                "coolant_temp": round(85 + 10 * math.sin(t * 0.05), 1),
                "oil_pressure": round(400 + 50 * math.sin(t * 0.08), 1),
                "boost_pressure": round(100 + 20 * math.sin(t * 0.12), 1),
                "battery_voltage": round(13.8 + 0.5 * math.sin(t * 0.03), 2),
            }

            # Simulate fault codes appearing
            if t == 10:
                self.fault_codes = [
                    {
                        "spn": 110,
                        "fmi": 0,
                        "count": 3,
                        "source": "Engine",
                        "description": "Above normal operating range",
                        "active": True,
                    },
                    {
                        "spn": 100,
                        "fmi": 1,
                        "count": 1,
                        "source": "Engine",
                        "description": "Below normal operating range",
                        "active": True,
                    },
                ]
                self.truck_info = {
                    "vin": "1FUJGBDV3CLBP8765",
                    "make": "Volvo",
                    "model": "VNL",
                    "year": 2023,
                    "engine": "D13 HW225",
                    "odometer": 187432,
                }

            self.connected = True
            self.post_status()
            time.sleep(2)

    def run(self):
        """Main agent loop"""
        print(f"[Agent] Starting for session {self.session_id}")
        print(f"[Agent] Server: {self.server_url}")

        # Try to connect adapter
        adapter_ok = self.connect_adapter()

        if not adapter_ok:
            # Run demo mode for development
            self.run_demo_mode()
            return

        print("[Agent] Entering main loop — reading J1939 data")

        try:
            while True:
                self.read_messages(duration_ms=1500)
                self.post_status()
                time.sleep(0.5)
        except KeyboardInterrupt:
            print("[Agent] Stopping")
        finally:
            self.rp1210.disconnect()
            # Post disconnected status
            self.connected = False
            self.fault_codes = []
            self.live_data = {}
            self.post_status()


# ── ENTRY POINT ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DieselLynk Nexiq Agent")
    parser.add_argument("--session", required=True, help="Session ID (e.g. DL-XXXX)")
    parser.add_argument("--token",   required=True, help="Driver token from session creation")
    parser.add_argument("--server",  default="http://localhost:8080", help="DieselLynk server URL")
    parser.add_argument("--dll",     default=None, help="Force specific RP1210 DLL (e.g. NULN2032). Default: auto-detect")
    args = parser.parse_args()

    agent = DieselLynkAgent(
        session_id   = args.session,
        driver_token = args.token,
        server_url   = args.server,
        dll_name     = args.dll,
    )
    agent.run()
