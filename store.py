"""Tiny persistence layer: JSON file by default, MongoDB when MONGO_URI is set (survives redeploys)."""
import asyncio
import json
import os
import time
from datetime import datetime, timezone


def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


class Store:
    def __init__(self, path: str = "bot_data.json", mongo_uri: str = "", mongo_db: str = "teraboxbot"):
        self.path = path
        self.mongo = None
        if mongo_uri:
            from pymongo import MongoClient  # imported lazily: only needed with Mongo
            cli = MongoClient(mongo_uri, serverSelectionTimeoutMS=8000)
            cli.admin.command("ping")
            self.mongo = cli[mongo_db]["state"]
        self.data = {"users": {}, "banned": [], "total_downloads": 0, "total_bytes": 0}
        self._dirty = False
        self._lock = asyncio.Lock()
        self._load()

    # ---- persistence -------------------------------------------------------
    def _load(self):
        try:
            if self.mongo is not None:
                doc = self.mongo.find_one({"_id": "main"})
                if doc:
                    doc.pop("_id")
                    self.data.update(doc)
            elif os.path.exists(self.path):
                with open(self.path, encoding="utf-8") as f:
                    self.data.update(json.load(f))
        except Exception as e:  # corrupt file must not stop the bot
            print(f"[store] load failed: {e}")

    def _write(self):
        if self.mongo is not None:
            self.mongo.replace_one({"_id": "main"}, {"_id": "main", **self.data}, upsert=True)
        else:
            tmp = self.path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self.data, f)
            os.replace(tmp, self.path)

    async def flush(self):
        async with self._lock:
            if not self._dirty:
                return
            self._dirty = False
            try:
                await asyncio.to_thread(self._write)
            except Exception as e:
                self._dirty = True
                print(f"[store] save failed: {e}")

    async def autosave_loop(self, every: int = 20):
        while True:
            await asyncio.sleep(every)
            await self.flush()

    # ---- users -------------------------------------------------------------
    def add_user(self, uid: int, name: str) -> bool:
        """Returns True if the user is new."""
        k = str(uid)
        new = k not in self.data["users"]
        if new:
            self.data["users"][k] = {"name": name, "joined": int(time.time()), "dl": 0, "day": _today(), "today": 0}
            self._dirty = True
        return new

    def user_ids(self):
        return [int(k) for k in self.data["users"]]

    def is_banned(self, uid: int) -> bool:
        return uid in self.data["banned"]

    def ban(self, uid: int, on: bool = True):
        b = self.data["banned"]
        if on and uid not in b:
            b.append(uid)
        elif not on and uid in b:
            b.remove(uid)
        self._dirty = True

    # ---- daily limit / stats ----------------------------------------------
    def downloads_today(self, uid: int) -> int:
        u = self.data["users"].get(str(uid))
        if not u:
            return 0
        if u.get("day") != _today():
            u["day"], u["today"] = _today(), 0
        return u["today"]

    def record_download(self, uid: int, size: int):
        self.downloads_today(uid)  # rolls the day over
        u = self.data["users"].setdefault(str(uid), {"name": "", "joined": int(time.time()), "dl": 0, "day": _today(), "today": 0})
        u["dl"] += 1
        u["today"] += 1
        self.data["total_downloads"] += 1
        self.data["total_bytes"] += size
        self._dirty = True

    # ---- premium -------------------------------------------------------------
    # users[uid]["premium_until"]: 0/missing = free, -1 = lifetime, else unix expiry time
    def premium_info(self, uid: int) -> dict:
        u = self.data["users"].get(str(uid)) or {}
        until = int(u.get("premium_until") or 0)
        if until == -1:
            return {"is_premium": True, "lifetime": True, "expires_at": None, "days_left": None}
        if until > time.time():
            return {"is_premium": True, "lifetime": False, "expires_at": until,
                    "days_left": int((until - time.time()) // 86400) + 1}
        return {"is_premium": False, "lifetime": False, "expires_at": None, "days_left": 0}

    def is_premium(self, uid: int) -> bool:
        return self.premium_info(uid)["is_premium"]

    def add_premium(self, uid: int, days: int = 0, name: str = "") -> dict:
        """days=0 -> lifetime. Adding days to an active plan extends it. Returns the new premium_info."""
        u = self.data["users"].setdefault(str(uid), {"name": name, "joined": int(time.time()), "dl": 0, "day": _today(), "today": 0})
        cur = int(u.get("premium_until") or 0)
        if days <= 0 or cur == -1:
            u["premium_until"] = -1
        else:
            u["premium_until"] = max(cur, int(time.time())) + days * 86400
        self._dirty = True
        return self.premium_info(uid)

    def remove_premium(self, uid: int) -> bool:
        u = self.data["users"].get(str(uid))
        had = bool(u and u.get("premium_until"))
        if u is not None:
            u["premium_until"] = 0
            self._dirty = True
        return had

    def premium_ids(self):
        return [int(k) for k in self.data["users"] if self.premium_info(int(k))["is_premium"]]

    def remove_user(self, uid: int):
        """Drop a user who blocked the bot / deleted their account (used by /broadcast)."""
        if self.data["users"].pop(str(uid), None) is not None:
            self._dirty = True

    def banned_count(self) -> int:
        return len(self.data["banned"])
