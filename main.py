from typing import Union
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import re
import redis
from urllib.parse import urlparse
import os

"""
To start redis server locally:
/opt/homebrew/opt/redis/bin/redis-server /opt/homebrew/etc/redis.conf
or just and Ctrl-C to stop it
redis-server
"""

# constants
CLOSED_CELL = "closed"
OPEN_CELL = "open"

class PinDTO(BaseModel):
    pin: str

class LockerDTO(BaseModel):
    lockerId: int
    cells: list

class CellDTO(BaseModel):
    lockerId: int
    cellId: str
    status: str
    pin: Union[str, None] = None

def initRedis():
    redisUrl = os.environ.get("REDIS_TLS_URL")
    if redisUrl:
        url = urlparse(redisUrl)
        return redis.Redis(host=url.hostname, port=url.port, password=url.password, ssl=(url.scheme == "rediss"), ssl_cert_reqs=None)
    else:
        return redis.Redis()    

r = initRedis()
r.hmset("locker:1234", {'lockerId': 1234, 'cells': "C-001,C-002"})
#r.hmset("locker:1235", {'lockerId': 1235, 'cells': "C-003,C-004,C-005"})

app = FastAPI()

class ParcelLocker():
    _lockerId: int
    _cells: set()
    _pinToCellId: {}
    
    def initLocker(self, lockerId: int):
        print("Checking locker:", lockerId)
        hkey = f"locker:{lockerId}"
        rLockerId = r.hget(hkey, 'lockerId')
        print("Locker ID:", rLockerId)
        print("Locker ID (decoded):", rLockerId.decode())
        if (not rLockerId) or (rLockerId.decode() != str(lockerId)):
            raise HTTPException(status_code=404, detail=f"Locker by ID: {lockerId} not found")
        self._lockerId = lockerId
        self._cells = set(r.hget(hkey, 'cells').decode().split(','))

    def initCell(self, cellId: str):
        print("Checking cell: ", cellId)
        hkey = f"cell:{self._lockerId}_{cellId}"
        if cellId not in self._cells:
            raise HTTPException(status_code=404, detail=f"Cell by ID: {cellId} not found (locker ID: {self._lockerId})")
        if not r.hget(hkey, "status"):
            # init new cell
            r.hset(hkey, "status", CLOSED_CELL)
            r.hset(hkey, "pin", "------")
            #r.hmset(hkey, {"status", CLOSED_CELL, "pin", "------"})
        cellStatus = r.hget(hkey, "status").decode()
        cellPin = r.hget(hkey, "pin").decode()
        return (cellStatus, cellPin)

    def setCellStatus(self, lockerId, cellId, newStatus):
        print("Updating cell status")
        self.initLocker(lockerId)
        (cellStatus, cellPin) = self.initCell(cellId)
        hkey = f"cell:{lockerId}_{cellId}"
        if cellStatus != newStatus:
            r.hset(hkey, "status", newStatus)
        if newStatus == CLOSED_CELL:
            r.hset(hkey, "pin", "xxxxxx")

    def setCellPin(self, lockerId, cellId, newPin):
        self.validatePin(newPin)
        self.initLocker(lockerId)
        (cellStatus, cellPin) = self.initCell(cellId)
        allPins = self.getAllPins(cellId)
        if newPin in allPins:
            raise HTTPException(status_code=404, detail="The PIN is already defined for another cell in this locker")
        r.hset(f"cell:{lockerId}_{cellId}", "pin", newPin)
        return (cellStatus, newPin)

    def validatePin(self, pin):
        pinPattern = re.compile('^\\d{6}$')
        if not pinPattern.match(pin):
            raise HTTPException(status_code=400, detail="Enter valid 6 digit PIN")

    def getAllPins(self, skipCellId = "") -> set:
        allPins = set()
        self._pinToCellId = {}
        for anotherCellId in self._cells:
            if anotherCellId != skipCellId:
                hkey = f"cell:{self._lockerId}_{anotherCellId}"
                pin = r.hget(hkey, "pin").decode()
                allPins.add(pin)
                self._pinToCellId[pin] = anotherCellId
        return allPins

    def enterPin(self, lockerId, pin) -> str:
        self.validatePin(pin)
        self.initLocker(lockerId)
        allPins = self.getAllPins()
        if pin not in allPins:
            raise HTTPException(status_code=404, detail=f"The PIN does not match to any cell (locker ID: {lockerId}")
        cellId = self._pinToCellId[pin]
        # open cell
        hkey = f"cell:{lockerId}_{cellId}"
        r.hset(hkey, "status", OPEN_CELL)
        return cellId


#TODO add authentication layer

@app.get("/")
async def root():
    return {"message": "Parcel Locker Service", "version": "1.0"}

@app.get("/locker/{locker_id}")
async def get_locker(locker_id: int):
    pl = ParcelLocker()
    pl.initLocker(locker_id)
    return LockerDTO(lockerId=pl._lockerId, cells=pl._cells)

@app.post("/locker/{locker_id}/enterPIN")
async def enter_pin(locker_id: int, body: PinDTO):
    cellId = ParcelLocker().enterPin(locker_id, body.pin)
    return CellDTO(lockerId=locker_id, cellId=cellId, status=OPEN_CELL, pin=body.pin)

@app.get("/locker/{locker_id}/cell/{cell_id}")
async def get_cell(locker_id: int, cell_id: str):
    pl = ParcelLocker()
    pl.initLocker(locker_id)
    (status, pin) = pl.initCell(cell_id)
    return CellDTO(lockerId=locker_id, cellId=cell_id, status=status, pin=pin)

@app.post("/locker/{locker_id}/cell/{cell_id}/open")
async def open_cell(locker_id: int, cell_id: str):
    ParcelLocker().setCellStatus(locker_id, cell_id, OPEN_CELL)
    return CellDTO(lockerId=locker_id, cellId=cell_id, status=OPEN_CELL)

@app.post("/locker/{locker_id}/cell/{cell_id}/close")
async def close_cell(locker_id: int, cell_id: str, body: PinDTO):
    ParcelLocker().setCellStatus(locker_id, cell_id, CLOSED_CELL)
    return CellDTO(lockerId=locker_id, cellId=cell_id, status=CLOSED_CELL)

@app.post("/locker/{locker_id}/cell/{cell_id}/setPIN")
async def set_cell_pin(locker_id: int, cell_id: str, body: PinDTO):
    pl = ParcelLocker()
    (status, pin) = pl.setCellPin(locker_id, cell_id, body.pin)
    return CellDTO(lockerId=locker_id, cellId=cell_id, pin=pin, status=status)

