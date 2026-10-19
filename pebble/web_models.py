"""Validated Firestore inputs for the local worker, independent of Sheets."""

from __future__ import annotations

import re
from datetime import date, datetime, timezone
from typing import Annotated, Literal
from zoneinfo import ZoneInfo

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StrictInt,
    model_validator,
)

from .config_loader import MongoConfig, RedisConfig, TimeConfig, WCLConfig

ID = re.compile(r"^[A-Za-z0-9_-]{1,100}$")
CODE = re.compile(r"^[A-Za-z0-9]{16}$")
PT = ZoneInfo("America/Los_Angeles")


class InputDocument(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    version: Annotated[StrictInt, Field(ge=1)]
    updatedBy: Annotated[str, Field(min_length=1, max_length=128)]
    updatedAt: datetime


class ReportInput(InputDocument):
    enabled: StrictBool
    notes: Annotated[str, Field(max_length=1000)] = ""


class PlayerInput(InputDocument):
    name: Annotated[str, Field(min_length=1, max_length=32)]
    className: Literal[
        "Death Knight",
        "Demon Hunter",
        "Druid",
        "Evoker",
        "Hunter",
        "Mage",
        "Monk",
        "Paladin",
        "Priest",
        "Rogue",
        "Shaman",
        "Warlock",
        "Warrior",
        "Unknown",
    ]
    role: Literal["Tank", "Healer", "Damage", "Unknown"] = "Unknown"
    active: StrictBool = True
    joinDate: str = ""
    leaveDate: str = ""
    notes: Annotated[str, Field(max_length=1000)] = ""

    @model_validator(mode="after")
    def validate_identity(self):
        if not self.name.isalpha():
            raise ValueError(
                "Main names must contain letters only; use character mappings for realms and alts"
            )
        for value in (self.joinDate, self.leaveDate):
            if value:
                date.fromisoformat(value)
        if self.joinDate and self.leaveDate and self.joinDate > self.leaveDate:
            raise ValueError("Join date must not follow leave date")
        return self


class CharacterInput(InputDocument):
    name: Annotated[str, Field(min_length=1, max_length=32)]
    realm: Annotated[str, Field(max_length=100)]
    region: Literal["us", "eu", "kr", "tw", "cn"]
    playerId: Annotated[str, Field(min_length=1, max_length=100)]


class Availability(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    mode: Literal["automatic", "available", "unavailable", "partial", "unavailableFor"]
    minutes: Annotated[StrictInt, Field(ge=1, le=720)] | None = None

    @model_validator(mode="after")
    def check_minutes(self):
        if (self.mode in ("partial", "unavailableFor")) != (self.minutes is not None):
            raise ValueError(
                "Partial availability and timed unavailability require minutes"
            )
        return self

    def effective(self):
        return {
            "automatic": None,
            "available": True,
            "unavailable": False,
            "partial": self.minutes,
            "unavailableFor": -self.minutes if self.minutes is not None else None,
        }[self.mode]


class AvailabilityInput(InputDocument):
    nightId: str
    playerId: str
    pre: Availability
    post: Availability
    notes: Annotated[str, Field(max_length=1000)] = ""


class TimingInput(InputDocument):
    start: str = ""
    breakStart: str = ""
    breakEnd: str = ""
    end: str = ""
    notes: Annotated[str, Field(max_length=1000)] = ""


def wall_ms(value: str) -> int | None:
    """Convert an explicit Pacific wall time, rejecting DST gaps and ambiguity."""
    if not value:
        return None
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}", value):
        raise ValueError("Use a date and Pacific time in YYYY-MM-DDTHH:MM format")
    naive = datetime.fromisoformat(value)
    dt = naive.replace(tzinfo=PT)
    if dt.astimezone(timezone.utc).astimezone(PT).replace(tzinfo=None) != naive:
        raise ValueError(
            "This Pacific time does not exist because of daylight saving time"
        )
    if dt.utcoffset() != naive.replace(tzinfo=PT, fold=1).utcoffset():
        raise ValueError(
            "This Pacific time is ambiguous because of daylight saving time"
        )
    return int(dt.timestamp() * 1000)


INPUT_MODELS = {
    "reports": ReportInput,
    "players": PlayerInput,
    "characters": CharacterInput,
    "nightOverrides": TimingInput,
    "availabilityOverrides": AvailabilityInput,
}


def validate_snapshot(raw: dict) -> dict:
    """Validate document schemas and relationships before contacting WCL."""
    result = {}
    for collection, model in INPUT_MODELS.items():
        result[collection] = {}
        for key, value in raw.get(collection, {}).items():
            if not ID.fullmatch(key):
                raise ValueError(f"Invalid {collection} document ID")
            result[collection][key] = model.model_validate(value)
    names = [p.name.casefold() for p in result["players"].values()]
    if len(names) != len(set(names)):
        raise ValueError("Main player names must be unique")
    for code in result["reports"]:
        if not CODE.fullmatch(code):
            raise ValueError("WCL report codes must contain 16 letters and numbers")
    aliases = {}
    for character in result["characters"].values():
        if character.playerId not in result["players"]:
            raise ValueError("Character mapping references an unknown player")
        identity = (character.name.casefold(), character.realm.casefold())
        if identity in aliases:
            raise ValueError("A character cannot have multiple main mappings")
        aliases[identity] = character.playerId
    for key, adjustment in result["availabilityOverrides"].items():
        date.fromisoformat(adjustment.nightId)
        if (
            adjustment.playerId not in result["players"]
            or key != f"{adjustment.nightId}_{adjustment.playerId}"
        ):
            raise ValueError(
                "Availability adjustment references an unknown player or mismatched ID"
            )
    for night, timing in result["nightOverrides"].items():
        date.fromisoformat(night)
        values = [
            wall_ms(timing.start),
            wall_ms(timing.breakStart),
            wall_ms(timing.breakEnd),
            wall_ms(timing.end),
        ]
        if bool(timing.breakStart) != bool(timing.breakEnd):
            raise ValueError("Set both break boundaries or leave both automatic")
        explicit = [v for v in values if v is not None]
        if any(b <= a for a, b in zip(explicit, explicit[1:])):
            raise ValueError("Timing must follow start, break start, break end, end")
        if explicit and explicit[-1] - explicit[0] > 12 * 60 * 60 * 1000:
            raise ValueError("Raid timing cannot span more than 12 hours")
    return result


class FirestoreConfig(BaseModel):
    project_id: str
    credentials_file: str | None = None
    season_id: Annotated[str, Field(pattern=r"^[A-Za-z0-9_-]{1,100}$")]


class WorkerConfig(BaseModel):
    """Local configuration; no Sheets credentials or cell references required."""

    firestore: FirestoreConfig
    mongo: MongoConfig
    wcl: WCLConfig
    redis: RedisConfig = Field(default_factory=RedisConfig)
    time: TimeConfig = Field(default_factory=TimeConfig)


class CalculationInput(InputDocument):
    tz: Literal["America/Los_Angeles"] = "America/Los_Angeles"
    breakWindowStart: str = "20:50"
    breakWindowEnd: str = "21:30"
    minBreakMinutes: Annotated[StrictInt, Field(ge=1, le=120)] = 10
    maxBreakMinutes: Annotated[StrictInt, Field(ge=1, le=120)] = 30
    defaultMythicStart: str = ""
    postExtensionMinutes: Annotated[float, Field(ge=0, le=60)] = 5.0

    @model_validator(mode="after")
    def check_window(self):
        for value in (
            self.breakWindowStart,
            self.breakWindowEnd,
            self.defaultMythicStart,
        ):
            if value and not re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", value):
                raise ValueError("Use HH:MM for calculation settings")
        if self.minBreakMinutes > self.maxBreakMinutes:
            raise ValueError("Minimum break duration exceeds maximum")
        return self

    def time_config(self) -> TimeConfig:
        return TimeConfig(
            tz=self.tz,
            break_window={
                "start_pt": self.breakWindowStart,
                "end_pt": self.breakWindowEnd,
                "min_gap_minutes": self.minBreakMinutes,
                "max_gap_minutes": self.maxBreakMinutes,
            },
            mythic_default_start_pt=self.defaultMythicStart,
            mythic_post_extension_min=self.postExtensionMinutes,
        )
