from __future__ import annotations

from typing import Any, Literal, Union
from typing_extensions import NotRequired, TypedDict


FolderTypeLiteral = Literal["input", "output", "temp"]


class FileResultDict(TypedDict):
    filename: str
    subfolder: str
    type: FolderTypeLiteral


class ImagesUIOutput(TypedDict):
    images: list[FileResultDict]
    animated: NotRequired[tuple[bool]]


class AudioUIOutput(TypedDict):
    audio: list[FileResultDict]


class VideoUIOutput(TypedDict):
    images: list[FileResultDict | dict[str, Any]]
    animated: tuple[Literal[True]]


class TextUIOutput(TypedDict):
    text: tuple[str, ...]


class CameraInfoDict(TypedDict, total=False):
    position: dict[str, float | int]
    target: dict[str, float | int]
    zoom: int
    cameraType: str


class UI3DUIOutput(TypedDict):
    result: list[str | CameraInfoDict | None]


class LatentsUIOutput(TypedDict):
    latents: list[FileResultDict]


UIOutputDict = Union[
    ImagesUIOutput,
    AudioUIOutput,
    VideoUIOutput,
    TextUIOutput,
    UI3DUIOutput,
    LatentsUIOutput,
]
