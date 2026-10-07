from __future__ import annotations

from typing import TYPE_CHECKING, cast

import cv2

from .source import SourceError, redact_url

if TYPE_CHECKING:
    import numpy as np
    from numpy.typing import NDArray


class OpenCVSource:
    def __init__(self, url: str, *, api_preference: int = cv2.CAP_ANY) -> None:
        self._url = url
        self._api_preference = api_preference
        self._capture: cv2.VideoCapture | None = None

    def open(self) -> None:
        self.close()
        capture = cv2.VideoCapture(self._url, self._api_preference)
        if not capture.isOpened():
            capture.release()
            raise SourceError(f"cannot open {redact_url(self._url)}")
        self._capture = capture

    def read(self) -> NDArray[np.uint8]:
        if self._capture is None:
            raise SourceError("source is not open")
        ok, frame = self._capture.read()
        if not ok:
            raise SourceError(f"read failed on {redact_url(self._url)}")
        return cast("NDArray[np.uint8]", frame)

    def close(self) -> None:
        if self._capture is not None:
            self._capture.release()
            self._capture = None
