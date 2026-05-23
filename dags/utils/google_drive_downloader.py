from __future__ import annotations

import html
import re
import zipfile
import warnings
from pathlib import Path
from sys import stdout
from urllib.parse import parse_qs, urljoin, urlparse

import requests


class GoogleDriveDownloader:
    """
    Downloader custom để tải file public từ Google Drive bằng file_id.

    Có xử lý trường hợp Google Drive trả về trang xác nhận virus scan
    thay vì trả file trực tiếp.
    """

    CHUNK_SIZE = 32768
    UC_DOWNLOAD_URL = "https://docs.google.com/uc"
    USER_CONTENT_DOWNLOAD_URL = "https://drive.usercontent.google.com/download"

    @staticmethod
    def download_file_from_google_drive(
        file_id: str,
        dest_path: str,
        overwrite: bool = False,
        unzip: bool = False,
        showsize: bool = True,
    ) -> None:
        dest = Path(dest_path)
        dest.parent.mkdir(parents=True, exist_ok=True)

        if dest.exists() and not overwrite:
            print(f"File already exists, skip download: {dest}")
            return

        session = requests.Session()

        response = GoogleDriveDownloader._get_download_response(session, file_id)

        GoogleDriveDownloader._save_response_content(
            response=response,
            destination=dest,
            showsize=showsize,
        )

        GoogleDriveDownloader._validate_downloaded_file(dest)

        print(f"\nDownload completed: {dest}")

        if unzip:
            GoogleDriveDownloader._unzip_file(dest)

    @staticmethod
    def _get_download_response(
        session: requests.Session,
        file_id: str,
    ) -> requests.Response:
        response = session.get(
            GoogleDriveDownloader.UC_DOWNLOAD_URL,
            params={
                "export": "download",
                "id": file_id,
            },
            stream=True,
        )

        if not GoogleDriveDownloader._is_html_response(response):
            return response

        token = GoogleDriveDownloader._get_confirm_token_from_cookies(response)

        if token:
            return session.get(
                GoogleDriveDownloader.UC_DOWNLOAD_URL,
                params={
                    "export": "download",
                    "confirm": token,
                    "id": file_id,
                },
                stream=True,
            )

        download_url, params = GoogleDriveDownloader._extract_download_url_from_html(
            response.text,
            file_id,
        )

        if download_url:
            return session.get(
                download_url,
                params=params,
                stream=True,
            )

        return session.get(
            GoogleDriveDownloader.USER_CONTENT_DOWNLOAD_URL,
            params={
                "export": "download",
                "confirm": "t",
                "id": file_id,
            },
            stream=True,
        )

    @staticmethod
    def _is_html_response(response: requests.Response) -> bool:
        content_type = response.headers.get("Content-Type", "").lower()

        if "text/html" in content_type:
            return True

        sample = response.content[:512].lower()

        return b"<html" in sample or b"<!doctype html" in sample

    @staticmethod
    def _get_confirm_token_from_cookies(response: requests.Response) -> str | None:
        for key, value in response.cookies.items():
            if key.startswith("download_warning"):
                return value

        return None

    @staticmethod
    def _extract_download_url_from_html(
        html_text: str,
        file_id: str,
    ) -> tuple[str | None, dict[str, str] | None]:
        html_text = html.unescape(html_text)

        href_match = re.search(
            r'href="([^"]*(?:uc\?export=download|drive\.usercontent\.google\.com/download)[^"]*)"',
            html_text,
        )

        if href_match:
            raw_href = href_match.group(1).replace("&amp;", "&")
            full_url = urljoin("https://docs.google.com", raw_href)

            parsed = urlparse(full_url)
            query_params = parse_qs(parsed.query)

            params = {
                key: values[0]
                for key, values in query_params.items()
                if values
            }

            if "id" not in params:
                params["id"] = file_id

            clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"

            return clean_url, params

        token_match = re.search(r"confirm=([0-9A-Za-z_\-]+)", html_text)

        if token_match:
            return GoogleDriveDownloader.UC_DOWNLOAD_URL, {
                "export": "download",
                "confirm": token_match.group(1),
                "id": file_id,
            }

        return None, None

    @staticmethod
    def _save_response_content(
        response: requests.Response,
        destination: Path,
        showsize: bool,
    ) -> None:
        response.raise_for_status()

        current_size = 0

        with destination.open("wb") as file:
            for chunk in response.iter_content(GoogleDriveDownloader.CHUNK_SIZE):
                if chunk:
                    file.write(chunk)
                    current_size += len(chunk)

                    if showsize:
                        print(
                            "\rDownloaded: "
                            + GoogleDriveDownloader._sizeof_fmt(current_size),
                            end=" ",
                        )
                        stdout.flush()

    @staticmethod
    def _validate_downloaded_file(file_path: Path) -> None:
        if not file_path.exists():
            raise FileNotFoundError(f"Downloaded file not found: {file_path}")

        if file_path.stat().st_size == 0:
            raise ValueError(f"Downloaded file is empty: {file_path}")

        with file_path.open("rb") as file:
            head = file.read(1024).lower()

        if b"<html" in head or b"<!doctype html" in head:
            raise ValueError(
                f"Downloaded file is HTML, not CSV: {file_path}. "
                "Check Google Drive permission, quota, or virus scan warning."
            )

    @staticmethod
    def _unzip_file(file_path: Path) -> None:
        try:
            with zipfile.ZipFile(file_path, "r") as zip_file:
                zip_file.extractall(file_path.parent)

        except zipfile.BadZipFile:
            warnings.warn(f"Ignoring unzip because {file_path} is not a zip file.")

    @staticmethod
    def _sizeof_fmt(num: float, suffix: str = "B") -> str:
        for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi"]:
            if abs(num) < 1024.0:
                return f"{num:.1f} {unit}{suffix}"

            num /= 1024.0

        return f"{num:.1f} Ei{suffix}"