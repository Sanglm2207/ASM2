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
    Custom Google Drive downloader.

    Dùng để tải file public từ Google Drive bằng file_id.

    Xử lý được case Google Drive trả về trang HTML xác nhận:
    - "Google Drive can't scan this file for viruses"
    - "Download anyway"
    - confirm token trong cookie
    - confirm token trong HTML link/form
    """

    CHUNK_SIZE = 32768

    # Endpoint cũ hay dùng.
    UC_DOWNLOAD_URL = "https://docs.google.com/uc"

    # Endpoint mới Google hay redirect sang khi file lớn.
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

        print(f"Downloading Google Drive file_id={file_id}")
        print(f"Destination: {dest}")

        response = GoogleDriveDownloader._get_download_response(
            session=session,
            file_id=file_id,
        )

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
        """
        Lấy response tải file thật từ Google Drive.

        Nếu lần đầu Google trả về HTML warning page,
        function này sẽ cố gắng parse confirm token/link/form
        rồi request lần 2 để lấy file thật.
        """

        # Request lần 1.
        response = session.get(
            GoogleDriveDownloader.UC_DOWNLOAD_URL,
            params={
                "export": "download",
                "id": file_id,
            },
            stream=True,
        )

        # Case 1: Google trả file trực tiếp.
        if not GoogleDriveDownloader._is_html_response(response):
            return response

        print("Google Drive returned an HTML confirmation page. Trying to bypass...")

        html_text = response.text

        # Case 2: confirm token nằm trong cookie.
        token = GoogleDriveDownloader._get_confirm_token_from_cookies(response)

        if token:
            print("Found confirm token from cookies.")

            return session.get(
                GoogleDriveDownloader.UC_DOWNLOAD_URL,
                params={
                    "export": "download",
                    "confirm": token,
                    "id": file_id,
                },
                stream=True,
            )

        # Case 3: Google trả link download thật trong HTML.
        download_url, params = GoogleDriveDownloader._extract_download_url_from_html(
            html_text=html_text,
            file_id=file_id,
        )

        if download_url:
            print(f"Found real download URL: {download_url}")

            return session.get(
                download_url,
                params=params,
                stream=True,
            )

        # Case 4: fallback sang drive.usercontent endpoint.
        print("Cannot parse confirm link. Trying fallback endpoint...")

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
        """
        Kiểm tra response có phải HTML không.

        Nếu là HTML thì khả năng cao là:
        - warning page
        - permission page
        - login page
        - quota page
        """

        content_type = response.headers.get("Content-Type", "").lower()

        if "text/html" in content_type:
            return True

        # Một số response không set content-type chuẩn,
        # nên đọc thử vài byte đầu.
        try:
            sample = response.content[:512].lower()
            return b"<html" in sample or b"<!doctype html" in sample
        except Exception:
            return False

    @staticmethod
    def _get_confirm_token_from_cookies(response: requests.Response) -> str | None:
        """
        Google Drive đôi khi nhét token vào cookie download_warning_xxx.
        """

        for key, value in response.cookies.items():
            if key.startswith("download_warning"):
                return value

        return None

    @staticmethod
    def _extract_download_url_from_html(
        html_text: str,
        file_id: str,
    ) -> tuple[str | None, dict[str, str] | None]:
        """
        Parse HTML warning page để tìm link/form download thật.

        Google Drive có thể trả:
        - href chứa confirm token
        - form action tới drive.usercontent.google.com/download
        """

        html_text = html.unescape(html_text)

        # -------------------------------------------------------------
        # Case A: tìm href có confirm token.
        # -------------------------------------------------------------
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

        # -------------------------------------------------------------
        # Case B: tìm form action và hidden input.
        # -------------------------------------------------------------
        form_match = re.search(
            r'<form[^>]+action="([^"]+)"[^>]*>(.*?)</form>',
            html_text,
            flags=re.DOTALL | re.IGNORECASE,
        )

        if form_match:
            action = form_match.group(1).replace("&amp;", "&")
            form_body = form_match.group(2)

            input_matches = re.findall(
                r'<input[^>]+>',
                form_body,
                flags=re.IGNORECASE,
            )

            params: dict[str, str] = {}

            for input_tag in input_matches:
                name_match = re.search(
                    r'name="([^"]+)"',
                    input_tag,
                    flags=re.IGNORECASE,
                )
                value_match = re.search(
                    r'value="([^"]*)"',
                    input_tag,
                    flags=re.IGNORECASE,
                )

                if name_match:
                    name = name_match.group(1)
                    value = value_match.group(1) if value_match else ""
                    params[name] = value

            if "id" not in params:
                params["id"] = file_id

            full_action = urljoin("https://docs.google.com", action)

            return full_action, params

        # -------------------------------------------------------------
        # Case C: tìm confirm token bằng regex thô.
        # -------------------------------------------------------------
        token_match = re.search(r"confirm=([0-9A-Za-z_\-]+)", html_text)

        if token_match:
            token = token_match.group(1)

            return GoogleDriveDownloader.UC_DOWNLOAD_URL, {
                "export": "download",
                "confirm": token,
                "id": file_id,
            }

        return None, None

    @staticmethod
    def _save_response_content(
        response: requests.Response,
        destination: Path,
        showsize: bool,
    ) -> None:
        """
        Ghi response content xuống file theo từng chunk.
        """

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
        """
        Kiểm tra file sau khi tải.

        Nếu Google trả HTML thay vì CSV thật thì báo lỗi rõ ràng.
        """

        if not file_path.exists():
            raise FileNotFoundError(f"Downloaded file not found: {file_path}")

        if file_path.stat().st_size == 0:
            raise ValueError(f"Downloaded file is empty: {file_path}")

        with file_path.open("rb") as file:
            head = file.read(1024).lower()

        if b"<html" in head or b"<!doctype html" in head:
            raise ValueError(
                f"Downloaded file is still an HTML page, not a real CSV file: {file_path}. "
                "This usually means Google Drive returned a warning, login, quota, or permission page."
            )

    @staticmethod
    def _unzip_file(file_path: Path) -> None:
        """
        Unzip file nếu file là zip.
        """

        try:
            print("Unzipping file...")

            with zipfile.ZipFile(file_path, "r") as zip_file:
                zip_file.extractall(file_path.parent)

            print("Unzip completed.")

        except zipfile.BadZipFile:
            warnings.warn(
                f'Ignoring unzip because "{file_path}" is not a valid zip file.'
            )

    @staticmethod
    def _sizeof_fmt(num: float, suffix: str = "B") -> str:
        """
        Convert file size sang dạng dễ đọc.
        """

        for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi"]:
            if abs(num) < 1024.0:
                return f"{num:.1f} {unit}{suffix}"
            num /= 1024.0

        return f"{num:.1f} Ei{suffix}"