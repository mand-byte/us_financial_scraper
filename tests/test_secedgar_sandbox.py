import os

import pytest

secedgar = pytest.importorskip("secedgar")
from secedgar import FilingType, filings


def test_secedgar(tmp_path):
    print("Testing secedgar download... AAPL 10-Q (1 document)")
    my_filings = filings(
        cik_lookup="AAPL",
        filing_type=FilingType.FILING_10Q,
        count=1,
        user_agent="Google_Deepmind_Tester (test@google.com)",
    )

    save_path = os.fspath(tmp_path)
    print(f"Saving to {save_path}")
    my_filings.save(save_path)
    print("Download complete. Checking downloaded files:")

    downloaded_files = []
    for root, dirs, files in os.walk(save_path):
        for file in files:
            if file.endswith(".txt") or file.endswith(".htm"):
                downloaded_files.append(os.path.join(root, file))

    assert downloaded_files, "secedgar 未下载到任何 txt/html 文件"

    with open(downloaded_files[0], "r", encoding="utf-8", errors="ignore") as f:
        content = f.read(50000)

    assert content, "下载文件为空"


if __name__ == "__main__":
    test_secedgar()
