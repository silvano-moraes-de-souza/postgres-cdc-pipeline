import json

import pytest

from postgres_cdc_pipeline.cli import main


def test_compare_prints_one_row_per_method(source_url, target_url, data, tmp_path, capsys):
    out = tmp_path / "r.json"
    main(["compare", "--data", str(data), "--cycles", "2", "--source", source_url,
          "--target", target_url, "--json", str(out)])  # fmt: skip
    table = capsys.readouterr().out
    for method in ("watermark", "trigger_seq", "trigger_queue", "wal"):
        assert f"| {method} |" in table
    assert len(json.loads(out.read_text())) == 4


def test_generate_writes_parquet(tmp_path):
    main(["generate", "--scale", "0.001", "--out", str(tmp_path / "d")])
    assert list((tmp_path / "d" / "orders").glob("part-*.parquet"))


def test_run_without_urls_is_an_error(monkeypatch, data):
    monkeypatch.delenv("SOURCE_URL", raising=False)
    monkeypatch.delenv("TARGET_URL", raising=False)
    with pytest.raises(SystemExit):
        main(["run", "--method", "wal", "--data", str(data)])
