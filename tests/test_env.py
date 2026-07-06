from lemongrass import _env


def test_reads_default_pool_var(monkeypatch):
    monkeypatch.delenv("LEMONGRASS_CONFIG", raising=False)
    monkeypatch.setenv("RACEMONITOR_TOKENS", "a,b,c")
    assert _env.resolve_tokens() == ["a", "b", "c"]


def test_single_token_returns_str(monkeypatch):
    monkeypatch.delenv("LEMONGRASS_CONFIG", raising=False)
    monkeypatch.delenv("RACEMONITOR_TOKENS", raising=False)
    monkeypatch.setenv("RACEMONITOR_TOKEN", "solo")
    assert _env.resolve_tokens() == "solo"


def test_pool_var_name_is_configurable(monkeypatch, tmp_path):
    cfg = tmp_path / "c.toml"
    cfg.write_text('[racemonitor]\ntokens_env = "MY_POOL"\n')
    monkeypatch.setenv("LEMONGRASS_CONFIG", str(cfg))
    monkeypatch.delenv("RACEMONITOR_TOKENS", raising=False)
    monkeypatch.setenv("MY_POOL", "x,y")
    assert _env.resolve_tokens() == ["x", "y"]
