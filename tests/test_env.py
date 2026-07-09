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


def test_single_item_pool_returns_str(monkeypatch):
    """A pool var holding one token collapses to the bare string, not a
    one-element list — distinct from the legacy-var path above."""
    monkeypatch.delenv("LEMONGRASS_CONFIG", raising=False)
    monkeypatch.setenv("RACEMONITOR_TOKENS", "only")
    assert _env.resolve_tokens() == "only"


def test_pool_var_name_is_configurable(monkeypatch, tmp_path):
    cfg = tmp_path / "c.toml"
    cfg.write_text('[racemonitor]\ntokens_env = "MY_POOL"\n')
    monkeypatch.setenv("LEMONGRASS_CONFIG", str(cfg))
    monkeypatch.delenv("RACEMONITOR_TOKENS", raising=False)
    monkeypatch.setenv("MY_POOL", "x,y")
    assert _env.resolve_tokens() == ["x", "y"]


def test_custom_pool_var_ignores_stale_legacy_token(monkeypatch, tmp_path):
    cfg = tmp_path / "c.toml"
    cfg.write_text('[racemonitor]\ntokens_env = "MY_POOL"\n')
    monkeypatch.setenv("LEMONGRASS_CONFIG", str(cfg))
    monkeypatch.delenv("MY_POOL", raising=False)
    monkeypatch.setenv("RACEMONITOR_TOKEN", "stale-legacy")
    assert _env.resolve_tokens() == ""


def test_hint_names_both_vars_for_default_pool(monkeypatch):
    monkeypatch.delenv("LEMONGRASS_CONFIG", raising=False)
    assert _env.tokens_env_hint() == "RACEMONITOR_TOKENS or RACEMONITOR_TOKEN"


def test_hint_names_only_the_custom_pool_var(monkeypatch, tmp_path):
    cfg = tmp_path / "c.toml"
    cfg.write_text('[racemonitor]\ntokens_env = "MY_POOL"\n')
    monkeypatch.setenv("LEMONGRASS_CONFIG", str(cfg))
    assert _env.tokens_env_hint() == "MY_POOL"
