# Changelog

## [2.0.0](https://github.com/WOT-Lemons/Lemongrass/compare/v1.0.2...v2.0.0) (2026-06-21)


### ⚠ BREAKING CHANGES

* old entry points removed; use `lemongrass <cmd>` form

### Features

* consolidate CLI to lemongrass dispatcher and add PyPI publishing ([9e5f85a](https://github.com/WOT-Lemons/Lemongrass/commit/9e5f85ad7352fcb03c12116faef0ca83c02103a8))

## [1.0.2](https://github.com/WOT-Lemons/Lemongrass/compare/v1.0.1...v1.0.2) (2026-06-21)


### Bug Fixes

* **docker:** install lemongrass as non-editable wheel in final image ([#121](https://github.com/WOT-Lemons/Lemongrass/issues/121)) ([e579fad](https://github.com/WOT-Lemons/Lemongrass/commit/e579faddec1b96ac1ec276b1f91a566c67e97566))

## [1.0.1](https://github.com/WOT-Lemons/Lemongrass/compare/v1.0.0...v1.0.1) (2026-06-20)


### Bug Fixes

* handle Ctrl+C gracefully in laps and race-backfill ([32be58e](https://github.com/WOT-Lemons/Lemongrass/commit/32be58e0638f09c037a7669649de2083c280ec89))

## [1.0.0](https://github.com/WOT-Lemons/Lemongrass/compare/v0.17.0...v1.0.0) (2026-06-19)


### ⚠ BREAKING CHANGES

* Docker image renamed from lemongrass-laps/lemongrass-pi to lemongrass. Runtime commands changed from `python <script>.py` to console script names (telem, pisugar-monitor, laps). Consuming repo's docker-compose must be updated before deploying.

### Features

* restructure as src layout with console_scripts and multi-arch Docker image ([5668b1c](https://github.com/WOT-Lemons/Lemongrass/commit/5668b1ca0d7e1f3e742a8b637520e4c0604eec52))

## [0.17.0](https://github.com/WOT-Lemons/Lemongrass/compare/v0.16.0...v0.17.0) (2026-06-19)


### Features

* **rankings:** show class and class position in rankings and competitor summary ([f2c41f8](https://github.com/WOT-Lemons/Lemongrass/commit/f2c41f80205c3ccd587be50618711fdff16b1a40))

## [0.16.0](https://github.com/WOT-Lemons/Lemongrass/compare/v0.15.0...v0.16.0) (2026-06-19)


### Features

* **backfill:** skip already-backfilled races; fix lap time parsing; show class ([66d5207](https://github.com/WOT-Lemons/Lemongrass/commit/66d5207feee388cbfb660c38f2b42ce57180fd19))

## [0.15.0](https://github.com/WOT-Lemons/Lemongrass/compare/v0.14.0...v0.15.0) (2026-06-15)


### Features

* **influx:** batch point writes to reduce HTTP requests ([#105](https://github.com/WOT-Lemons/Lemongrass/issues/105)) ([1f09656](https://github.com/WOT-Lemons/Lemongrass/commit/1f09656fb21df903e4fb7bb41931df9fb745517b))

## [0.14.0](https://github.com/WOT-Lemons/Lemongrass/compare/v0.13.0...v0.14.0) (2026-06-15)


### Features

* **laps:** delete-and-replace car laps on historical backfill ([#103](https://github.com/WOT-Lemons/Lemongrass/issues/103)) ([8ea247b](https://github.com/WOT-Lemons/Lemongrass/commit/8ea247b3f4fc7bfe857209e8c25833c2bbbf3483))

## [0.13.0](https://github.com/WOT-Lemons/Lemongrass/compare/v0.12.0...v0.13.0) (2026-06-08)


### Features

* **laps:** InfluxDB schema redesign with laps/races buckets and historical backfill ([2c6e3c0](https://github.com/WOT-Lemons/Lemongrass/commit/2c6e3c0a3cfd2befc4cec04cdc5c9b78f45e25fe))

## [0.12.0](https://github.com/WOT-Lemons/Lemongrass/compare/v0.11.2...v0.12.0) (2026-06-07)


### Features

* **laps:** enrich InfluxDB lap writes with race and competitor metadata ([a2c9277](https://github.com/WOT-Lemons/Lemongrass/commit/a2c9277ba1c98406d2cffd637bbd7a6986e985de))

## [0.11.2](https://github.com/WOT-Lemons/Lemongrass/compare/v0.11.1...v0.11.2) (2026-06-06)


### Bug Fixes

* **laps:** anchor historical timestamps to SessionStartDateEpoc ([#94](https://github.com/WOT-Lemons/Lemongrass/issues/94)) ([da710d1](https://github.com/WOT-Lemons/Lemongrass/commit/da710d109880f5045681e6152baf8bc9bf5c629a))

## [0.11.1](https://github.com/WOT-Lemons/Lemongrass/compare/v0.11.0...v0.11.1) (2026-06-06)


### Bug Fixes

* **laps:** don't write stale class_position on initial live_race push ([2ce05d3](https://github.com/WOT-Lemons/Lemongrass/commit/2ce05d32d52af7c2d5972b506409d8242f08ca16))

## [0.11.0](https://github.com/WOT-Lemons/Lemongrass/compare/v0.10.0...v0.11.0) (2026-06-06)


### Features

* **laps:** add class tag and class_position field to influx lap writes ([5dcd7fb](https://github.com/WOT-Lemons/Lemongrass/commit/5dcd7fbbd8b0b332d4bdc29d1621b9327d3eb896))

## [0.10.0](https://github.com/WOT-Lemons/Lemongrass/compare/v0.9.1...v0.10.0) (2026-05-30)


### Features

* **telem:** connect OBD on configurable /dev/obd port ([#88](https://github.com/WOT-Lemons/Lemongrass/issues/88)) ([fa0283e](https://github.com/WOT-Lemons/Lemongrass/commit/fa0283eebfaf36ecb1b113111b9448c715e47ab9))

## [0.9.1](https://github.com/WOT-Lemons/Lemongrass/compare/v0.9.0...v0.9.1) (2026-05-24)


### Bug Fixes

* **docker:** fix docker venv PATH and clean up files migrated to iac ([#78](https://github.com/WOT-Lemons/Lemongrass/issues/78)) ([002fd1d](https://github.com/WOT-Lemons/Lemongrass/commit/002fd1db73bd4cc3d46acbbbfba5dc8a2621b7f7))

## [0.9.0](https://github.com/WOT-Lemons/Lemongrass/compare/v0.8.0...v0.9.0) (2026-05-11)


### Features

* document both live and completed races for laps.py ([#70](https://github.com/WOT-Lemons/Lemongrass/issues/70)) ([c505747](https://github.com/WOT-Lemons/Lemongrass/commit/c505747419b89af4d6a6bc24946bf69075be8bd9))

## [0.8.0](https://github.com/WOT-Lemons/Lemongrass/compare/v0.7.0...v0.8.0) (2026-05-11)


### Features

* **deps:** race-monitor 0.4.0 ([#67](https://github.com/WOT-Lemons/Lemongrass/issues/67)) ([bfb028f](https://github.com/WOT-Lemons/Lemongrass/commit/bfb028fb661588ed2e9c87d8789ddc3e6bd7edf3))

## [0.7.0](https://github.com/WOT-Lemons/Lemongrass/compare/v0.6.0...v0.7.0) (2026-05-11)


### Features

* **deps:** python 3.14 ([#55](https://github.com/WOT-Lemons/Lemongrass/issues/55)) ([301610e](https://github.com/WOT-Lemons/Lemongrass/commit/301610e7995e0b3900b659208011ff64fd28a9f8))
* upgrade to race-monitor 0.3.0 ([#57](https://github.com/WOT-Lemons/Lemongrass/issues/57)) ([2602e4b](https://github.com/WOT-Lemons/Lemongrass/commit/2602e4b609311bf32a34cdac080dc970427439f5))
* use uv base images ([#65](https://github.com/WOT-Lemons/Lemongrass/issues/65)) ([6c899d2](https://github.com/WOT-Lemons/Lemongrass/commit/6c899d23dfd5bbb44bdd344bf0199e07d6d7d8f2))

## [0.6.0](https://github.com/WOT-Lemons/Lemongrass/compare/v0.5.0...v0.6.0) (2026-05-10)


### Features

* switch to race-monitor package ([#50](https://github.com/WOT-Lemons/Lemongrass/issues/50)) ([672ab4e](https://github.com/WOT-Lemons/Lemongrass/commit/672ab4ed01cb35d62f5e7f63d69c107c2a4de4e0))

## [0.5.0](https://github.com/WOT-Lemons/Lemongrass/compare/v0.4.2...v0.5.0) (2026-05-09)


### Features

* comply with pylint and use uv for dependencies ([#49](https://github.com/WOT-Lemons/Lemongrass/issues/49)) ([8ac5269](https://github.com/WOT-Lemons/Lemongrass/commit/8ac52694e67b11908a738d42a21e5123d9a3ebd0))
* pisguar token expiration handler ([#47](https://github.com/WOT-Lemons/Lemongrass/issues/47)) ([3a77ad1](https://github.com/WOT-Lemons/Lemongrass/commit/3a77ad14142eb0d4e4a75a1d13293aad8f9d7fa8))

## [0.4.2](https://github.com/WOT-Lemons/Lemongrass/compare/v0.4.1...v0.4.2) (2026-05-07)


### Bug Fixes

* Fix indentation ([#44](https://github.com/WOT-Lemons/Lemongrass/issues/44)) ([6478986](https://github.com/WOT-Lemons/Lemongrass/commit/64789862a4181d7a36ac17d7af57f6b9c980ae8a))

## [0.4.1](https://github.com/WOT-Lemons/Lemongrass/compare/v0.4.0...v0.4.1) (2026-05-07)


### Bug Fixes

* pisugar http API format ([#42](https://github.com/WOT-Lemons/Lemongrass/issues/42)) ([e649973](https://github.com/WOT-Lemons/Lemongrass/commit/e6499735f5a9049eab79e7740382dc4193066241))

## [0.4.0](https://github.com/WOT-Lemons/Lemongrass/compare/v0.3.0...v0.4.0) (2026-05-07)


### Features

* Use pisugar http API instead of python ([#40](https://github.com/WOT-Lemons/Lemongrass/issues/40)) ([36727cc](https://github.com/WOT-Lemons/Lemongrass/commit/36727ccd5ae0649a089928940ef813905b824b0c))

## [0.3.0](https://github.com/WOT-Lemons/Lemongrass/compare/v0.2.1...v0.3.0) (2026-05-05)


### Features

* add tags to pisguar data points ([e18e3ab](https://github.com/WOT-Lemons/Lemongrass/commit/e18e3abbb418f39b946db05857ea80f5ecad14ab))

## [0.2.1](https://github.com/WOT-Lemons/Lemongrass/compare/v0.2.0...v0.2.1) (2026-04-26)


### Bug Fixes

* **deps:** Revert "Bump python from `46cb7cc` to `4bdca44` ([#22](https://github.com/WOT-Lemons/Lemongrass/issues/22))" ([#32](https://github.com/WOT-Lemons/Lemongrass/issues/32)) ([8eec684](https://github.com/WOT-Lemons/Lemongrass/commit/8eec6843def54e48a883e319e1e4b19ded68cf24))

## [0.2.0](https://github.com/WOT-Lemons/Lemongrass/compare/v0.1.1...v0.2.0) (2026-04-26)


### Features

* separate and better influx token names ([#30](https://github.com/WOT-Lemons/Lemongrass/issues/30)) ([82e0af7](https://github.com/WOT-Lemons/Lemongrass/commit/82e0af724c440dfde1a59f34baefe480816b6ef2))

## [0.1.1](https://github.com/WOT-Lemons/Lemongrass/compare/v0.1.0...v0.1.1) (2026-04-26)


### Bug Fixes

* **deps:** Bump python from `46cb7cc` to `4bdca44` ([#22](https://github.com/WOT-Lemons/Lemongrass/issues/22)) ([561276a](https://github.com/WOT-Lemons/Lemongrass/commit/561276aba64d1956b6fad64b5848d54b80efced8))

## [0.1.0](https://github.com/WOT-Lemons/Lemongrass/compare/v0.0.1...v0.1.0) (2026-04-26)


### Features

* initial release ([3abc023](https://github.com/WOT-Lemons/Lemongrass/commit/3abc02379faaec7d7854fe95ff40c97439b59996))
* initial release ([89c2d07](https://github.com/WOT-Lemons/Lemongrass/commit/89c2d07455867b28264f18073b9c1c632df83aac))
* initial release ([#26](https://github.com/WOT-Lemons/Lemongrass/issues/26)) ([ba9915e](https://github.com/WOT-Lemons/Lemongrass/commit/ba9915e94cd95783545db09a3f960aaee52bebfb))
* release please fixes ([ab6bbd0](https://github.com/WOT-Lemons/Lemongrass/commit/ab6bbd042c821b851c73011bc126418ec23e459b))
