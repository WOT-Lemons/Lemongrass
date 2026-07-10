from lemongrass._backfill_tui import RaceListModel


def _race(id, name, start_epoc):
    return {'ID': id, 'Name': name, 'StartDateEpoc': start_epoc}


def _model(races_by_term=None, terms=('t1',), start_epoc=0):
    return RaceListModel(terms, races_by_term or {}, start_epoc)


class TestRaceListModel:
    def test_all_races_checked_initially(self):
        m = _model({'t1': [_race(1, 'one', 100), _race(2, 'two', 200)]})
        assert m.checked == {1, 2}

    def test_races_deduped_across_terms_and_date_sorted(self):
        shared = _race(2, 'shared', 200)
        m = _model({'t1': [shared, _race(3, 'three', 300)],
                    't2': [shared, _race(1, 'one', 100)]},
                   terms=('t1', 't2'))
        assert [r['ID'] for r in m.races()] == [1, 2, 3]

    def test_races_filtered_by_start_epoc(self):
        m = _model({'t1': [_race(1, 'old', 50), _race(2, 'new', 150)]},
                   start_epoc=100)
        assert [r['ID'] for r in m.races()] == [2]
        assert m.checked == {2}

    def test_toggle_unchecks_then_rechecks(self):
        m = _model({'t1': [_race(1, 'one', 100)]})
        m.toggle(1)
        assert m.checked == set()
        m.toggle(1)
        assert m.checked == {1}

    def test_set_all_and_invert(self):
        m = _model({'t1': [_race(1, 'one', 100), _race(2, 'two', 200)]})
        m.set_all(False)
        assert m.checked == set()
        m.toggle(1)
        m.invert()
        assert m.checked == {2}
        m.set_all(True)
        assert m.checked == {1, 2}

    def test_add_term_merges_prechecked_preserving_existing_state(self):
        m = _model({'t1': [_race(1, 'one', 100), _race(2, 'two', 200)]})
        m.toggle(1)  # user unchecked race 1
        m.add_term('t2', [_race(2, 'two', 200), _race(3, 'three', 300)])
        assert [r['ID'] for r in m.races()] == [1, 2, 3]
        assert m.checked == {2, 3}  # 1 stays unchecked, new 3 pre-checked

    def test_add_term_filters_by_start_epoc(self):
        m = _model({'t1': [_race(1, 'one', 150)]}, start_epoc=100)
        m.add_term('t2', [_race(2, 'old', 50)])
        assert [r['ID'] for r in m.races()] == [1]

    def test_add_blank_or_duplicate_term_is_noop(self):
        m = _model({'t1': [_race(1, 'one', 100)]})
        m.add_term('  ', [])
        m.add_term('t1', [_race(9, 'dup', 900)])
        assert m.terms == ['t1']
        assert [r['ID'] for r in m.races()] == [1]

    def test_remove_term_drops_only_races_matched_solely_by_that_term(self):
        shared = _race(2, 'shared', 200)
        m = _model({'t1': [_race(1, 'one', 100), shared],
                    't2': [shared, _race(3, 'three', 300)]},
                   terms=('t1', 't2'))
        m.remove_term('t2')
        assert [r['ID'] for r in m.races()] == [1, 2]
        assert m.checked == {1, 2}

    def test_readd_removed_term_uses_cache(self):
        m = _model({'t1': [_race(1, 'one', 100)]})
        m.add_term('t2', [_race(2, 'two', 200)])
        m.remove_term('t2')
        assert m.has_cached('t2')
        m.add_term('t2')  # no results arg: served from session cache
        assert [r['ID'] for r in m.races()] == [1, 2]
        assert m.checked == {1, 2}  # reappearing race is pre-checked again

    def test_terms_changed_tracks_difference_from_initial(self):
        m = _model({'t1': [], 't2': []}, terms=('t1', 't2'))
        assert m.terms_changed is False
        m.remove_term('t2')
        assert m.terms_changed is True
        m.add_term('t2')
        assert m.terms_changed is False  # back to the initial tuple

    def test_selected_returns_checked_races_date_sorted(self):
        m = _model({'t1': [_race(2, 'two', 200), _race(1, 'one', 100),
                           _race(3, 'three', 300)]})
        m.toggle(2)
        assert [r['ID'] for r in m.selected()] == [1, 3]
