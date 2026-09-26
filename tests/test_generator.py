from datetime import date


def test_history_is_prefix_stable(generator):
    seg = generator.SEGMENTS[0]
    short = generator.simulate_segment(seg, date(2025, 3, 31), seed=1)
    long = generator.simulate_segment(seg, date(2026, 3, 31), seed=1)
    assert long[: len(short)] == short


def test_accounts_do_not_depend_on_as_of(generator):
    seg = generator.SEGMENTS[-1]
    assert generator.generate_accounts(seg, 20, seed=1) == generator.generate_accounts(seg, 20, seed=1)


def test_rate_cycle_shape(generator):
    f = generator.rate_cycle_outflow
    assert f(date(2022, 1, 1)) == 0.0
    assert f(date(2023, 3, 31)) == 1.0
    assert 0.0 < f(date(2022, 11, 1)) < 1.0
    assert f(date(2027, 1, 1)) == generator.RATE_CYCLE[-1][1]


def test_rate_sensitive_segment_dips_in_hike_cycle(generator):
    hni = next(s for s in generator.SEGMENTS if s["segment_id"] == "SA_HNI")
    path = dict(generator.simulate_segment(hni, date(2024, 1, 1), seed=7))
    assert path[date(2023, 4, 15)] < path[date(2022, 4, 15)]


def test_salary_shape_bounds(generator):
    assert generator.salary_shape(date(2026, 1, 1)) == 1.0
    assert generator.salary_shape(date(2026, 1, 31)) == -1.0


def test_segment_categories_are_valid(generator):
    allowed = {"retail_transactional", "retail_non_transactional", "wholesale"}
    assert {s["irrbb_category"] for s in generator.SEGMENTS} <= allowed
    assert len({s["segment_id"] for s in generator.SEGMENTS}) == len(generator.SEGMENTS)
