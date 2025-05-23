# Profiling

To profile a test

```
pyinstrument -m pytest tests/ams_test.py::test_ams -xsv --tb=short
```

To see the flamechart
```
pyinstrument --load-pref <some timestamp> -r html
```

# Results

Ingesting the first 42 sp-traffic NA AMS files
- initial - 92.808s for 2,422,779 rows @26k / sec
- after optimizations - 29.401s for 2,422,779 rows @82k / sec

Over a 3x speedup