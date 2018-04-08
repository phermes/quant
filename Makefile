init:
    pip install -r requirements.txt

test:
    py.test testsQAntTom

.PHONY: init test
