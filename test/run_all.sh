#!/bin/bash

PASSED=0
FAILED=0
PASSED_TESTS=""
FAILED_TESTS=""

echo -e "Running all Raft tests"
echo ""

for testdir in */; do
    if [ -f "${testdir}main.go" ]; then
        testname="${testdir%/}"
        echo -n "Running ${testname} "
        output=$(go run "./${testdir}" 2>&1)
        exit_code=$?
        
        if [ $exit_code -eq 0 ]; then
            echo -e "PASSED"
            ((PASSED++))
            PASSED_TESTS="${PASSED_TESTS}\n  ✓ ${testname}"
        else
            echo -e "FAILED"
            ((FAILED++))
            FAILED_TESTS="${FAILED_TESTS}\n  ✗ ${testname}"
            echo "    Error: $(echo "$output" | tail -3)"
        fi
    fi
done

echo ""
echo -e "Passed: ${PASSED}"
echo -e "Failed: ${FAILED}"
echo ""

if [ $FAILED -eq 0 ]; then
    echo -e "All tests passed!"
    exit 0
else
    echo -e "Some tests failed!"
    exit 1
fi
