# Capstone-Project

A Fault-Tolerant Key/Value Storage System with Raft Consensus Algorithm

## Related Documents

- [Capstone Project Proposal](https://docs.google.com/document/d/1X6tT59Wi79GaQ95csaioKsGmxmlS6OltIo7_B06zPKY/edit?usp=sharing)
- [Timestamp Data](https://docs.google.com/spreadsheets/d/1pZAP1ZwJSw5ikStkUlCAP-HZXAcn0GY5p6KzAoBi_DY/edit?usp=sharing)

## Run Tests & See Goroutine Stack Dump

```bash
# Turn on debug logger
./run_tests.sh -debug

# Turn off debug logger
./run_tests.sh
```

Then go to your browser at http://localhost:6060/debug/pprof/goroutine?debug=2 to see the goroutine stack dump.
