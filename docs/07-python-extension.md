# Python Extension

We are working to support Python API for user to process and analyze data in Python.  


## Build and Use Python APIs
```
pip3 install maturin
matruin build -F python
pip3 install ./target/wheels/risinglight-*.whl
python3
import risinglight
db = risinglight.open("risinglight.db")
db.query("select 1 + 1")
```

## Progress

- [x] Support Python API on x86-64 Linux   
- [x] Support Python API on arm macOS  
- [ ] Support more APIs