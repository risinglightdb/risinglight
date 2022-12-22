# Python Extension

We are working to support Python API for user to process and analyze data in Python.  


## Build and Use Python APIs
```
matruin build
pip3 install ./target/wheels/package_name.whl
python3
import risinglight
db = risnglight.open("risinglight.db")
db.query("select 1 + 1")
```

## Progress

- [x] Support Python API on x86-64 Linux   
- [x] Support Python API on arm macOS  
- [ ] Support more APIs