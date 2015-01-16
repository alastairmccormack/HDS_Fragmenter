# HDS_Fragmenter
Parses F4V Index files (f4x) to create HDS Fragments

## Usage
### Basic
In the directory holding `mystreamSeg1234.f4x` and `mystreamSeg1234.f4f`:

```
python hds_seg_fragmenter.py mystreamSeg1234.f4x
```

This will create the fragments in the current directory.

### Help

```
python hds_seg_fragmenter.py --help
```

## Prerequisites
1. Bitstring - https://pypi.python.org/pypi/bitstring/
