# HDS_Fragmenter
Parses F4V Index files (f4x) to create HDS Fragments

## Usage
### Basic
In the directory holding `mystreamSeg1234.f4x` and `mystreamSeg1234.f4f`:

    python hds_seg_fragmenter.py mystreamSeg1234.f4x

This will create the fragments in the current directory, which can be served by any file serving HTTP server, such as Apache HTTPD, Nginx, IIS. Upload to S3 for instant cloud-scale serving.

Be sure to set the following mime types:

    Fragments: video/f4f
    .bootstrap: application/binary
    .f4m: application/f4m
    
If your HDS packager has created multiple segments for an asset, you may pass multiple .f4x files or use wildcards:

    python hds_seg_fragmenter.py mystreamSeg*.f4x

### Live Streaming and S3 Upload (Linux Only)

S3Inotifier monitors a directory for changes, automatically fragments and uploads all components to an S3 bucket.

### Flash Access / FAX / DRM

The encrypted video and audio is unaltered during the fragmentation process. As long as the client is able to reference the .drmmeta file and/or the drm data within the stream-level .f4m file, and retrieve the required keys, the client will be able to play the content.

### Help


    python hds_seg_fragmenter.py --help


## Prerequisites
1. Bitstring - https://pypi.python.org/pypi/bitstring/
2. pyinotify (Only required for S3Inotifier - Linux Only)
