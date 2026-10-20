"""Storage layer: the same calls work on a local folder or an S3 location (s3://bucket/prefix)."""
import base64
import hashlib
import os
import shutil
from pathlib import Path


def is_s3(location) -> bool:
    return isinstance(location, str) and location.startswith("s3://")


def join(base, *parts):
    """Append path parts to a local Path or an s3:// URI."""
    if is_s3(base):
        return "/".join([str(base).rstrip("/"), *(p.strip("/") for p in parts)])
    return Path(base).joinpath(*parts)


def sha256_of(path: Path) -> str:
    """Checksum read in chunks, so large files never load fully into memory."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _split(uri: str) -> tuple[str, str]:
    bucket, _, key = uri[len("s3://"):].partition("/")
    return bucket, key


def _s3():
    import boto3  # imported here so local-only runs and tests never need AWS libraries
    return boto3.client("s3")


def put_bytes(data: bytes, dest) -> None:
    """Write bytes to dest. Local: atomic via a temp file. S3: via PUT. is atomic."""
    if is_s3(dest):
        bucket, key = _split(dest)
        checksum = base64.b64encode(hashlib.sha256(data).digest()).decode()
        _s3().put_object(Bucket=bucket, Key=key, Body=data, ChecksumSHA256=checksum)
        return
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".tmp")
    try:
        tmp.write_bytes(data)
        os.replace(tmp, dest)
    finally:
        tmp.unlink(missing_ok=True)


def put_file(src: Path, dest) -> str:
    """Copy a local file to dest, verify the copy, and return the SHA-256 of the source."""
    digest = sha256_of(src)
    if is_s3(dest):
        bucket, key = _split(dest)
        checksum = base64.b64encode(bytes.fromhex(digest)).decode()
        client = _s3()
        with src.open("rb") as f:   # one PUT handles files up to 5 GB; ours are far smaller
            client.put_object(Bucket=bucket, Key=key, Body=f, ChecksumSHA256=checksum)
        if client.head_object(Bucket=bucket, Key=key)["ContentLength"] != src.stat().st_size:
            raise IOError(f"Size mismatch after uploading {src.name}")
        return digest

    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".tmp")
    try:
        shutil.copy2(src, tmp)
        if sha256_of(tmp) != digest:
            raise IOError(f"Checksum mismatch after copying {src.name}")
        os.replace(tmp, dest)
    finally:
        tmp.unlink(missing_ok=True)
    return digest