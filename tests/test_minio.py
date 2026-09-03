from minio.config import MinIOConfig


def test_minio_default_endpoint():
    assert MinIOConfig.ENDPOINT == "http://localhost:9000"


def test_minio_default_credentials():
    assert MinIOConfig.ACCESS_KEY == "icestream"
    assert MinIOConfig.SECRET_KEY == "icestream-minio"


def test_minio_bucket_name():
    assert MinIOConfig.BUCKET_NAME == "icestream"


def test_minio_is_not_secure_by_default():
    assert MinIOConfig.SECURE is False