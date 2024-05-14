def get_redis_url(
    host: str = "localhost",
    port: int = 6379,
    db: int = 0,
    cluster: bool = False,
    tls: bool = False,
):
    if not cluster:
        url = f"{host}:{port}/{db}"
    else:
        # Ignore db
        url = f"{host}:{port}"

    if tls:
        return f"rediss://{url}"
    else:
        return f"redis://{url}"
