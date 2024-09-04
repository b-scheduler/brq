from brq import task


@task
def echo(message):
    print(f"Received message: {message}")


if __name__ == "__main__":
    # Run the task once, for local debug
    # echo("hello")

    # Run as a daemon
    echo.serve()
