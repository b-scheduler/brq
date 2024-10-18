from brq import task


def callback(job, exception_or_result, consumer):
    print(f"Callback for {job} with {exception_or_result}")


@task(callback_func=callback)
def echo(message):
    print(f"Received message: {message}")
    return "processed"


if __name__ == "__main__":
    # Run the task once, for local debug
    # echo("hello")

    # Run as a daemon
    echo.serve()
