from quixstreams import Application
import json


def main():
    app = Application(
        broker_address="127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092",
        loglevel="DEBUG",
        consumer_group="wikipedia-consumer",
        auto_offset_reset="earliest",
    )

    with app.get_consumer() as consumer:
        consumer.subscribe(["wikipedia-changes"])
        


        while True:
            msg = consumer.poll(1)
            new=0
            edit=0
            move=0
            protect=0
            unprotect=0
            revert=0
            upload=0
            log=0
            categorize=0
            delete=0

            if msg is None:
                print("Waiting...")
            elif msg.error() is not None:
                raise Exception(msg.error())
            else:
                key = msg.key().decode("utf8")
                value = json.loads(msg.value())
                offset = msg.offset()

                change_type=value.get("type")

                

                print(f"{offset} {key} {change_type}")
                consumer.store_offsets(msg)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"New: {new}, Edit: {edit}, Delete: {delete}, Move: {move}, Protect: {protect}")
