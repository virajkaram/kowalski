import datetime
import os
import pathlib
import time

from kowalski.alert_brokers.alert_broker_wtp import watchdog
from kowalski.ingesters.ingester import KafkaStream
from test_ingester_ztf import Program, Filter
from kowalski.utils import Mongo, init_db_sync
from kowalski.config import load_config
from kowalski.log import log

"""
Essentially the same as the ZTF ingester tests (test_ingester_ztf.py), except -
1. Only a small number of test WTP alerts are used. These alerts are stored in data/, as opposed to ZTF test_ingester_ztf.py that pulls alerts from googleapis.
2. Collections and Skyportal programs and filters relevant to WTP are used instead of the ztf ones.
"""

""" load config and secrets """
config = load_config(config_files=["config.yaml"])["kowalski"]

USING_DOCKER = os.environ.get("USING_DOCKER", False)
if USING_DOCKER:
    config["server"]["host"] = "kowalski-api-1"


class TestIngester:
    """ """

    def test_ingester(self):
        init_db_sync(config=config, verbose=True)

        log("Setting up paths")

        path_logs = pathlib.Path("logs/")
        if not path_logs.exists():
            path_logs.mkdir(parents=True, exist_ok=True)

        log("Checking the existing WTP alert collection states")
        mongo = Mongo(
            host=config["database"]["host"],
            port=config["database"]["port"],
            replica_set=config["database"]["replica_set"],
            username=config["database"]["username"],
            password=config["database"]["password"],
            db=config["database"]["db"],
            srv=config["database"]["srv"],
            verbose=True,
        )
        collection_alerts = config["database"]["collections"]["alerts_wtp"]
        collection_alerts_aux = config["database"]["collections"]["alerts_wtp_aux"]

        # check if the collection exists, drop it if it does
        if collection_alerts in mongo.db.list_collection_names():
            try:
                mongo.db[collection_alerts].drop()
            except Exception as e:
                log(f"Failed to drop the collection {collection_alerts}: {e}")
        if collection_alerts_aux in mongo.db.list_collection_names():
            try:
                mongo.db[collection_alerts_aux].drop()
            except Exception as e:
                log(f"Failed to drop the collection {collection_alerts_aux}: {e}")

        if config["misc"]["broker"]:
            log("Setting up test groups and filters in Fritz")
            program = Program(
                group_name="FRITZ_TEST_WTP",
                group_nickname="test-wtp",
                filter_name="Infrared transients",
                stream_ids=[5],
            )
            Filter(
                collection="WTP_alerts",
                group_id=program.group_id,
                filter_id=program.filter_id,
                pipeline=[{"$match": {"candid": {"$gt": 0}}}],  # pass all
            )

            program2 = Program(
                group_name="FRITZ_TEST_WTP_AUTOSAVE",
                group_nickname="test2-wtp",
                filter_name="Infraorange transients",
                stream_ids=[5],
            )
            Filter(
                collection="WTP_alerts",
                group_id=program2.group_id,
                filter_id=program2.filter_id,
                autosave=True,
                pipeline=[{"$match": {"objectId": "WIRC21aaaab"}}],
            )

            program3 = Program(
                group_name="FRITZ_TEST_WTP_UPDATE_ANNOTATIONS",
                group_nickname="test3-wtp",
                filter_name="Infraorange transients",
                stream_ids=[5],
            )
            Filter(
                collection="WTP_alerts",
                group_id=program3.group_id,
                filter_id=program3.filter_id,
                update_annotations=True,
                pipeline=[
                    {"$match": {"objectId": "WTP24auhaa"}}
                ],  # there is 1 alerts in the test set for this oid
            )

        # create a test WTP topic for the current UTC date
        date = datetime.datetime.utcnow().strftime("%Y%m%d")
        topic_name = f"wtp_{date}_test"
        path_alerts = "wtp_alerts/test_alerts"

        with KafkaStream(
            topic_name,
            pathlib.Path(f"data/{path_alerts}"),
            config=config,
            test=True,
        ):
            log("Starting up Ingester")
            watchdog(obs_dates=[date], test=True)
            log("Digested and ingested: all done!")

        log("Checking the WTP alert collection states")
        num_retries = 7
        # alert processing takes time, which depends on the available resources
        # so allow some additional time for the processing to finish
        for i in range(num_retries):
            if i == num_retries - 1:
                raise RuntimeError("WTP Alert ingestion failed")

            n_alerts = mongo.db[collection_alerts].count_documents({})
            n_alerts_aux = mongo.db[collection_alerts_aux].count_documents({})

            try:
                assert n_alerts == 18
                assert n_alerts_aux == 18
                print("----Passed WTP ingester tests----")
                break
            except AssertionError:
                print(
                    "Found an unexpected amount of alert/aux data: "
                    f"({n_alerts}/{n_alerts_aux}, expecting 5/4). "
                    "Retrying in 15 seconds..."
                )
                time.sleep(15)
                continue


if __name__ == "__main__":
    testIngest = TestIngester()
    testIngest.test_ingester()
